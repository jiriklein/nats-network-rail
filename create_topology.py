"""Builds a static rail network graph / topology from Network Rail Schedule data (daily refresh)."""

import gzip
import json
import sys
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Type


class ScheduleRecord(ABC):
    """Abstract base class for all schedule record types"""

    # Registry mapping record type keys to classes
    _registry: Dict[str, Type["ScheduleRecord"]] = {}

    @classmethod
    @abstractmethod
    def get_record_type(cls) -> str:
        """Return the JSON key for this record type"""
        pass

    @classmethod
    @abstractmethod
    def from_dict(cls, data: dict) -> "ScheduleRecord":
        """Parse from JSON dict"""
        pass

    @classmethod
    def register(cls, record_class: Type["ScheduleRecord"]):
        """Register a concrete record class"""
        record_type = record_class.get_record_type()
        cls._registry[record_type] = record_class
        return record_class

    @classmethod
    def parse(cls, line: str) -> Optional["ScheduleRecord"]:
        """
        Parse a JSON line and return the appropriate concrete record type.
        Uses registry pattern to route to correct subclass.
        """
        try:
            data = json.loads(line)
            if not data:
                return None

            # top-level key denotes what message we're dealing with, e.g. "TiplocV1"
            key = next(iter(data))

            # select the right class from the registry
            record_class = cls._registry.get(key)
            if record_class:
                return record_class.from_dict(data[key])

            return None

        except (json.JSONDecodeError, KeyError, ValueError, StopIteration):
            return None


@dataclass
class ScheduleLocation:
    """A single location in a train schedule"""

    tiploc_code: str
    arrival: str | None = None
    departure: str | None = None
    pass_time: str | None = None
    public_arrival: str | None = None
    public_departure: str | None = None
    platform: str | None = None
    line: str | None = None
    path: str | None = None
    activity: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "ScheduleLocation":
        """Parse from JSON dict"""
        return cls(
            tiploc_code=data.get("tiploc_code", ""),
            arrival=data.get("arrival"),
            departure=data.get("departure"),
            pass_time=data.get("pass"),
            public_arrival=data.get("public_arrival"),
            public_departure=data.get("public_departure"),
            platform=data.get("platform"),
            line=data.get("line"),
            path=data.get("path"),
            activity=data.get("activity"),
        )


@dataclass
class ScheduleSegment:
    signalling_id: str | None = None
    train_category: str | None = None
    toc_code: str | None = None
    schedule_location: List[ScheduleLocation] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict) -> "ScheduleSegment":
        """Parse from JSON dict"""
        locations = [
            ScheduleLocation.from_dict(loc) for loc in data.get("schedule_location", [])
        ]

        return cls(
            signalling_id=data.get("signalling_id"),
            train_category=data.get("train_category"),
            toc_code=data.get("CIF_train_service_code"),
            schedule_location=locations,
        )


@ScheduleRecord.register
@dataclass
class TiplocRecord(ScheduleRecord):
    transaction_type: str
    tiploc_code: str
    stanox: str | None = None
    nalco: str | None = None
    crs_code: str | None = None
    description: str | None = None
    tps_description: str | None = None

    @classmethod
    def get_record_type(cls) -> str:
        return "TiplocV1"

    @classmethod
    def from_dict(cls, data: dict) -> "TiplocRecord":
        """Parse from JSON dict"""
        return cls(
            transaction_type=data.get("transaction_type", ""),
            tiploc_code=data.get("tiploc_code", ""),
            stanox=data.get("stanox"),
            nalco=data.get("nalco"),
            crs_code=data.get("crs_code"),
            description=data.get("description"),
            tps_description=data.get("tps_description"),
        )

    def get_station_name(self) -> str:
        """Get the best available station name"""
        return self.description or self.tps_description or self.tiploc_code


@ScheduleRecord.register
@dataclass
class JsonScheduleRecord(ScheduleRecord):
    transaction_type: str
    train_uid: str
    schedule_start_date: str
    schedule_end_date: str
    schedule_days_runs: str
    train_status: str | None = None
    schedule_segment: ScheduleSegment | None = None

    @classmethod
    def get_record_type(cls) -> str:
        return "JsonScheduleV1"

    @classmethod
    def from_dict(cls, data: dict) -> "JsonScheduleRecord":
        """Parse from JSON dict"""
        segment = None
        if "schedule_segment" in data:
            segment = ScheduleSegment.from_dict(data["schedule_segment"])

        return cls(
            transaction_type=data.get("transaction_type", ""),
            train_uid=data.get("CIF_train_uid", ""),
            schedule_start_date=data.get("schedule_start_date", ""),
            schedule_end_date=data.get("schedule_end_date", ""),
            schedule_days_runs=data.get("schedule_days_runs", ""),
            train_status=data.get("train_status"),
            schedule_segment=segment,
        )

    def get_route(self, tiploc_to_stanox: Dict[str, str]) -> List[str]:
        if not self.schedule_segment:
            return []

        route = []
        for location in self.schedule_segment.schedule_location:
            stanox = tiploc_to_stanox.get(location.tiploc_code)
            if stanox:
                route.append(stanox)

        return route


class NetworkBuilder:
    """Builds a rail network graph from schedule data"""

    def __init__(self):
        self.tiploc_to_stanox: Dict[str, str] = {}
        self.station_names: Dict[str, str] = {}
        self.connections: Dict[str, Set[str]] = defaultdict(set)

        self.stats = {
            "lines_processed": 0,
            "tiplocs_loaded": 0,
            "schedules_processed": 0,
            "schedules_skipped": 0,
            "unknown_records": 0,
        }

    def process_tiploc(self, tiploc: TiplocRecord):
        """Process a TIPLOC record"""
        if tiploc.tiploc_code and tiploc.stanox:
            self.tiploc_to_stanox[tiploc.tiploc_code] = tiploc.stanox

            # Store station name if we don't have one yet
            if tiploc.stanox not in self.station_names:
                self.station_names[tiploc.stanox] = tiploc.get_station_name()

            self.stats["tiplocs_loaded"] += 1

    def process_schedule(self, schedule: JsonScheduleRecord):
        if schedule.transaction_type != "Create":
            self.stats["schedules_skipped"] += 1
            return

        route = schedule.get_route(self.tiploc_to_stanox)

        if len(route) < 2:
            self.stats["schedules_skipped"] += 1
            return

        # build topology from neighbouring stations in one direction only
        for i in range(len(route) - 1):
            from_stanox = route[i]
            to_stanox = route[i + 1]

            # skip dupes and add bi-directionality
            if from_stanox != to_stanox:
                # Bidirectional connections
                self.connections[from_stanox].add(to_stanox)
                self.connections[to_stanox].add(from_stanox)

        self.stats["schedules_processed"] += 1

    def process_file(self, schedule_file: str):
        print(f"Reading schedule data from {schedule_file}...")

        # Open gzipped or plain file
        if schedule_file.endswith(".gz"):
            f = gzip.open(schedule_file, "rt", encoding="utf-8")
        else:
            f = open(schedule_file, "r", encoding="utf-8")

        try:
            for line in f:
                self.stats["lines_processed"] += 1

                if self.stats["lines_processed"] % 10000 == 0:
                    self._print_progress()

                # Parse record using registry pattern
                record = ScheduleRecord.parse(line)

                if record is None:
                    self.stats["unknown_records"] += 1
                elif isinstance(record, TiplocRecord):
                    self.process_tiploc(record)
                elif isinstance(record, JsonScheduleRecord):
                    self.process_schedule(record)

        finally:
            f.close()

    def _print_progress(self):
        print(
            f"Processed {self.stats['lines_processed']:,} lines... "
            f"({self.stats['tiplocs_loaded']:,} TIPLOCs, "
            f"{self.stats['schedules_processed']:,} schedules)"
        )

    def build_network_json(self) -> dict:
        # nodes
        nodes = [
            {"id": stanox, "name": self.station_names.get(stanox, stanox)}
            for stanox in sorted(self.connections.keys())
        ]

        # edges (avoiding bidirectional dupes)
        links = []
        seen_links: Set[tuple] = set()

        for source in sorted(self.connections.keys()):
            for target in sorted(self.connections[source]):
                link_key = tuple(sorted([source, target]))
                if link_key not in seen_links:
                    seen_links.add(link_key)
                    links.append({"source": source, "target": target})

        return {"nodes": nodes, "links": links}

    def save(self, output_file: str):
        network_data = self.build_network_json()

        print(f"\nNetwork created:")
        print(f"  Nodes: {len(network_data['nodes']):,}")
        print(f"  Links: {len(network_data['links']):,}")

        output_path = Path(output_file)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(network_data, f, indent=2)

        print(f"\nNetwork saved to {output_file}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python create_topology.py <schedule_file> [output_file]")
        print("")
        print("Example:")
        print("  python create_topology.py toc-full.gz")
        print("  python create_topology.py toc-full.json rail_network.json")
        print("")
        print("The schedule file should be the JSON format from Network Rail.")
        print("Download from: https://publicdatafeeds.networkrail.co.uk/")
        sys.exit(1)

    schedule_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "rail_network.json"

    if not Path(schedule_file).exists():
        print(f"Error: Schedule file not found: {schedule_file}")
        sys.exit(1)

    # Build network
    builder = NetworkBuilder()
    builder.process_file(schedule_file)
    builder.save(output_file)


if __name__ == "__main__":
    main()
