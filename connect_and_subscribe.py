import asyncio
import json
import os
import queue
import socket
import sys
import threading
import time
import logging

import nats
import stomp


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("network-rail-client")

# NROD and STOMP feed related
CLIENT_ID = socket.getfqdn()
NROD_USERNAME = os.getenv("NROD_USERNAME")
NROD_PASSWORD = os.getenv("NROD_PASSWORD")
MOVEMENT_TOPIC = "/topic/TRAIN_MVT_ALL_TOC"
PUBLIC_DATA_FEED_URL = "publicdatafeeds.networkrail.co.uk"
PUBLIC_DATA_FEED_PORT = 61618
STATIONS_ALLOWLIST = ["54238", "54291", "52053", "54295", "54297", "54299"]

# NATS
NATS_URL = os.getenv("NATS_URL", "nats://nats:4222")
NATS_SUBJECT = os.getenv("NATS_SUBJECT", "train.mvt.raw")
NATS_MAX_RECONNECTS = int(os.getenv("NATS_MAX_RECONNECTS", "60"))
NATS_RECONNECT_WAIT_SECS = int(os.getenv("NATS_RECONNECT_WAIT_SECS", "1"))


class NatsBridge:
    def __init__(self):
        self.nats_url = NATS_URL
        self.loop: asyncio.AbstractEventLoop | None = None
        self.nc: nats.NATS | None = None
        self.thread: threading.Thread | None = None
        self.mq: queue.Queue | None = None
        self._running: bool = False
        self._ready = threading.Event()

    def __repr__(self):
        return "NatsBridge"

    async def _run_nats_client(self):
        try:
            self.nc = await nats.connect(self.nats_url)
            logger.info(f"Connected to NATS at {self.nats_url}")
            # NATS is ready
            self._ready.set()
            while True:
                if not self.mq.empty():
                    subject, data = self.mq.get_nowait()
                    await self.nc.publish(subject, data.encode())
                else:
                    await asyncio.sleep(0.01)

        except Exception as e:
            logger.error(f"NATS connection error: {e}")
            raise
        finally:
            if self.nc:
                await self.nc.close()

    def start(self):
        """Async event loop has to run in a separate (daemon) thread"""
        if self._running:
            return
        self._running = True

        def run_loop():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self._run_nats_client())

        self.mq = queue.Queue(maxsize=1000)
        self.thread = threading.Thread(target=run_loop, daemon=True)
        self.thread.start()

        # Wait for NATS to be ready
        if not self._ready.wait(timeout=10):
            raise TimeoutError("NATS connection timeout")

        logger.info("%s started and ready", repr(self))

    def publish_to_nats(self, subject: str, data: str):
        """Thread-safe method callable from sync code"""
        self.mq.put((subject, data))


def connect_and_subscribe(conn: stomp.Connection):
    connect_header = {"client-id": NROD_USERNAME + "-" + CLIENT_ID}
    conn.connect(NROD_USERNAME, NROD_PASSWORD, wait=True, headers=connect_header)
    conn.subscribe(destination=MOVEMENT_TOPIC, id=1, ack="auto")


class NRClient(stomp.ConnectionListener):
    def __init__(
        self,
        conn: stomp.Connection,
        bridge: NatsBridge,
    ):
        self.conn = conn
        self.bridge = bridge

    def on_error(self, frame):
        logger.error('received an error "%s"' % frame.body)

    def on_message(self, frame):
        try:
            evt = json.loads(frame.body)
            messages = evt if isinstance(evt, list) else [evt]
            for m in messages:
                body = m.get("body") or m
                loc_stanox = str(
                    body.get("loc_stanox") or body.get("location_stanox") or ""
                )
                self.bridge.publish_to_nats(subject=NATS_SUBJECT, data=json.dumps(body))

        except Exception as exc:
            logger.error(exc)

    def on_disconnected(self):
        logger.info("disconnected")
        connect_and_subscribe(self.conn)


def main():
    bridge = NatsBridge()
    bridge.start()

    conn = stomp.Connection(
        [(PUBLIC_DATA_FEED_URL, PUBLIC_DATA_FEED_PORT)], heartbeats=(4000, 4000)
    )
    conn.set_listener("", NRClient(conn=conn, bridge=bridge))
    connect_and_subscribe(conn)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down.")
    finally:
        conn.disconnect()


if __name__ == "__main__":
    main()
