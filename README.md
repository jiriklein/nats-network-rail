# STOMP to NATS Bridge with Monitoring

## Architecture

```
NTROD Feed → STOMP Python Bridge → NATS → Other subs
                                    ↓
                       NATS Prometheus Exporter → Prometheus → Grafana
```

## What does this repo do?

- Bridges sync STOMP callbacks with async NATS publish
- Monitoring with Prometheus metrics and a Grafana dashboard
- Monitor messages/sec and bytes/sec
- Containerised in a Docker Compose setup for NATS, Prometheus, and Grafana, so it should run anywhere

## Prerequisites

- Python 3.10+ (built with 3.13)
- Docker and Docker Compose

## How do I run this?

### via Makefile

Ensure that your environment variables are set in an `.env` file in line with `.env.template`. Then run `source .env`.
You will require an NTROD account. This can be set up at https://datafeeds.networkrail.co.uk/ntrod/welcome.

```bash
make local-stack
```

This will spin up
1. STOMP client/bridge
2. NATS server
3. NATS exporter
4. Prometheus
5. Grafana


## Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| **NATS Client** | nats://localhost:4222 | - |
| **NATS Monitoring** | http://localhost:8222 | - |
| **NATS Exporter** | http://localhost:7777/metrics | - |
| **Prometheus** | http://localhost:9090 | - |
| **Grafana** | http://localhost:3000 | admin / admin |

## Other usage

### Testing Message Flow

**Terminal 1** - Start the bridge:
```bash
make local-stack 
```

**Terminal 2** - Subscribe to NATS:

Install NATS CLI locally with

```bash
brew tap nats-io/nats-tools
brew install nats-io/nats-tools/nats```
```

```bash
nats sub "train.mvt.raw"
# or with wildcards
nats sub "train.*"
```

## Monitoring

### Grafana Dashboard

1. Open http://localhost:3000
2. Login with `admin` / `admin`
3. Go to **Dashboards** → View the NATS dashboard
4. Or navigate to **Explore** and run queries like:
   - `rate(gnatsd_varz_in_msgs[1m])` - Message rate
   - `gnatsd_varz_connections` - Active connections
   - `gnatsd_varz_subscriptions` - Total subscriptions

## Project Structure

```
.
├── docker/
│   ├── docker-compose.yml
│   └── Dockerfile-client
├── prometheus/
│   └── prometheus.yml
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── prometheus.yaml
│   │   └── dashboards/
│   │       └── dashboard.yaml
│   └── dashboards/
│       └── nats-overview.json
├── Makefile
├── pyproject.toml
├── uv.lock
├── TODO.md
└── README.md
```

## What do next?

- **Interactivity**: Add an interactive whitelist/allowlist for [STANOX stations](https://raw.githubusercontent.com/openraildata/reference-data/main/stanox-stanme.csv) to monitor 
- **TLS/SSL**: Enable encrypted connections
- **High Availability**: Use NATS clustering
- **Persistence**: NATS JetStream for message persistence
- **Logging**: Add structured logging
- **Metrics**: Expose custom application metrics

## License

MIT

## Support

- Network Rail data feeds: https://datafeeds.networkrail.co.uk/ntrod/welcome
- Network Rail TRUST mvmt schema: https://github.com/openraildata/network-rail-json-schema/blob/master/network-rail-trust-movement.schema.json
- STOMP API: https://jasonrbriggs.github.io/stomp.py/api.html
- NATS Documentation: https://docs.nats.io
- Prometheus Documentation: https://prometheus.io/docs
- Grafana Documentation: https://grafana.com/docs
