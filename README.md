# xboard-node — Docker Compose

## Quick start

```bash
git clone -b compose --depth 1 https://github.com/oballm/Xboard-Node.git
cd Xboard-Node
# Edit config/config.yml: panel.url, panel.token, panel.node_id
docker compose up -d
```

## Update / logs / stop

```bash
cd compose
docker compose pull && docker compose up -d
docker compose logs -f
docker compose down
```

## Layout

| Path | Purpose |
|------|---------|
| `compose.yml` | Service definition |
| `config/config.yml.example` | Template (safe to commit) |
| `config/config.yml` | Your secrets (**do not commit**; add to `.gitignore` upstream) |
| `.env.example` | Optional env overrides |

## Host network

`network_mode: host` is set so the node uses the host network stack (same as `docker run --network host`). Free the ports your protocols need.

## Image

Default: `ghcr.io/oballm/xboard-node:latest`.
