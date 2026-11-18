# cp-node — xboard-node (oballm) with Docker Compose

Container, compose project and volume are named `cp-node`, so this can run
next to an upstream xboard-node on the same host (its nodes must use other
ports). The image is still `ghcr.io/oballm/xboard-node`.

## Quick start

```bash
git clone -b compose --depth 1 https://github.com/oballm/Xboard-Node.git cp-node
cd cp-node
# Edit config/config.yml: panel.url, panel.token, panel.node_id
docker compose up -d
```

## Update / logs / stop

```bash
cd cp-node
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
| `cp-node-update` (named volume) | Versions installed by a panel-requested upgrade; without it such upgrades are refused |

`./config` also holds the TLS certificates and the kernel cache. Keep it
mounted: otherwise every container recreate (`docker compose pull && up -d`,
changed options) requests new certificates, and Let's Encrypt allows only 5 per
domain per week. `docker compose down` keeps both (it removes named volumes
only with `-v`).

## Host network

`network_mode: host` is set so the node uses the host network stack (same as `docker run --network host`). Free the ports your protocols need.

## Image

Default: `ghcr.io/oballm/xboard-node:latest`.

## Moving from the xboard-node names

Checkouts from before the rename ran as project, container and volume
`xboard-node`. Stop that first, with the old compose file:

```bash
docker compose down        # old file still in place
git pull
docker compose up -d
```

Without the `down`, compose treats the renamed project as a new one and
leaves the old container running on the same ports. `./config` (certificates
included) carries over. The upgrade volume does not: the node starts on the
image's version, and a panel-requested upgrade installs the wanted one again.
