# xboard-node

Node backend for [Xboard](https://github.com/cedar2025/Xboard). Supports `sing-box` / `xray-core` dual kernels.

> **Disclaimer**: This project is for educational and learning purposes only.

## Features

- Protocols: V2Ray family, Trojan, Shadowsocks, Hysteria2, TUIC, AnyTLS
- Sync: WebSocket push + REST polling dual channel
- User controls: speed limit, device limit, alive-IP tracking, hot update
- Deploy modes: node mode, machine mode, standalone mode
- Multi-instance: single process binding multiple panels / nodes

## Install

### Docker

```bash
docker run -d --name xboard-node --restart=always --network=host \
  -v /etc/xboard-node:/etc/xboard-node \
  -v xboard-node-update:/var/lib/xboard-node \
  -e apiHost=https://panel.com -e apiKey=TOKEN -e nodeID=1 \
  ghcr.io/oballm/xboard-node:latest
```

Both mounts matter:

- `/etc/xboard-node` holds the config (if any), the TLS certificates and the
  kernel cache. Without it they live inside the container and are lost every
  time the container is recreated (new image, changed options): each node then
  requests new certificates, and Let's Encrypt allows only 5 per domain per
  week. Restarts (`docker restart`, panel restart, panel upgrade) keep them.
- `xboard-node-update` holds versions installed by a panel-requested upgrade
  (`control.upgrade`). Without a volume there, such upgrades are refused.
  Pulling a new image and recreating the container always wins over an
  installed upgrade.

### Docker Compose

```bash
git clone -b compose --depth 1 https://github.com/oballm/Xboard-Node.git
cd Xboard-Node
vim config/config.yml   # set panel.url / token / node_id
docker compose up -d
```

### Installer (Linux systemd)

```bash
# Node mode
curl -fsSL https://raw.githubusercontent.com/oballm/Xboard-Node/dev/install.sh | \
  sudo bash -s -- --mode node --panel https://panel.example.com --token TOKEN --node-id 1

# Machine mode
curl -fsSL https://raw.githubusercontent.com/oballm/Xboard-Node/dev/install.sh | \
  sudo bash -s -- --mode machine --panel https://panel.example.com --token TOKEN --machine-id 1

## xbctl

Run `xbctl` after installation for help. Common commands:

```bash
xbctl list                          # list all instances
xbctl status                        # running status
xbctl bind add-node --panel URL --token TOKEN --node-id 1
xbctl bind add-machine --panel URL --token TOKEN --machine-id 1
xbctl bind remove-node --panel URL --node-id 1
xbctl service restart
```

## Configuration

Legacy single-panel config is fully compatible. Appending bindings auto-migrates to `instances` format. See `config.yml.example`.

## Extensions

- Custom routes: [docs-custom-routes.md](docs-custom-routes.md)
- Custom outbounds: [docs-custom-outbounds.md](docs-custom-outbounds.md)
- DNS providers (ACME DNS-01): [docs-dns-providers.md](docs-dns-providers.md)

## License

MPL-2.0.
