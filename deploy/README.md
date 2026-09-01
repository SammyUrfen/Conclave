# Deploying the conclave arbiter

> The arbiter is the small central node: meet rendezvous, signaling relay, epoch
> minting, election arbitration, and the `/api` surface the dashboard reads.
> **It never carries media.** Every byte of audio and video flows peer-to-peer over
> the subnet a coordinator computes, so this process stays cheap enough for a free
> tier no matter how many people are in a meet.
>
> Contents: `Dockerfile` (multi-stage, distroless, non-root) ·
> `docker-compose.yml` + `Caddyfile` (local `wss://`) · this note.
> The static frontend deploys separately, to GitHub Pages — see
> `.github/workflows/pages.yml`.

---

## The one decision that matters: `ws://` or `wss://`

The dashboard is served from `https://sammyurfen.github.io`. What it may connect to
is decided by the browser, not by you:

| Arbiter reachable at | From the Pages site | Why |
|---|---|---|
| `ws://localhost:9000` | **works** | W3C *Secure Contexts* classifies `localhost`, `127.0.0.1` and `[::1]` as potentially trustworthy, and mixed-content blocking exempts them. Chrome, Edge, Firefox and Safari all implement this. |
| `ws://192.168.1.5:9000` | **blocked** | The exemption is by HOSTNAME, not by network. Your own LAN does not count. |
| `wss://arbiter.example.com` | **works** | A real certificate. This is the hosted path. |

So: running the arbiter on the viewer's own machine needs nothing. Running it
anywhere else needs TLS, and that is what the compose stack below rehearses.

A caveat worth knowing before it confuses you: the loopback exemption depends on
`localhost` actually resolving to loopback. It normally does; where it does not, the
UI's server field also accepts `127.0.0.1`.

---

## Local: plain HTTP

```sh
make run-server ARGS="-coordinate -addr :9000"
```

Then open the dashboard (`web/index.html`, or the Pages site) and point it at
`localhost:9000`. The default `-allowed-origins` already lists the Pages origin and
any localhost port, so nothing else is needed.

## Local: `wss://` through Caddy

```sh
docker compose -f deploy/docker-compose.yml up --build
```

Caddy issues a locally-trusted certificate for `localhost` and prints the root it
wants trusted on first run. Point the dashboard at `wss://localhost`.

## Hosted (Render / Fly.io / any container platform)

Build `deploy/Dockerfile` from the **repository root** — it copies `go.mod` and the
whole source tree:

```sh
docker build -f deploy/Dockerfile -t conclave-server .
```

Then run it with the three flags a hosted deployment cannot omit:

```sh
conclave-server \
  -addr :9000 \
  -coordinate \
  -public-url https://arbiter.example.com \
  -allowed-origins "https://sammyurfen.github.io"
```

**`-public-url` is not optional behind a TLS terminator.** The server cannot know
whether something in front of it terminates TLS, and it will not guess: with the flag
absent it derives join URLs from `-addr` and hands every participant an `http://` /
`ws://` command that fails. Guessing `https://` would be worse — it would break every
plain deployment instead. One value in, both `join.ws_url` and `join.peer_command`
out, so they are correct together or obviously wrong together, never subtly wrong.

**`-allowed-origins` is one list used in two places** — the REST CORS headers and the
WebSocket upgrade check — because a security control that exists twice is one that
will diverge (`internal/policy`). A single trailing `*` is a **port** wildcard only;
`http://*.example.com` is rejected at startup. Drop the localhost entries on a hosted
deployment and list only the origins that should reach it.

### Platform notes

- **Render** — Web Service, Docker runtime, root directory `.`, Dockerfile path
  `deploy/Dockerfile`. Render terminates TLS for you and sets `$PORT`, which this
  binary does not read: set `-addr :10000` to match Render's default, or set the
  `PORT` env var to `9000`. Health check path `/healthz`.
- **Fly.io** — `fly launch --dockerfile deploy/Dockerfile`. Fly terminates TLS at its
  edge; keep `internal_port = 9000` and `force_https = true`.
- **Hugging Face Spaces** — Docker SDK. Spaces serves on port 7860, so use
  `-addr :7860`, and set `-public-url https://<user>-<space>.hf.space`.

All three sleep an idle free instance. That is survivable and worth understanding
rather than working around: **the arbiter's state is entirely in memory** — meets,
epochs, the tombstone ring — so a cold start is an empty registry. §6.7 covers what
that means for the fence: a restarted arbiter has no meets, so every peer must rejoin,
and `TypeJoined` resets each peer's epoch to 0. It is a real limitation, closed by a
rule rather than by persistence, and persisting the counter is out of scope.

---

## Flags a deployment actually touches

| Flag | Hosted value | Why |
|---|---|---|
| `-addr` | the platform's port | Render `:10000`, Spaces `:7860`, Fly `:9000`. |
| `-public-url` | your `https://` origin | The join rendezvous. Mandatory behind TLS. |
| `-allowed-origins` | `https://sammyurfen.github.io` | Drop localhost entries in production. |
| `-coordinate` | on | Hosts the coordinator in-process, so a meet works from the first two peers. |
| `-elect` | optional | Phase 6: hands the role to a fit peer. With `-coordinate` too, the arbiter holds it until a peer earns it. |
| `-log-format` | `json` | Every platform's log viewer parses it. |
| `-demo` | **off** | See below. |

### `-demo` is off, and should stay off

The two demo routes let an **unauthenticated** caller terminate a participant's
connection and force a control-plane transition. That is a denial-of-service
primitive, and there is no authentication anywhere on this surface by design — it is a
portfolio demo, not a product. So the routes are **not registered at all** unless the
flag is set: `404`, not `403`, because an unregistered route cannot be reached by a
bug in a permission check. Turn it on for a live demo, and turn it off after.

The dashboard learns whether they exist from `demo_enabled` on `GET /api/meets`,
which is derived from the same absent-dependency check that decides whether the routes
were registered — so the button and the route cannot disagree.

### Startup is fail-loud

A self-contradictory configuration does not start. `-gone-after` shorter than the
Hub's socket-detection window, `-degraded-after` at or past `-gone-after`, a
`-log-format` typo, an unparseable origin, an empty origin list with the dashboard
on, a `-public-url` that is not an http(s)/ws(s) URL: each is one sentence on stderr
naming the flag. That is deliberate — an operator typo in a rarely-exercised value
should break the process immediately, not surface hours later as "the dashboard
mysteriously cannot reach the arbiter".

---

## Verifying a deployment

```sh
curl -sS https://arbiter.example.com/healthz
# {"status":"ok","service":"conclave-server"}

curl -sS https://arbiter.example.com/api/meets
# {"api_version":1,"demo_enabled":false,"meets":[],"ended":[]}

# Demo routes must not exist unless you asked for them:
curl -sS -o /dev/null -w '%{http_code}\n' -X POST \
  https://arbiter.example.com/api/demo/meets/x/evict -d '{"name":"a"}'
# 404

# CORS must ECHO the origin, never "*":
curl -sSI -H 'Origin: https://sammyurfen.github.io' https://arbiter.example.com/api/meets \
  | grep -i access-control-allow-origin
# access-control-allow-origin: https://sammyurfen.github.io
```

A request with **no** `Origin` header (curl, a health checker) is answered normally
with no CORS headers at all — CORS is a browser mechanism and there is nothing to
answer. A request whose origin does not match is answered normally **without**
`Access-Control-Allow-Origin`, letting the browser block it; the server does not
`403`, because a distinguishable error would leak the allow-list to a probing page.

Then join it:

```sh
make run-peer ARGS="-call -managed -server https://arbiter.example.com -room standup -name alice"
```

`POST /api/meets` returns that exact command in its `join` block, built from
`-public-url`, which is the whole point of setting it.
