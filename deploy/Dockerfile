# conclave arbiter — multi-stage build.
#
# Stage 1 compiles with the full Go toolchain; stage 2 ships nothing but the static
# binary. The result is a few megabytes with no shell, no package manager, and no libc
# — which matters here for a specific reason and not as a size contest: this process
# is an UNAUTHENTICATED public surface (docs/PLAN.md §9.6, §13), so the cheapest real
# mitigation available is that a foothold in it finds no tools to use.

# --- build ---------------------------------------------------------------------
# Pinned to the toolchain go.mod declares. Bumping go.mod without bumping this fails
# the build loudly rather than compiling against something else.
FROM golang:1.26-alpine AS build

WORKDIR /src

# Dependencies first, as their own layer: they change far less often than the source,
# so an edit to cmd/server does not re-download the module graph.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 is what makes the binary static and therefore runnable on a
# distroless/scratch base at all. -trimpath keeps build-machine paths out of panics
# (they are a small information leak on a public surface); -s -w drops the symbol
# table and DWARF, which is the bulk of the size.
RUN CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags="-s -w" \
        -o /out/server ./cmd/server

# --- run -----------------------------------------------------------------------
# distroless/static is the smallest base that still carries the two things a static Go
# binary genuinely needs: CA certificates and /etc/passwd (so the nonroot user
# resolves). scratch would need both copied in by hand for no gain.
#
# The :nonroot tag runs as uid 65532. It is the tag, not a USER line, that makes this
# hold on platforms which ignore USER — and a container that cannot write to its own
# filesystem is a container whose compromise does not persist.
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=build /out/server /app/server

# 9000 is the -addr default. It is documentation: EXPOSE publishes nothing by itself.
EXPOSE 9000

# No HEALTHCHECK: distroless has no shell and no curl to run one with. GET /healthz is
# the probe, and every target this is meant for (Render, Fly, Cloud Run) polls it over
# HTTP from outside the container, which is the more honest check anyway — it tests the
# path a user takes rather than one the container can fake.

USER nonroot:nonroot
ENTRYPOINT ["/app/server"]

# Defaults chosen so `docker run` alone produces a working, SAFE server:
#   -addr :9000        listen on every interface (the container's own namespace)
#   -coordinate        host the coordinator in-process — the Phase 5 posture, and what
#                      makes a meet usable the moment two peers join
#   -dashboard         serve /api for the GitHub Pages frontend
# Deliberately absent: -demo (destructive, unauthenticated) and -elect (Phase 6; turn
# it on once peers are running with -coordinatable). Override the whole list with
# `docker run ... conclave-server -addr :9000 -elect ...`.
#
# A hosted deployment MUST also pass -public-url and -allowed-origins; see README.md.
CMD ["-addr", ":9000", "-coordinate", "-dashboard"]
