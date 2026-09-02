// Command turn is conclave's TURN relay: the last-resort media path for a peer whose
// direct path to a neighbour does not exist.
//
// WHY THIS IS A BINARY OF OUR OWN, and not coturn. The ROADMAP said "stand up coturn",
// and coturn remains the right answer for a real deployment — deploy/docker-compose.yml
// carries that recipe. It is the wrong answer for this repository's own verification.
// `github.com/pion/turn/v5` is ALREADY in the module graph: pion/webrtc depends on it
// for the ICE TURN *client*, so the server costs zero new dependencies and about fifty
// lines. coturn costs a container or a system package, an external config file, and a
// test path that cannot run on a machine which does not have it installed. A relay
// nobody can start is a relay nobody verifies.
//
// WHY IT IS SEPARATE FROM cmd/server. The arbiter is control plane and is deliberately
// NEVER a media relay (see cmd/server's doc comment). A TURN server is pure data
// plane — sustained SRTP, one allocation per relayed path — and folding it into the
// arbiter would put the meet's control decisions behind the one process whose CPU and
// uplink the media is saturating. Two roles, two processes, the same split §2.1 draws
// everywhere else.
//
// WHAT IT IS NOT. It is not authenticated against conclave's identities (there are
// none — DESIGN.md §8.2), it has no quota handler, and its credentials are static
// long-term ones passed on the command line. It exists so a TURN-only path can be
// created and PROVEN on one host, not so an open relay can be run on the internet.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	pionlog "github.com/pion/logging"
	"github.com/pion/turn/v5"

	"github.com/SammyUrfen/conclave/internal/logging"
)

// defaultRealm is the STUN long-term-credential realm. It is part of the key
// derivation (username:realm:password), so client and server must agree on it
// exactly; a mismatch rejects every correct password with a bare 401.
const defaultRealm = "conclave"

func main() {
	// The same thin main → run(error) hop as cmd/server and cmd/peer: main translates
	// an error into an exit code and does nothing else.
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "turn: "+err.Error())
		os.Exit(1)
	}
}

// options is the validated configuration for the TURN server.
type options struct {
	// addr is the host:port the TURN server listens on for UDP. The relay ADDRESS is
	// separate and is publicIP: the listener is normally a wildcard, and a client
	// cannot send media to 0.0.0.0.
	addr string
	// publicIP is the address handed to clients as their relay address. It must be an
	// IP literal — a hostname would be resolved at an unknown time by an unknown
	// resolver, and this value goes into an ICE candidate, not into a URL.
	publicIP string
	realm    string
	// users maps username → password. Passwords are hashed into long-term keys at
	// construction, not stored, but they are still command-line arguments and are
	// visible in /proc: see the package comment on what this binary is not.
	users map[string]string
	// relayPorts confines every relay allocation to [min, max] UDP ports; the zero
	// value means "let the OS choose".
	//
	// It exists for the same reason media.SessionConfig.MediaPortRange does, and the
	// reasoning is worth repeating because it is the whole point of the flag: a
	// firewall rule can only be written against ports known in advance. Proving "media
	// flows over TURN when the direct path is blocked" means dropping the direct path
	// and NOT the relayed one, and with ephemeral relay ports there is no rule that can
	// tell them apart. Unpinned, the verification is not merely inconvenient, it is
	// unfalsifiable.
	relayPorts [2]uint16
	// deniedPeers are the peer address ranges this relay refuses to forward to; see
	// defaultDeniedPeers. Empty means no restriction.
	deniedPeers []*net.IPNet
	level       slog.Level
	format      logging.Format
}

// parseArgs parses argv into a validated options. Separated from run, and taking args
// rather than reading os.Args, so every default and every rejection is table-testable
// without opening a socket.
func parseArgs(args []string) (options, error) {
	fs := flag.NewFlagSet("turn", flag.ContinueOnError)
	addr := fs.String("addr", ":3478", "UDP host:port to listen on")
	publicIP := fs.String("public-ip", "",
		"IP address handed to clients as their relay address; required (0.0.0.0 is not routable by a client)")
	realm := fs.String("realm", defaultRealm, "long-term-credential realm; must match what the peer's -turn-user was issued for")
	users := fs.String("users", "", "comma-separated user=password list; required (there is no anonymous mode)")
	relayPorts := fs.String("relay-ports", "",
		"confine relay allocations to these UDP ports, e.g. 49160-49200; empty lets the OS choose")
	// DENY BY DEFAULT, and the default is deliberately inconvenient for this repo's own
	// verification. A relay that will forward into the LAN or loopback it sits in is a
	// pivot, and that is not a state anyone should reach by omission — the same argument
	// -users already makes about anonymous allocations. docs/verify-turn.md runs inside
	// a netns on 10.99.0.0/24 and therefore passes this flag explicitly, which is the
	// honest arrangement: the recipe documents the range it is opening, in the recipe,
	// instead of every operator inheriting an open relay so that one test can pass.
	deniedPeers := fs.String("denied-peers", defaultDeniedPeers,
		"comma-separated CIDR blocks this relay refuses to forward to; empty disables the "+
			"restriction (a LAN-relaying TURN server is a pivot). Note the default denies "+
			"10/8, so a run on a private network must narrow it — see docs/verify-turn.md")
	logLevel := fs.String("log-level", "info", "log level: debug|info|warn|error")
	logFormat := fs.String("log-format", "text", "log format: json|text")
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}

	// Every failure below is a configuration that would produce a server which starts,
	// answers, and relays nothing — the most expensive failure in this area, because
	// ICE simply never completes and nothing logs a reason.
	ip := strings.TrimSpace(*publicIP)
	if ip == "" {
		return options{}, fmt.Errorf("-public-ip is required: the relay address cannot be inferred from a wildcard listener")
	}
	if net.ParseIP(ip) == nil {
		return options{}, fmt.Errorf("invalid -public-ip %q: must be an IP literal, not a hostname", ip)
	}
	parsedUsers, err := parseUsers(*users)
	if err != nil {
		return options{}, err
	}
	ports, err := parseRelayPorts(*relayPorts)
	if err != nil {
		return options{}, err
	}
	denied, err := parseDeniedPeers(*deniedPeers)
	if err != nil {
		return options{}, err
	}
	level, err := logging.ParseLevel(*logLevel)
	if err != nil {
		return options{}, err
	}
	format, err := parseFormat(*logFormat)
	if err != nil {
		return options{}, err
	}

	return options{
		addr:     *addr,
		publicIP: ip,
		realm:    *realm,
		users:    parsedUsers,
		// Reported to the operator at startup rather than kept private: "which ports is
		// this relay actually using" is the first question the firewall rule needs.
		relayPorts:  ports,
		deniedPeers: denied,
		level:       level,
		format:      format,
	}, nil
}

// parseUsers reads the -users flag: "alice=secret,bob=other".
//
// An empty list is REJECTED rather than treated as "no authentication". pion/turn will
// happily run with an AuthHandler that admits everyone, and a TURN server reachable by
// anyone is a bandwidth amplifier — not a default anybody should reach by omission.
func parseUsers(s string) (map[string]string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("-users is required: user=password[,user=password...]")
	}
	out := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		user, pass, found := strings.Cut(pair, "=")
		user, pass = strings.TrimSpace(user), strings.TrimSpace(pass)
		if !found || user == "" || pass == "" {
			return nil, fmt.Errorf("invalid -users entry %q: want user=password", pair)
		}
		out[user] = pass
	}
	return out, nil
}

// parseRelayPorts reads the -relay-ports flag: "lo-hi", a bare port meaning a range of
// one, or empty for the OS default. It mirrors cmd/peer's parseMediaPorts deliberately
// — the live verification writes one nft rule against BOTH ranges, so an operator who
// learned one spelling should not have to learn a second.
func parseRelayPorts(s string) ([2]uint16, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return [2]uint16{}, nil
	}
	lo, hi, found := strings.Cut(s, "-")
	if !found {
		hi = lo
	}
	parse := func(field string) (uint16, error) {
		n, err := strconv.Atoi(strings.TrimSpace(field))
		// Port 0 is excluded deliberately: to the kernel it means "pick any", so a
		// range starting at 0 silently disables the confinement this flag provides.
		if err != nil || n < 1 || n > 65535 {
			return 0, fmt.Errorf("invalid -relay-ports %q: %q is not a port in 1-65535", s, strings.TrimSpace(field))
		}
		return uint16(n), nil
	}
	min, err := parse(lo)
	if err != nil {
		return [2]uint16{}, err
	}
	max, err := parse(hi)
	if err != nil {
		return [2]uint16{}, err
	}
	if max < min {
		return [2]uint16{}, fmt.Errorf("invalid -relay-ports %q: the high port %d is below the low port %d", s, max, min)
	}
	return [2]uint16{min, max}, nil
}

// defaultDeniedPeers is the peer-address deny set cmd/turn ships with. It is the SAME
// set deploy/docker-compose.yml gives coturn (0/8, 127/8, 169.254/16, 10/8, 172.16/12,
// 192.168/16) plus 224/4 for coturn's --no-multicast-peers, because the reasoning in
// that file applies verbatim here: a TURN server that will relay into 127.0.0.0/8 or a
// LAN is a pivot into whatever network it sits in.
const defaultDeniedPeers = "0.0.0.0/8,10.0.0.0/8,127.0.0.0/8,169.254.0.0/16," +
	"172.16.0.0/12,192.168.0.0/16,224.0.0.0/4"

// verifyTurnDeniedPeers is the value docs/verify-turn.md passes: the default with 10/8
// removed, because that recipe runs every process inside one netns on 10.99.0.0/24.
// It lives here, next to the default, so a change to either is a change to a test.
const verifyTurnDeniedPeers = "0.0.0.0/8,127.0.0.0/8,169.254.0.0/16," +
	"172.16.0.0/12,192.168.0.0/16,224.0.0.0/4"

// parseDeniedPeers reads the -denied-peers flag: a comma-separated CIDR list, or empty
// for no restriction at all.
//
// A malformed entry is FATAL rather than skipped, on the same principle as -users. The
// failure mode of a silently-dropped entry is that exactly the range the operator meant
// to protect is the one left open, and nothing anywhere says so.
func parseDeniedPeers(s string) ([]*net.IPNet, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	var out []*net.IPNet
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		// net.ParseCIDR rejects a bare IP, which is the likeliest typo here and the
		// one worth naming in the error: "10.0.0.0" is not "10.0.0.0/8".
		_, network, err := net.ParseCIDR(entry)
		if err != nil {
			return nil, fmt.Errorf("invalid -denied-peers entry %q: want a CIDR block like 10.0.0.0/8", entry)
		}
		out = append(out, network)
	}
	return out, nil
}

// peerAllowed reports whether this relay may forward to peerIP. An empty deny set
// admits everything, which is what an operator who empties the flag has asked for.
func peerAllowed(denied []*net.IPNet, peerIP net.IP) bool {
	for _, network := range denied {
		if network.Contains(peerIP) {
			return false
		}
	}
	return true
}

// permissionHandler is the callback pion/turn consults on every CreatePermission and
// ChannelBind, i.e. before a client can name a new peer to relay to.
//
// The DENIAL IS LOGGED, at warn, because this is the one place a safe default can look
// like a broken server: a refused permission surfaces to the operator as ICE simply
// never completing, with nothing anywhere saying why. One line naming the flag turns
// twenty minutes of packet capture into a fix.
func permissionHandler(log *slog.Logger, denied []*net.IPNet) turn.PermissionHandler {
	return func(clientAddr net.Addr, peerIP net.IP) bool {
		if peerAllowed(denied, peerIP) {
			return true
		}
		log.Warn("refusing to relay to a denied peer address",
			slog.String("client", clientAddr.String()),
			slog.String("peer", peerIP.String()),
			slog.String("hint", "pass -denied-peers to narrow the deny set"))
		return false
	}
}

// authHandler builds the long-term-credential callback pion/turn consults on every
// authenticated request.
//
// Passwords are turned into keys ONCE, here, rather than compared per request: the
// STUN long-term mechanism never transmits the password, it transmits a MAC keyed on
// MD5(username:realm:password), so the server needs the key and not the secret. The
// realm is part of that derivation, which is why it is a parameter and not a constant
// read from somewhere else — a handler deriving under one realm while the server
// advertises another rejects every correct password with an unexplained 401.
func authHandler(users map[string]string, realm string) turn.AuthHandler {
	keys := make(map[string][]byte, len(users))
	for user, pass := range users {
		keys[user] = turn.GenerateAuthKey(user, realm, pass)
	}
	return func(ra *turn.RequestAttributes) (string, []byte, bool) {
		key, ok := keys[ra.Username]
		if !ok {
			return "", nil, false
		}
		return ra.Username, key, true
	}
}

// relayGenerator picks the allocation strategy from the configured port range. The two
// generators are separate types in pion/turn rather than one with an optional range,
// so the choice is made here and stated once.
func relayGenerator(publicIP string, ports [2]uint16) turn.RelayAddressGenerator {
	ip := net.ParseIP(publicIP)
	if ports == [2]uint16{} {
		return &turn.RelayAddressGeneratorStatic{RelayAddress: ip, Address: "0.0.0.0"}
	}
	return &turn.RelayAddressGeneratorPortRange{
		RelayAddress: ip, Address: "0.0.0.0",
		MinPort: ports[0], MaxPort: ports[1],
	}
}

// run builds and serves until the process is signalled. Every failure before the
// listener opens is returned rather than logged and swallowed.
func run(args []string) error {
	opts, err := parseArgs(args)
	if err != nil {
		return err
	}

	log := logging.New(os.Stdout, opts.level, opts.format).With(slog.String("service", "conclave-turn"))

	conn, err := net.ListenPacket("udp4", opts.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", opts.addr, err)
	}

	server, err := turn.NewServer(turn.ServerConfig{
		Realm:       opts.realm,
		AuthHandler: authHandler(opts.users, opts.realm),
		// pion/turn logs through its own factory, not log/slog, so its output cannot
		// join the structured stream. The default factory is quiet unless PION_LOG_*
		// is set in the environment, which is the right trade: two interleaved formats
		// by default would be worse than one, and the escape hatch is still there for
		// the one debugging session that needs allocation-level detail.
		LoggerFactory: pionlog.NewDefaultLoggerFactory(),
		PacketConnConfigs: []turn.PacketConnConfig{{
			PacketConn:            conn,
			RelayAddressGenerator: relayGenerator(opts.publicIP, opts.relayPorts),
			// Per-listener in pion v5, not on ServerConfig. Without it pion installs
			// DefaultPermissionHandler, which admits every peer address on earth;
			// deploy/docker-compose.yml does not let coturn do that and there is no
			// reason this binary should.
			PermissionHandler: permissionHandler(log, opts.deniedPeers),
		}},
	})
	if err != nil {
		conn.Close()
		return fmt.Errorf("turn server: %w", err)
	}

	log.Info("turn relay listening",
		slog.String("addr", opts.addr),
		slog.String("public_ip", opts.publicIP),
		slog.String("realm", opts.realm),
		slog.Int("users", len(opts.users)),
		slog.String("relay_ports", relayPortsString(opts.relayPorts)),
		slog.Int("denied_peer_ranges", len(opts.deniedPeers)))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	log.Info("shutting down")
	if err := server.Close(); err != nil {
		return fmt.Errorf("close turn server: %w", err)
	}
	return nil
}

// relayPortsString renders the configured range for the startup log. "ephemeral" is
// spelled out rather than shown as "0-0", because that is the state in which the
// firewall-based verification cannot work and an operator should see it named.
func relayPortsString(p [2]uint16) string {
	if p == [2]uint16{} {
		return "ephemeral"
	}
	return fmt.Sprintf("%d-%d", p[0], p[1])
}

// parseFormat validates the -log-format flag, for the same reason cmd/peer and
// cmd/server each do: logging.New treats any unrecognised format as JSON, so a typo
// would silently change the shape of every line this process emits.
func parseFormat(s string) (logging.Format, error) {
	switch logging.Format(strings.ToLower(strings.TrimSpace(s))) {
	case logging.FormatText:
		return logging.FormatText, nil
	case logging.FormatJSON:
		return logging.FormatJSON, nil
	default:
		return "", fmt.Errorf("invalid -log-format %q: must be text or json", s)
	}
}
