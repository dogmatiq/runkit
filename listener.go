package runkit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
)

// defaultListenAddr is the default address the engine listens on if no listen
// address is configured. It uses port 0 to let the OS choose an available port.
const defaultListenAddr = ":0"

// stubListener binds a TCP port but never accepts connections.
type stubListener struct {
	// listenAddr is the local address to listen on (e.g. "0.0.0.0:7831").
	listenAddr string
	listener   net.Listener
}

// Listen binds the configured address and returns the bound address.
func (s *stubListener) Listen() (net.Addr, error) {
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return nil, fmt.Errorf("binding listener: %w", err)
	}
	s.listener = ln
	return ln.Addr(), nil
}

// Serve blocks until ctx is canceled, then closes the listener.
func (s *stubListener) Serve(ctx context.Context) error {
	defer s.listener.Close()
	<-ctx.Done()
	return ctx.Err()
}

// resolveAdvertiseAddrs determines the addresses to advertise to peers.
//
// If advertiseAddr is non-empty, it is used as the sole address. Otherwise,
// listenAddr determines which address families to discover:
//
//   - host ""        - bound to all interfaces on both families (e.g. net.Listen("tcp", ":0"))
//   - host "0.0.0.0" - bound to all IPv4 interfaces
//   - host "::"      - bound to all IPv6 interfaces
//   - other host     - bound to a specific IP; that IP is advertised directly
func resolveAdvertiseAddrs(
	bound net.Addr,
	listenAddr, advertiseAddr string,
) ([]string, error) {
	if advertiseAddr != "" {
		return []string{advertiseAddr}, nil
	}

	tcp, ok := bound.(*net.TCPAddr)
	if !ok {
		return nil, fmt.Errorf("resolveAdvertiseAddrs: expected *net.TCPAddr, got %T", bound)
	}

	port := strconv.Itoa(tcp.Port)

	listenHost, _, _ := net.SplitHostPort(listenAddr)

	// Normalize listenHost to its canonical form (e.g. "0" -> "0.0.0.0").
	if ip := net.ParseIP(listenHost); ip != nil {
		listenHost = ip.String()
	}

	switch listenHost {
	case "": // both families
		var addrs []string
		if ipv4, err := firstRoutableAddr(false); err == nil {
			addrs = append(addrs, net.JoinHostPort(ipv4, port))
		}
		if ipv6, err := firstRoutableAddr(true); err == nil {
			addrs = append(addrs, net.JoinHostPort(ipv6, port))
		}
		if len(addrs) == 0 {
			return nil, errors.New("no routable address found; set DOGMA_ADVERTISE_ADDRESS explicitly")
		}
		return addrs, nil

	case "0.0.0.0": // IPv4 only
		addr, err := firstRoutableAddr(false)
		if err != nil {
			return nil, err
		}
		return []string{net.JoinHostPort(addr, port)}, nil

	case "::": // IPv6 only
		addr, err := firstRoutableAddr(true)
		if err != nil {
			return nil, err
		}
		return []string{net.JoinHostPort(addr, port)}, nil

	default: // explicit IP
		return []string{net.JoinHostPort(listenHost, port)}, nil
	}
}

// firstRoutableAddr returns the first non-loopback, non-link-local address of
// the appropriate family found on any network interface. If ipv6 is true, it
// looks for a global IPv6 address; otherwise it looks for an IPv4 address.
func firstRoutableAddr(ipv6 bool) (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("enumerating network interfaces: %w", err)
	}

	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			// Skip interfaces we can't inspect; fall through to the
			// "no address found" error if none succeed.
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}

			isIPv4 := ip.To4() != nil
			if ipv6 == isIPv4 {
				continue
			}

			return ip.String(), nil
		}
	}

	family := "IPv4"
	if ipv6 {
		family = "IPv6"
	}
	return "", fmt.Errorf(
		"no routable %s address found; set DOGMA_ADVERTISE_ADDRESS explicitly",
		family,
	)
}
