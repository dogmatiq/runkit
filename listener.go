package runkit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
)

// listener manages the lifecycle of the network listener.
//
// The stub implementation holds a TCP port open without accepting connections.
// The future gRPC implementation will replace it with no changes to the startup
// sequence in Run().
type listener interface {
	// ListenAndServe binds to the configured address and begins serving.
	// onReady is called with the resolved advertise address once the listener is
	// bound. ListenAndServe then blocks until ctx is cancelled or a fatal error
	// occurs.
	ListenAndServe(ctx context.Context, onReady func(advertiseAddr string)) error
}

// stubListener binds a TCP port but never accepts connections.
type stubListener struct {
	// bindAddr is the local address to bind (e.g. "0.0.0.0:7831").
	bindAddr string
	// advertiseAddr is the address to report to peers. If empty, it is derived
	// from bindAddr and network interface introspection.
	advertiseAddr string
}

func (s *stubListener) ListenAndServe(ctx context.Context, onReady func(string)) error {
	ln, err := net.Listen("tcp", s.bindAddr)
	if err != nil {
		return fmt.Errorf("binding listener: %w", err)
	}
	defer ln.Close()

	addr, err := resolveAdvertiseAddr(ln.Addr().(*net.TCPAddr), s.advertiseAddr)
	if err != nil {
		return err
	}

	onReady(addr)

	<-ctx.Done()
	return nil
}

// resolveAdvertiseAddr determines the address to advertise to peers.
//
// If configured is non-empty, it is used verbatim.
// Otherwise, the host from bound is used. If bound's IP is unspecified
// (0.0.0.0 or ::), the first non-loopback non-link-local IPv4 address found
// on any network interface is used instead.
func resolveAdvertiseAddr(bound *net.TCPAddr, configured string) (string, error) {
	if configured != "" {
		return configured, nil
	}

	host := bound.IP.String()

	if bound.IP.IsUnspecified() {
		found, err := firstRoutableIPv4()
		if err != nil {
			return "", err
		}
		host = found
	}

	return net.JoinHostPort(host, strconv.Itoa(bound.Port)), nil
}

// firstRoutableIPv4 returns the first non-loopback, non-link-local IPv4
// address found on any network interface.
func firstRoutableIPv4() (string, error) {
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

			if ip == nil || ip.To4() == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}

			return ip.String(), nil
		}
	}

	return "", errors.New(
		"no routable IPv4 address found; set DOGMA_ADVERTISE_ADDRESS explicitly",
	)
}
