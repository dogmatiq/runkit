package network

import (
	"fmt"
	"net"
	"strconv"
)

// Listen starts a TCP listener bound to listenAddr. It returns the listener and
// the addresses that should be advertised to other nodes in the cluster.
//
// advertiseAddr is the configured advertise address, if any. If unset, the
// advertise address is derived from the listen address and network interface
// introspection.
//
// The host portion of listenAddr determines the address family (IPv4 vs IPv6)
// of the listener, and therefore the default addresses that are advertised.
//
// | Listen Host      | Address Family        | Advertised Host (unless provided)         |
// | ---------------- | --------------------- | ----------------------------------------- |
// | `""`             | IPv4 & IPv6           | All unicast IP addresses on the machine   |
// | `0` or `0.0.0.0` | IPv4                  | All unicast IPv4 addresses on the machine |
// | `::`             | IPv6                  | All unicast IPv6 addresses on the machine |
// | <IP address>     | Determined by IP type | The specified <IP address>                |
// | <hostname>       | IPv4 & IPv6           | The specified <hostname>                  |
//
// This behavior differs slightly to the behavior of [net.Listen] itself; which
// on some platforms treats `::` (the IPv6 wildcard) as dual-stack and listens
// for both IPv4 and IPv6 connections, while on other platforms it only listens
// for IPv6 connections.
// TODO: verify which platforms, be more specific
func Listen(listenAddr, advertiseAddr string) (lis net.Listener, advertiseAddrs []string, err error) {
	addressFamily := "tcp"
	listenHostFromConfig, _, _ := net.SplitHostPort(listenAddr)

	bindHostname := listenHostFromConfig
	bindIP := net.ParseIP(listenHostFromConfig)

	if bindIP != nil {
		// We're binding to an IP address, not a hostname by definition.
		bindHostname = ""

		// And we can determine the address family directly.
		if bindIP.To4() != nil {
			addressFamily = "tcp4"
		} else {
			addressFamily = "tcp6"
		}
	}

	// Start the listener, we close it if we return an error. If we return a nil
	// error it's the caller's responsibility to close it.
	lis, err = net.Listen(addressFamily, listenAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to start listener: %w", err)
	}
	defer func() {
		if err != nil {
			lis.Close()
		}
	}()

	// We have our listener, if we also have an explicit advertise address then
	// we're done.
	if advertiseAddr != "" {
		return lis, []string{advertiseAddr}, nil
	}

	// Get the bound port directly from the listener, it may have been specified
	// as "0" (auto-assign) in the configuration.
	bindPort := strconv.Itoa(lis.Addr().(*net.TCPAddr).Port)

	// If the configuration had us bind to a hostname, we advertise that
	// hostname as-is.
	if bindHostname != "" {
		return lis, []string{net.JoinHostPort(bindHostname, bindPort)}, nil
	}

	// Otherwise, if we are bound explicitly to a single address we can
	// advertise that directly.
	if bindIP != nil && !bindIP.IsUnspecified() {
		// If the listen host is a global unicast IP address, use that as the
		// advertise address.
		return lis, []string{net.JoinHostPort(bindIP.String(), bindPort)}, nil
	}

	// Finally, the listener is listening on all network interfaces; advertise
	// all of the unicast IP addresses on the machine that match the address
	// family.
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, nil, fmt.Errorf("unable to list network interfaces: %w", err)
	}

	for _, iface := range ifaces {
		// Get all of the interface's unicast addresses. If we're unable to do so,
		// we just assume the interface has no unicast addresses and move on to the
		// next interface.
		addrs, _ := iface.Addrs()

		for _, addr := range addrs {
			ip := extractIP(addr)

			if !isAddressable(addressFamily, ip) {
				continue
			}

			advertiseAddrs = append(
				advertiseAddrs,
				net.JoinHostPort(ip.String(), bindPort),
			)
		}
	}

	if len(advertiseAddrs) == 0 {
		return nil, nil, fmt.Errorf("unable to determine which IP address(es) to advertise: no global unicast addresses found")
	}

	return lis, advertiseAddrs, nil
}

// extractIP returns the IP address from a [net.Addr], if possible. If addr does
// not contain an IP address, it returns nil.
func extractIP(addr net.Addr) net.IP {
	// TODO: Why are both of these possible? Is it platform specific,
	// interface specific, address specific?
	switch addr := addr.(type) {
	case *net.IPNet:
		return addr.IP
	case *net.IPAddr:
		return addr.IP
	default:
		return nil
	}
}

// isAddressable returns true if the given IP can (potentially) be used by other
// nodes in the cluster as a way to contact this node directly using the given
// address family.
func isAddressable(addressFamily string, ip net.IP) bool {
	if addressFamily != "tcp" {
		gotV4 := ip.To4() != nil
		wantV4 := addressFamily == "tcp4"

		if gotV4 != wantV4 {
			return false
		}
	}

	return ip.IsGlobalUnicast()
}
