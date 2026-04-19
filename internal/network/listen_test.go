package network_test

import (
	"net"
	"slices"
	"strconv"
	"testing"

	. "github.com/dogmatiq/runkit/internal/network"
)

func TestListen(t *testing.T) {
	// Allocate a free port once so all table cases can use a known port value
	// without deriving it from lis.Addr() after the fact.
	probe, _, err := Listen("127.0.0.1:0", "")
	if err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(probe.Addr().(*net.TCPAddr).Port)
	probe.Close()

	t.Run("when port 0 is used", func(t *testing.T) {
		lis, addrs, err := Listen("127.0.0.1:0", "")
		if err != nil {
			t.Fatal(err)
		}
		defer lis.Close()

		t.Run("it advertises the auto-assigned port", func(t *testing.T) {
			assigned := strconv.Itoa(lis.Addr().(*net.TCPAddr).Port)
			want := []string{"127.0.0.1:" + assigned}
			if !slices.Equal(addrs, want) {
				t.Fatalf("unexpected advertise addresses: got %v, want %v", addrs, want)
			}
		})

		t.Run("it is reachable from all advertised addresses", func(t *testing.T) {
			for _, addr := range addrs {
				conn, err := net.Dial("tcp", addr)
				if err != nil {
					t.Fatalf("listener is not accepting connections at %s: %v", addr, err)
				}
				conn.Close()
			}
		})
	})

	cases := []struct {
		Name                 string
		ListenAddr           string
		AdvertiseAddr        string
		CheckAdvertisedHosts func(t *testing.T, hosts []string)
	}{
		{
			Name:       "when a specific IPv4 address is used",
			ListenAddr: "127.0.0.1:" + port,
			CheckAdvertisedHosts: func(t *testing.T, hosts []string) {
				t.Helper()
				if want := []string{"127.0.0.1"}; !slices.Equal(hosts, want) {
					t.Fatalf("unexpected advertised hosts: got %v, want %v", hosts, want)
				}
			},
		},
		{
			Name:       "when a specific IPv6 address is used",
			ListenAddr: "[::1]:" + port,
			CheckAdvertisedHosts: func(t *testing.T, hosts []string) {
				t.Helper()
				if want := []string{"::1"}; !slices.Equal(hosts, want) {
					t.Fatalf("unexpected advertised hosts: got %v, want %v", hosts, want)
				}
			},
		},
		{
			Name:       "when a hostname is used",
			ListenAddr: "localhost:" + port,
			CheckAdvertisedHosts: func(t *testing.T, hosts []string) {
				t.Helper()
				if want := []string{"localhost"}; !slices.Equal(hosts, want) {
					t.Fatalf("unexpected advertised hosts: got %v, want %v", hosts, want)
				}
			},
		},
		{
			Name:          "when an explicit advertise address is configured",
			ListenAddr:    "127.0.0.1:" + port,
			AdvertiseAddr: "localhost:" + port,
			CheckAdvertisedHosts: func(t *testing.T, hosts []string) {
				t.Helper()
				if want := []string{"localhost"}; !slices.Equal(hosts, want) {
					t.Fatalf("unexpected advertised hosts: got %v, want %v", hosts, want)
				}
			},
		},
		{
			Name:       "when the IPv4 wildcard address is used",
			ListenAddr: "0.0.0.0:" + port,
			CheckAdvertisedHosts: func(t *testing.T, hosts []string) {
				t.Helper()
				for _, host := range hosts {
					if ip := net.ParseIP(host); ip == nil || ip.To4() == nil {
						t.Fatalf("host %q is not an IPv4 address", host)
					}
				}
			},
		},
		{
			Name:       "when the IPv6 wildcard address is used",
			ListenAddr: "[::]:" + port,
			CheckAdvertisedHosts: func(t *testing.T, hosts []string) {
				t.Helper()
				for _, host := range hosts {
					if ip := net.ParseIP(host); ip == nil || ip.To4() != nil {
						t.Fatalf("host %q is not an IPv6 address", host)
					}
				}
			},
		},
		{
			Name:       "when the dual-stack wildcard address is used",
			ListenAddr: ":" + port,
			CheckAdvertisedHosts: func(t *testing.T, hosts []string) {
				t.Helper()
				for _, host := range hosts {
					if net.ParseIP(host) == nil {
						t.Fatalf("host %q is not a valid IP address", host)
					}
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			lis, addrs, err := Listen(tc.ListenAddr, tc.AdvertiseAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer lis.Close()

			t.Run("it advertises the expected addresses", func(t *testing.T) {
				if len(addrs) == 0 {
					t.Fatal("Listen() succeeded but returned no advertise addresses")
				}

				var hosts []string
				for _, addr := range addrs {
					host, p, err := net.SplitHostPort(addr)
					if err != nil {
						t.Fatalf("invalid address %q: %v", addr, err)
					}
					if p != port {
						t.Fatalf("address %q has wrong port, want %s", addr, port)
					}
					hosts = append(hosts, host)
				}
				if tc.CheckAdvertisedHosts != nil {
					tc.CheckAdvertisedHosts(t, hosts)
				}
			})

			t.Run("it is reachable from all advertised addresses", func(t *testing.T) {
				for _, addr := range addrs {
					conn, err := net.Dial("tcp", addr)
					if err != nil {
						t.Fatalf("listener is not accepting connections at %s: %v", addr, err)
					}
					conn.Close()
				}
			})
		})
	}
}
