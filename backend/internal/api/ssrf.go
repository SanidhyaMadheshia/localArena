package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"syscall"
	"time"
)

// MaxAvatarBytes bounds an imported avatar.
const MaxAvatarBytes = 512 << 10

// UnsafeURLError is a request the SSRF guard refused.
type UnsafeURLError struct{ Reason string }

func (e *UnsafeURLError) Error() string { return "url not allowed: " + e.Reason }

// blockedAddr reports whether connecting to ip could reach something the
// public internet should not: loopback, private ranges, link-local (cloud
// metadata 169.254.169.254), CGNAT, multicast, unspecified, and IPv6
// equivalents (including IPv4-mapped forms).
func blockedAddr(ip netip.Addr) bool {
	ip = ip.Unmap()
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() || ip.IsUnspecified() || ip.IsInterfaceLocalMulticast() {
		return true
	}
	for _, p := range []string{"100.64.0.0/10", "192.0.0.0/24", "198.18.0.0/15", "240.0.0.0/4", "64:ff9b::/96"} {
		if netip.MustParsePrefix(p).Contains(ip) {
			return true
		}
	}
	return false
}

// SafeClient returns an HTTP client that refuses non-public destinations.
// The check runs in the dialer's Control hook, i.e. on the *resolved* address
// at connect time, for every connection including redirects. Checking the
// hostname before the request would be bypassed by DNS rebinding (resolve to a
// public IP during the check, a private one when connecting).
func SafeClient() *http.Client {
	d := &net.Dialer{Timeout: 2 * time.Second, Control: func(_, address string, _ syscall.RawConn) error {
		ap, err := netip.ParseAddrPort(address)
		if err != nil || blockedAddr(ap.Addr()) {
			return &UnsafeURLError{Reason: "destination address is not public"}
		}
		if ap.Port() != 443 && ap.Port() != 80 {
			return &UnsafeURLError{Reason: "port not allowed"}
		}
		return nil
	}}
	tr := &http.Transport{DialContext: d.DialContext, Proxy: nil, MaxIdleConns: 4, ResponseHeaderTimeout: 2 * time.Second,
		TLSHandshakeTimeout: 2 * time.Second}
	return &http.Client{Transport: tr, Timeout: 3 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return errors.New("too many redirects")
		}
		if req.URL.Scheme != "https" {
			return &UnsafeURLError{Reason: "redirect to non-https"}
		}
		return nil
	}}
}

// FetchAvatar downloads an image from a user-supplied https URL with the SSRF
// guard, a size cap, and content sniffing (the declared Content-Type is not
// trusted). Returns the bytes and the sniffed type.
func FetchAvatar(ctx context.Context, cl *http.Client, raw string) ([]byte, string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || len(raw) > 2048 {
		return nil, "", &UnsafeURLError{Reason: "must be an https URL without credentials"}
	}
	if cl == nil {
		cl = SafeClient()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, "", &UnsafeURLError{Reason: "bad url"}
	}
	resp, err := cl.Do(req)
	if err != nil {
		var ue *UnsafeURLError
		if errors.As(err, &ue) {
			return nil, "", ue
		}
		return nil, "", fmt.Errorf("fetch: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("fetch: status %d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, MaxAvatarBytes+1))
	if err != nil {
		return nil, "", err
	}
	if len(b) > MaxAvatarBytes {
		return nil, "", errors.New("avatar too large")
	}
	switch ct := http.DetectContentType(b); ct {
	case "image/png", "image/jpeg", "image/webp":
		return b, ct, nil
	default:
		return nil, "", fmt.Errorf("not an allowed image type (%s)", ct)
	}
}
