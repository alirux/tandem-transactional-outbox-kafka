package tandem

import (
	"net/url"
	"strings"
)

// IsDataSchemaURI is a candidate contract rule for the dataschema header: an RFC 3986 URI reference
// in ASCII, with valid percent-encoding, at most one '#', a well-formed scheme when one is present,
// and a numeric port. Meant to be never looser than java.net.URI.
func IsDataSchemaURI(s string) bool {
	const allowed = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:/?#[]@!$&'()*+,;=%"
	for i := 0; i < len(s); i++ {
		if !strings.ContainsRune(allowed, rune(s[i])) {
			return false
		}
		if s[i] == '%' && (i+2 >= len(s) || !isHex(s[i+1]) || !isHex(s[i+2])) {
			return false
		}
	}
	if strings.Count(s, "#") > 1 {
		return false
	}
	if c := strings.IndexAny(s, ":/?#"); c >= 0 && s[c] == ':' {
		if c == 0 || !isAlpha(s[0]) {
			return false
		}
		for i := 1; i < c; i++ {
			if !isAlpha(s[i]) && !isDigit(s[i]) && !strings.ContainsRune("+-.", rune(s[i])) {
				return false
			}
		}
	}
	u, err := url.Parse(s)
	if err != nil {
		return false
	}
	if u.Scheme != "" && len(s) == len(u.Scheme)+1 {
		return false // a scheme with nothing after it
	}
	if u.Host == "" && u.Path == "" && (strings.HasPrefix(s, "//") || strings.Contains(s, "://")) {
		return false // an empty authority and nothing after it
	}
	if p := u.Port(); p != "" {
		for i := 0; i < len(p); i++ {
			if !isDigit(p[i]) {
				return false
			}
		}
	}
	return true
}

func isHex(c byte) bool   { return isDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') }
func isAlpha(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }
func isDigit(c byte) bool { return c >= '0' && c <= '9' }
