package natsbus

import "testing"

// FuzzParseSubject locks the subject parser against panics and the
// builder round-trip (the builder is allowed to be stricter).
func FuzzParseSubject(f *testing.F) {
	f.Add("afep.local.tenant/agent.envelopes")
	f.Add("afep.a.b.c")
	f.Add("")
	f.Add("afep.a.b.c.d")
	f.Add("afep..b.c")
	f.Fuzz(func(t *testing.T, s string) {
		d, a, v, err := ParseSubject(s)
		if err != nil {
			return
		}
		out, err := Subject(d, a, v)
		if err != nil {
			return // builder stricter than parser: allowed
		}
		if out != s {
			t.Fatalf("roundtrip mismatch: built %q from parsed %q", out, s)
		}
	})
}
