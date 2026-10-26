package natsbus

import "testing"

func TestSubjectRoundtrip(t *testing.T) {
	s, err := Subject("edge", "node7", VerbEnvelopes)
	if err != nil {
		t.Fatal(err)
	}
	if s != "afep.edge.node7.envelopes" {
		t.Fatalf("subject = %q", s)
	}
	domain, agent, verb, err := ParseSubject(s)
	if err != nil {
		t.Fatal(err)
	}
	if domain != "edge" || agent != "node7" || verb != VerbEnvelopes {
		t.Fatalf("parse = %q %q %q", domain, agent, verb)
	}
}

func TestSubjectValidation(t *testing.T) {
	if _, err := Subject("", "x", VerbEnvelopes); err == nil {
		t.Fatal("empty domain must fail")
	}
	if _, err := Subject("edge", "a b", VerbEnvelopes); err == nil {
		t.Fatal("space must fail")
	}
	if _, err := Subject("edge", "n.1", VerbEnvelopes); err == nil {
		t.Fatal("dot must fail (subject is fixed 4 segments)")
	}
	if _, err := Subject("edge", "n1", ""); err == nil {
		t.Fatal("empty verb must fail")
	}
}

func TestSubjectWildcardAllowed(t *testing.T) {
	// Wildcards are legal in subscription filters.
	s, err := Subject("edge", Wildcard, VerbEnvelopes)
	if err != nil {
		t.Fatal(err)
	}
	if s != "afep.edge.*.envelopes" {
		t.Fatalf("subject = %q", s)
	}
}

func TestParseSubjectRejectsGarbage(t *testing.T) {
	for _, s := range []string{"foo.bar", "afep.a.b", "afep.a.b.c.d", ""} {
		if _, _, _, err := ParseSubject(s); err == nil {
			t.Errorf("parse(%q) must fail", s)
		}
	}
}
