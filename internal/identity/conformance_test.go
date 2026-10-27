package identity

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

type conformanceVectors struct {
	SeedHex string `json:"seed_hex"`
	Card    struct {
		ID            string `json:"id"`
		DisplayName   string `json:"display_name"`
		IssuedAtUnix  int64  `json:"issued_at_unix"`
		PublicKeyHex  string `json:"public_key_hex"`
		SignedBodyHex string `json:"signed_body_hex"`
		SignatureHex  string `json:"signature_hex"`
	} `json:"card"`
	Envelope struct {
		TaskID              string `json:"task_id"`
		RootTaskID          string `json:"root_task_id"`
		Tenant              string `json:"tenant"`
		IssuerAgent         string `json:"issuer_agent"`
		Capability          string `json:"capability"`
		PayloadType         string `json:"payload_type"`
		PayloadHex          string `json:"payload_hex"`
		DeadlineStartUnix   int64  `json:"deadline_start_unix"`
		DeadlineExpiresUnix int64  `json:"deadline_expires_unix"`
		State               int32  `json:"state"`
		SignedCoreHex       string `json:"signed_core_hex"`
		SignatureHex        string `json:"signature_hex"`
	} `json:"envelope"`
	Bid struct {
		AgentID              string `json:"agent_id"`
		TaskID               string `json:"task_id"`
		Score                uint64 `json:"score"`
		Rationale            string `json:"rationale"`
		ProposedDeadlineUnix int64  `json:"proposed_deadline_unix"`
		SignedBodyHex        string `json:"signed_body_hex"`
		SignatureHex         string `json:"signature_hex"`
	} `json:"bid"`
	Subject struct {
		Domain  string `json:"domain"`
		Agent   string `json:"agent"`
		Verb    string `json:"verb"`
		Subject string `json:"subject"`
		Dedup   string `json:"dedup_key"`
	} `json:"subject"`
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("hex: %v", err)
	}
	return b
}

// TestConformanceVectors byte-verifies the cross-language vectors
// against the Go implementation: schema or signed-core drift fails here
// AND in conformance/verify.py until vectors are regenerated on purpose.
func TestConformanceVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "conformance", "vectors.json"))
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var v conformanceVectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}

	kp, err := FromSeed(mustHex(t, v.SeedHex))
	if err != nil {
		t.Fatalf("FromSeed: %v", err)
	}
	if !bytes.Equal(kp.Public, mustHex(t, v.Card.PublicKeyHex)) {
		t.Fatal("public key does not match seed")
	}

	// card body + signature
	card := &afepv1.AgentCard{
		Id:           v.Card.ID,
		DisplayName:  v.Card.DisplayName,
		PublicKey:    mustHex(t, v.Card.PublicKeyHex),
		IssuedAtUnix: v.Card.IssuedAtUnix,
	}
	body, err := cardBody(card)
	if err != nil {
		t.Fatalf("cardBody: %v", err)
	}
	if hex.EncodeToString(body) != v.Card.SignedBodyHex {
		t.Fatal("card signed body drifted")
	}
	card.Signature = mustHex(t, v.Card.SignatureHex)
	if err := VerifyCard(card); err != nil {
		t.Fatalf("card signature: %v", err)
	}

	// envelope signed core + signature
	payload, _ := hex.DecodeString(v.Envelope.PayloadHex)
	env := &afepv1.TaskEnvelope{
		TaskId:      v.Envelope.TaskID,
		RootTaskId:  v.Envelope.RootTaskID,
		Tenant:      v.Envelope.Tenant,
		IssuerAgent: v.Envelope.IssuerAgent,
		Capability:  v.Envelope.Capability,
		PayloadType: v.Envelope.PayloadType,
		Payload:     payload,
		State:       afepv1.TaskState(v.Envelope.State),
		Deadline: &afepv1.Deadline{
			StartUnix:   v.Envelope.DeadlineStartUnix,
			ExpiresUnix: v.Envelope.DeadlineExpiresUnix,
		},
	}
	core, err := envelopeBody(env)
	if err != nil {
		t.Fatalf("envelopeBody: %v", err)
	}
	if hex.EncodeToString(core) != v.Envelope.SignedCoreHex {
		t.Fatal("envelope signed core drifted")
	}
	env.Signature = mustHex(t, v.Envelope.SignatureHex)
	if err := VerifyEnvelope(env, card); err != nil {
		t.Fatalf("envelope signature: %v", err)
	}

	// tail exclusion: mutating relay-mutable fields changes no core byte
	env.State = afepv1.TaskState_TASK_STATE_ESCALATED
	env.Hops = 7
	env.AssigneeAgent = "someone/else"
	core2, err := envelopeBody(env)
	if err != nil {
		t.Fatalf("envelopeBody (mutated): %v", err)
	}
	if hex.EncodeToString(core2) != v.Envelope.SignedCoreHex {
		t.Fatal("tail mutation changed the signed core (exclusion broken)")
	}

	// bid body + signature
	bid := &afepv1.Bid{
		AgentId:              v.Bid.AgentID,
		TaskId:               v.Bid.TaskID,
		Score:                v.Bid.Score,
		Rationale:            v.Bid.Rationale,
		ProposedDeadlineUnix: v.Bid.ProposedDeadlineUnix,
	}
	bbody, err := bidBody(bid)
	if err != nil {
		t.Fatalf("bidSignedBytes: %v", err)
	}
	if hex.EncodeToString(bbody) != v.Bid.SignedBodyHex {
		t.Fatal("bid signed body drifted")
	}
	bid.Signature = mustHex(t, v.Bid.SignatureHex)
	if err := VerifyBid(bid, card); err != nil {
		t.Fatalf("bid signature: %v", err)
	}

	// subject + dedup key formulas (mirrors internal/bus/nats)
	s := v.Subject
	builtSubject := "afep." + s.Domain + "." + s.Agent + "." + s.Verb
	if builtSubject != s.Subject {
		t.Fatalf("subject formula drifted: %s != %s", builtSubject, s.Subject)
	}
	builtDedup := v.Envelope.TaskID + ":" + afepv1.TaskState(v.Envelope.State).String() + ":0"
	if builtDedup != s.Dedup {
		t.Fatalf("dedup key formula drifted: %s != %s", builtDedup, s.Dedup)
	}
}
