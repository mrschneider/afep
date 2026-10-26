// Package interop verifies cross-language fixtures: artifacts signed by
// the Python reference must satisfy the Go verifier and vice versa.
package interop

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/identity"
	"google.golang.org/protobuf/proto"
)

const testdataDir = "../../python/testdata"

// fixed inputs: derived, not literal key material
func testSeed(label string) []byte {
	sum := sha256.Sum256([]byte(label))
	return sum[:]
}

type cardDoc struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	PublicKey   string `json:"public_key"`
	Endpoint    struct {
		GRPCAddress       string   `json:"grpc_address"`
		NatsSubjectPrefix string   `json:"nats_subject_prefix"`
		NatsGateways      []string `json:"nats_gateways"`
	} `json:"endpoint"`
	Capabilities []struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"capabilities"`
	SecurityClearances []string          `json:"security_clearances"`
	Metadata           map[string]string `json:"metadata"`
	IssuedAtUnix       int64             `json:"issued_at_unix"`
	ExpiresAtUnix      int64             `json:"expires_at_unix"`
	Signature          string            `json:"signature"`
}

type envDoc struct {
	TaskID        string `json:"task_id"`
	RootTaskID    string `json:"root_task_id"`
	Tenant        string `json:"tenant"`
	IssuerAgent   string `json:"issuer_agent"`
	AssigneeAgent string `json:"assignee_agent"`
	State         int32  `json:"state"`
	Capability    string `json:"capability"`
	PayloadType   string `json:"payload_type"`
	Payload       string `json:"payload"`
	Deadline      *struct {
		StartUnix   int64 `json:"start_unix"`
		ExpiresUnix int64 `json:"expires_unix"`
	} `json:"deadline"`
	Escalation *struct {
		FromDomain string `json:"from_domain"`
		ToDomain   string `json:"to_domain"`
		ReasonCode string `json:"reason_code"`
		Detail     string `json:"detail"`
	} `json:"escalation"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
	Hops          uint32 `json:"hops"`
	Signature     string `json:"signature"`
}

func cardToDoc(c *afepv1.AgentCard) cardDoc {
	var d cardDoc
	d.ID = c.GetId()
	d.DisplayName = c.GetDisplayName()
	d.PublicKey = hex.EncodeToString(c.GetPublicKey())
	d.Endpoint.GRPCAddress = c.GetEndpoint().GetGrpcAddress()
	d.Endpoint.NatsSubjectPrefix = c.GetEndpoint().GetNatsSubjectPrefix()
	d.Endpoint.NatsGateways = append([]string{}, c.GetEndpoint().GetNatsGateways()...)
	for _, cap := range c.GetCapabilities() {
		d.Capabilities = append(d.Capabilities, struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		}{cap.GetName(), cap.GetVersion()})
	}
	d.SecurityClearances = append([]string{}, c.GetSecurityClearances()...)
	d.Metadata = c.GetMetadata()
	if d.Metadata == nil {
		d.Metadata = map[string]string{}
	}
	d.IssuedAtUnix = c.GetIssuedAtUnix()
	d.ExpiresAtUnix = c.GetExpiresAtUnix()
	d.Signature = hex.EncodeToString(c.GetSignature())
	return d
}

func docToCard(d cardDoc) *afepv1.AgentCard {
	c := &afepv1.AgentCard{
		Id:            d.ID,
		DisplayName:   d.DisplayName,
		PublicKey:     mustHex(d.PublicKey),
		Endpoint:      &afepv1.Endpoint{GrpcAddress: d.Endpoint.GRPCAddress, NatsSubjectPrefix: d.Endpoint.NatsSubjectPrefix, NatsGateways: d.Endpoint.NatsGateways},
		Metadata:      d.Metadata,
		IssuedAtUnix:  d.IssuedAtUnix,
		ExpiresAtUnix: d.ExpiresAtUnix,
		Signature:     mustHex(d.Signature),
	}
	for _, cap := range d.Capabilities {
		c.Capabilities = append(c.Capabilities, &afepv1.Capability{Name: cap.Name, Version: cap.Version})
	}
	c.SecurityClearances = d.SecurityClearances
	return c
}

func envToDoc(e *afepv1.TaskEnvelope) envDoc {
	d := envDoc{
		TaskID: e.GetTaskId(), RootTaskID: e.GetRootTaskId(), Tenant: e.GetTenant(),
		IssuerAgent: e.GetIssuerAgent(), AssigneeAgent: e.GetAssigneeAgent(),
		State: int32(e.GetState()), Capability: e.GetCapability(),
		PayloadType: e.GetPayloadType(), Payload: hex.EncodeToString(e.GetPayload()),
		UpdatedAtUnix: e.GetUpdatedAtUnix(), Hops: e.GetHops(),
		Signature: hex.EncodeToString(e.GetSignature()),
	}
	if e.GetDeadline() != nil {
		d.Deadline = &struct {
			StartUnix   int64 `json:"start_unix"`
			ExpiresUnix int64 `json:"expires_unix"`
		}{e.GetDeadline().GetStartUnix(), e.GetDeadline().GetExpiresUnix()}
	}
	if e.GetEscalation() != nil {
		d.Escalation = &struct {
			FromDomain string `json:"from_domain"`
			ToDomain   string `json:"to_domain"`
			ReasonCode string `json:"reason_code"`
			Detail     string `json:"detail"`
		}{e.GetEscalation().GetFromDomain(), e.GetEscalation().GetToDomain(),
			e.GetEscalation().GetReasonCode(), e.GetEscalation().GetDetail()}
	}
	return d
}

func docToEnv(d envDoc) *afepv1.TaskEnvelope {
	e := &afepv1.TaskEnvelope{
		TaskId: d.TaskID, RootTaskId: d.RootTaskID, Tenant: d.Tenant,
		IssuerAgent: d.IssuerAgent, AssigneeAgent: d.AssigneeAgent,
		State: afepv1.TaskState(d.State), Capability: d.Capability,
		PayloadType: d.PayloadType, Payload: mustHex(d.Payload),
		UpdatedAtUnix: d.UpdatedAtUnix, Hops: d.Hops, Signature: mustHex(d.Signature),
	}
	if d.Deadline != nil {
		e.Deadline = &afepv1.Deadline{StartUnix: d.Deadline.StartUnix, ExpiresUnix: d.Deadline.ExpiresUnix}
	}
	if d.Escalation != nil {
		e.Escalation = &afepv1.Escalation{
			FromDomain: d.Escalation.FromDomain, ToDomain: d.Escalation.ToDomain,
			ReasonCode: d.Escalation.ReasonCode, Detail: d.Escalation.Detail,
		}
	}
	return e
}

func mustHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

// buildGoFixtures derives the canonical Go-side artifacts from a fixed
// seed with fixed timestamps (deterministic regeneration).
func buildGoFixtures(t *testing.T) (*afepv1.AgentCard, *afepv1.TaskEnvelope) {
	t.Helper()
	kp, err := identity.FromSeed(testSeed("afep-interop-fixture"))
	if err != nil {
		t.Fatal(err)
	}
	card, err := identity.NewCard("go/interop", "Go Interop", kp, func(c *afepv1.AgentCard) {
		c.Capabilities = []*afepv1.Capability{{Name: "vision.tag", Version: "9.9"}}
		c.SecurityClearances = []string{"tenant:acme"}
		c.Endpoint = &afepv1.Endpoint{GrpcAddress: "go.example.com:7700", NatsSubjectPrefix: "afep.go.interop"}
		c.Metadata = map[string]string{"description": "go interop agent", "version": "3.1.4"}
		c.IssuedAtUnix = 1793000000
		c.ExpiresAtUnix = 1793600000
	})
	if err != nil {
		t.Fatal(err)
	}
	env := &afepv1.TaskEnvelope{
		TaskId: "go-task-1", RootTaskId: "go-root-1", Tenant: "acme",
		IssuerAgent: "go/interop", AssigneeAgent: "go/worker",
		State: afepv1.TaskState_TASK_STATE_BID, Capability: "vision.tag",
		PayloadType: "application/json", Payload: []byte(`{"n":7}`),
		Deadline:      &afepv1.Deadline{StartUnix: 1793000050, ExpiresUnix: 1793003650},
		Escalation:    &afepv1.Escalation{FromDomain: "beta", ToDomain: "alpha", ReasonCode: "NO_BIDS", Detail: "hops=2"},
		UpdatedAtUnix: 1793000100,
		Hops:          2,
	}
	if err := identity.SignEnvelope(env, kp); err != nil {
		t.Fatal(err)
	}
	return card, env
}

func writeJSON(t *testing.T, name string, v any) {
	t.Helper()
	path := filepath.Join(testdataDir, name)
	if err := os.MkdirAll(testdataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readJSON(t *testing.T, name string, v any) bool {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(testdataDir, name))
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return true
}

// TestGenerateFixtures writes the Go-side fixtures (AFEP_FIXTURES=1).
func TestGenerateFixtures(t *testing.T) {
	if os.Getenv("AFEP_FIXTURES") != "1" {
		t.Skip("AFEP_FIXTURES not set")
	}
	card, env := buildGoFixtures(t)
	writeJSON(t, "go_card.json", cardToDoc(card))
	writeJSON(t, "go_envelope.json", envToDoc(env))
	t.Log("go fixtures written")
}

// TestGoFixturesSelfVerify re-derives and re-checks the committed Go files.
func TestGoFixturesSelfVerify(t *testing.T) {
	var cd cardDoc
	if !readJSON(t, "go_card.json", &cd) {
		t.Skip("go fixtures not generated yet")
	}
	card := docToCard(cd)
	if err := identity.VerifyCard(card); err != nil {
		t.Fatalf("committed go_card.json: %v", err)
	}
	// must match a fresh deterministic build
	want, _ := buildGoFixtures(t)
	if !proto.Equal(card, want) {
		t.Fatal("committed go_card.json diverged from canonical build")
	}
	var ed envDoc
	if !readJSON(t, "go_envelope.json", &ed) {
		t.Fatal("go_envelope.json missing while go_card.json exists")
	}
	env := docToEnv(ed)
	if err := identity.VerifyEnvelope(env, card); err != nil {
		t.Fatalf("committed go_envelope.json: %v", err)
	}
}

// TestPythonFixturesVerify is the cross-language check: Python-signed
// artifacts must satisfy the Go verifier.
func TestPythonFixturesVerify(t *testing.T) {
	var cd cardDoc
	if !readJSON(t, "python_card.json", &cd) {
		t.Skip("python fixtures not generated yet")
	}
	card := docToCard(cd)
	if err := identity.VerifyCard(card); err != nil {
		t.Fatalf("python_card.json: %v", err)
	}
	var ed envDoc
	if !readJSON(t, "python_envelope.json", &ed) {
		t.Fatal("python_envelope.json missing while python_card.json exists")
	}
	env := docToEnv(ed)
	if err := identity.VerifyEnvelope(env, card); err != nil {
		t.Fatalf("python_envelope.json: %v", err)
	}
	if env.GetTaskId() != "py-task-1" || env.GetHops() != 2 || env.GetState() != afepv1.TaskState_TASK_STATE_BID {
		t.Fatalf("python envelope fields: %+v", env)
	}
}
