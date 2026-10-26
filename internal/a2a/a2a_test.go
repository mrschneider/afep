package a2a

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/identity"
)

func testCard(t *testing.T) *afepv1.AgentCard {
	t.Helper()
	kp, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	card, err := identity.NewCard("acme/echo-bot", "Echo Bot", kp, func(c *afepv1.AgentCard) {
		c.Endpoint = &afepv1.Endpoint{GrpcAddress: "echo.example.com:7700"}
		c.Capabilities = []*afepv1.Capability{
			{Name: "vision.tag", Version: "1.2"},
			{Name: "llm.infer", Version: "0.9"},
		}
		c.Metadata = map[string]string{"description": "Echoes everything.", "version": "2.1.0"}
	})
	if err != nil {
		t.Fatal(err)
	}
	return card
}

func TestCardRequiredFields(t *testing.T) {
	raw, err := json.Marshal(Card(testCard(t)))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	// required fields per A2A v1.0
	for _, k := range []string{"name", "description", "version", "supportedInterfaces",
		"capabilities", "defaultInputModes", "defaultOutputModes", "skills"} {
		if _, ok := doc[k]; !ok {
			t.Fatalf("missing required field %q", k)
		}
	}
	if doc["name"] != "Echo Bot" || doc["version"] != "2.1.0" {
		t.Fatalf("name/version = %v/%v", doc["name"], doc["version"])
	}
	if doc["description"] != "Echoes everything." {
		t.Fatalf("description = %v", doc["description"])
	}
}

func TestSupportedInterfaceIsGRPC(t *testing.T) {
	c := Card(testCard(t))
	if len(c.SupportedInterfaces) != 1 {
		t.Fatalf("interfaces = %d", len(c.SupportedInterfaces))
	}
	iface := c.SupportedInterfaces[0]
	if iface.ProtocolBinding != "GRPC" || iface.ProtocolVersion != ProtocolVersion {
		t.Fatalf("iface = %+v", iface)
	}
	if iface.URL != "grpc://echo.example.com:7700" {
		t.Fatalf("url = %q", iface.URL)
	}
}

func TestCapabilitiesStreaming(t *testing.T) {
	c := Card(testCard(t))
	if !c.Capabilities.Streaming || c.Capabilities.PushNotifications {
		t.Fatalf("capabilities = %+v", c.Capabilities)
	}
	if c.Capabilities.Extensions == nil {
		t.Fatal("extensions must be an array (canonicalization removes empty ones)")
	}
}

func TestSkillsFromCapabilities(t *testing.T) {
	c := Card(testCard(t))
	if len(c.Skills) != 2 {
		t.Fatalf("skills = %d", len(c.Skills))
	}
	if c.Skills[0].ID != "vision-tag" {
		t.Fatalf("skill id = %q, want vision-tag", c.Skills[0].ID)
	}
	if c.Skills[0].Name != "vision.tag" || c.Skills[0].Tags[0] != "afep" || c.Skills[0].Tags[1] != "vision" {
		t.Fatalf("skill = %+v", c.Skills[0])
	}
}

func TestStateMapping(t *testing.T) {
	cases := []struct {
		in   afepv1.TaskState
		want string
	}{
		{afepv1.TaskState_TASK_STATE_UNSPECIFIED, StateUnspecified},
		{afepv1.TaskState_TASK_STATE_PROBE, StateSubmitted},
		{afepv1.TaskState_TASK_STATE_BID, StateSubmitted},
		{afepv1.TaskState_TASK_STATE_COMMIT, StateSubmitted},
		{afepv1.TaskState_TASK_STATE_RUNNING, StateWorking},
		{afepv1.TaskState_TASK_STATE_DONE, StateCompleted},
		{afepv1.TaskState_TASK_STATE_FAILED, StateFailed},
		{afepv1.TaskState_TASK_STATE_ESCALATED, StateInputRequired},
	}
	for _, tc := range cases {
		if got := StateOf(tc.in); got != tc.want {
			t.Errorf("StateOf(%s) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

func TestHandlerServesBothWellKnownPaths(t *testing.T) {
	h := Handler(testCard(t))
	for _, path := range []string{WellKnownPath, LegacyWellKnownPath} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: code %d", path, rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("%s: content-type %q", path, ct)
		}
		if rec.Header().Get("ETag") == "" || !strings.Contains(rec.Header().Get("Cache-Control"), "max-age") {
			t.Fatalf("%s: caching headers missing: %v", path, rec.Header())
		}
		var doc map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil || doc["skills"] == nil {
			t.Fatalf("%s: body invalid: %v", path, err)
		}
	}
}

func TestHandlerConditionalGet(t *testing.T) {
	h := Handler(testCard(t))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, WellKnownPath, nil))
	etag := rec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("no etag")
	}

	rec2 := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, WellKnownPath, nil)
	req.Header.Set("If-None-Match", etag)
	h.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusNotModified {
		t.Fatalf("conditional get = %d, want 304", rec2.Code)
	}
}

func TestHandlerUnknownPath404(t *testing.T) {
	h := Handler(testCard(t))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/other.json", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code = %d", rec.Code)
	}
}
