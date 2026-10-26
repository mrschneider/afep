// Package a2a adapts AFEP artifacts to the A2A (Agent-to-Agent) protocol
// for discovery and lifecycle interop: the v1.0 Agent Card JSON at the
// well-known URI, and the AFEP -> A2A task-state mapping.
//
// Scope honesty: card-level discovery and state vocabulary only. AFEP does
// not implement A2A's SendMessage/Task RPC surface — see ARCHITECTURE.md.
package a2a

import (
	"fmt"
	"strings"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

// WellKnownPath is the discovery URI from the A2A v1.0 specification.
const WellKnownPath = "/.well-known/agent-card.json"

// LegacyWellKnownPath is the v0.3 discovery URI, kept as an alias.
const LegacyWellKnownPath = "/.well-known/agent.json"

// ProtocolVersion is the A2A interface version this adapter emits.
const ProtocolVersion = "1.0"

// Interface is one entry of supportedInterfaces (preference order).
type Interface struct {
	URL             string `json:"url"`
	ProtocolBinding string `json:"protocolBinding"`
	ProtocolVersion string `json:"protocolVersion"`
}

// Capabilities mirrors the A2A capabilities object.
type Capabilities struct {
	Streaming         bool     `json:"streaming"`
	PushNotifications bool     `json:"pushNotifications"`
	Extensions        []string `json:"extensions"`
}

// Skill is one AgentSkill entry.
type Skill struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
}

// AgentCard is the A2A v1.0 JSON document (required fields only; optional
// security/signature objects are not emitted by this adapter).
type AgentCard struct {
	Name                string       `json:"name"`
	Description         string       `json:"description"`
	SupportedInterfaces []Interface  `json:"supportedInterfaces"`
	Version             string       `json:"version"`
	Capabilities        Capabilities `json:"capabilities"`
	DefaultInputModes   []string     `json:"defaultInputModes"`
	DefaultOutputModes  []string     `json:"defaultOutputModes"`
	Skills              []Skill      `json:"skills"`
}

// Built-in payload modes this scaffold validates at its schema boundary.
var (
	defaultInputModes  = []string{"application/json", "application/x-afep-echo+proto"}
	defaultOutputModes = []string{"application/json", "application/x-afep-echo+proto"}
)

// Card derives the A2A v1.0 document from an AFEP AgentCard.
func Card(card *afepv1.AgentCard) *AgentCard {
	name := card.GetDisplayName()
	if name == "" {
		name = card.GetId()
	}
	description := fmt.Sprintf("AFEP agent %s", card.GetId())
	if d := card.GetMetadata()["description"]; d != "" {
		description = d
	}
	version := card.GetMetadata()["version"]
	if version == "" {
		version = "0.0.0"
	}

	out := &AgentCard{
		Name:        name,
		Description: description,
		Version:     version,
		SupportedInterfaces: []Interface{{
			URL:             "grpc://" + card.GetEndpoint().GetGrpcAddress(),
			ProtocolBinding: "GRPC",
			ProtocolVersion: ProtocolVersion,
		}},
		Capabilities: Capabilities{
			Streaming:         true, // Probe stream + bi-di TaskStream
			PushNotifications: false,
			Extensions:        []string{},
		},
		DefaultInputModes:  defaultInputModes,
		DefaultOutputModes: defaultOutputModes,
	}
	for _, c := range card.GetCapabilities() {
		out.Skills = append(out.Skills, Skill{
			ID:          skillID(c.GetName()),
			Name:        c.GetName(),
			Description: fmt.Sprintf("AFEP capability %s v%s", c.GetName(), c.GetVersion()),
			Tags:        []string{"afep", firstSegment(c.GetName())},
		})
	}
	return out
}

// skillID maps a dotted capability name onto an A2A skill id
// ("vision.tag" -> "vision-tag").
func skillID(name string) string {
	return strings.ReplaceAll(name, ".", "-")
}

func firstSegment(name string) string {
	if i := strings.IndexByte(name, '.'); i > 0 {
		return name[:i]
	}
	return name
}
