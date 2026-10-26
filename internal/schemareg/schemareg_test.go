package schemareg

import (
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"google.golang.org/protobuf/proto"
)

func echoBytes(t *testing.T, text string) []byte {
	t.Helper()
	body, err := proto.Marshal(&afepv1.EchoPayload{Text: text, Seq: 7})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestValidateEcho(t *testing.T) {
	r := New()
	if err := r.Validate(MediaTypeEcho, echoBytes(t, "hello")); err != nil {
		t.Fatalf("valid echo rejected: %v", err)
	}
}

func TestUnregisteredType(t *testing.T) {
	r := New()
	if err := r.Validate("application/x-nonsense", []byte("x")); err == nil {
		t.Fatal("unregistered type must fail")
	}
}

func TestBodyNotMatchingSchema(t *testing.T) {
	r := New()
	// field 1 with varint wire type: EchoPayload.text expects length-delimited
	if err := r.Validate(MediaTypeEcho, []byte{0x08, 0x01}); err == nil {
		t.Fatal("wire-mismatched body must fail")
	}
}

func TestJSONBody(t *testing.T) {
	r := New()
	if err := r.Validate(MediaTypeJSON, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("valid json rejected: %v", err)
	}
	if err := r.Validate(MediaTypeJSON, []byte(`{broken`)); err == nil {
		t.Fatal("broken json must fail")
	}
}

func TestPayloadWithoutTypeFails(t *testing.T) {
	r := New()
	if err := r.Validate("", []byte("orphan")); err == nil {
		t.Fatal("payload without type must fail")
	}
	if err := r.Validate("", nil); err != nil {
		t.Fatalf("empty pair must pass: %v", err)
	}
}

func TestTypeDeclarationWithoutBody(t *testing.T) {
	r := New()
	if err := r.Validate(MediaTypeEcho, nil); err != nil {
		t.Fatalf("declaration must pass: %v", err)
	}
}

func TestDuplicateRegistrationFails(t *testing.T) {
	r := New()
	if err := r.Register(MediaTypeEcho, func() proto.Message { return &afepv1.EchoPayload{} }); err == nil {
		t.Fatal("duplicate must fail")
	}
}

func TestValidateEnvelope(t *testing.T) {
	r := New()
	env := &afepv1.TaskEnvelope{TaskId: "t", PayloadType: MediaTypeEcho, Payload: echoBytes(t, "x")}
	if err := r.ValidateEnvelope(env); err != nil {
		t.Fatal(err)
	}
	env.PayloadType = "application/x-nope"
	if err := r.ValidateEnvelope(env); err == nil {
		t.Fatal("unregistered envelope type must fail")
	}
	if err := r.ValidateEnvelope(nil); err == nil {
		t.Fatal("nil envelope must fail")
	}
}
