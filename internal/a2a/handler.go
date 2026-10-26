package a2a

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

// Handler serves the A2A Agent Card at both well-known URIs with the
// caching headers the specification recommends (Cache-Control + ETag +
// conditional 304).
func Handler(card *afepv1.AgentCard) http.Handler {
	body, err := json.Marshal(Card(card))
	if err != nil {
		// Card() only produces marshalable data; degrade to an error body.
		body = []byte(`{"error":"card marshal failed"}`)
	}
	sum := sha256.Sum256(body)
	etag := `"` + hex.EncodeToString(sum[:]) + `"`

	mux := http.NewServeMux()
	serve := func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}
	mux.HandleFunc(WellKnownPath, serve)
	mux.HandleFunc(LegacyWellKnownPath, serve)
	return mux
}
