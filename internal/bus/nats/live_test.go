package natsbus

import (
	"context"
	"testing"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/quictransport"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

// TestLiveJetstreamRound verifies the Jetstream configuration against a
// REAL broker: it skips when none is reachable (CI provides one as a
// service; `make nats-up` provides one locally).
func TestLiveJetstreamRound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b, err := Dial(ctx, "nats://127.0.0.1:4222")
	if err != nil {
		t.Skipf("no local broker: %v", err)
	}
	defer b.Close()

	// stream bounds: MaxMsgs + AllowDirect + S2 live on the server
	st, err := b.js.Stream(ctx, StreamName)
	if err != nil {
		t.Fatalf("tasks stream: %v", err)
	}
	info, err := st.Info(ctx)
	if err != nil {
		t.Fatalf("tasks info: %v", err)
	}
	if info.Config.MaxMsgs == 0 {
		t.Fatal("MaxMsgs not applied")
	}
	if !info.Config.AllowDirect {
		t.Fatal("AllowDirect not applied")
	}
	if info.Config.Compression != jetstream.S2Compression {
		t.Fatalf("Compression = %v, want S2", info.Config.Compression)
	}

	// bid archive stream accepts
	if _, err := b.js.Stream(ctx, BidStreamName); err != nil {
		t.Fatalf("bid stream: %v", err)
	}

	// consumer: tail start, ack window, server backoff, pending cap
	cc, err := b.SubscribeEnvelopes(ctx, "local", "live-test", func(*afepv1.TaskEnvelope) error { return nil })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cc.Stop()
	if b.statsCons == nil {
		t.Fatal("statsCons not set")
	}
	ci, err := b.statsCons.Info(ctx)
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	if ci.Config.DeliverPolicy != jetstream.DeliverNewPolicy {
		t.Fatalf("DeliverPolicy = %v, want DeliverNew", ci.Config.DeliverPolicy)
	}
	if ci.Config.AckWait != 60*time.Second {
		t.Fatalf("AckWait = %v", ci.Config.AckWait)
	}
	if len(ci.Config.BackOff) != 5 {
		t.Fatalf("BackOff len = %d, want 5", len(ci.Config.BackOff))
	}
	if ci.Config.MaxAckPending != 100 {
		t.Fatalf("MaxAckPending = %d, want 100", ci.Config.MaxAckPending)
	}

	// KV handle cached + watch mirror: prove the WATCH-fed path by
	// evicting the local entry and forcing a real KV event through the
	// raw handle (which bypasses PutCard's instant cache update).
	card := &afepv1.AgentCard{Id: "tenant/live", PublicKey: make([]byte, 32)}
	if err := b.PutCard(ctx, card); err != nil {
		t.Fatalf("put card: %v", err)
	}
	if got, ok, err := b.GetCard(ctx, "tenant/live"); err != nil || !ok || got.GetId() != "tenant/live" {
		t.Fatalf("get card: ok=%v err=%v", ok, err)
	}
	b.kvL.Lock()
	delete(b.cardCache, "tenant/live")
	b.kvL.Unlock()
	rawCard, err := proto.MarshalOptions{Deterministic: true}.Marshal(card)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.kv.Put(ctx, "tenant/live", rawCard); err != nil { // watch event
		t.Fatal(err)
	}
	waitForMirror(t, b, "tenant/live", false)

	// revocation: kv-backed, mirrored with the same watcher
	if err := b.RevokeCard(ctx, "tenant/live"); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if revoked, err := b.IsRevoked(ctx, "tenant/live"); err != nil || !revoked {
		t.Fatalf("isRevoked: revoked=%v err=%v", revoked, err)
	}
	b.kvL.Lock()
	delete(b.revokedCache, "tenant/live")
	b.kvL.Unlock()
	if _, err := b.kv.Put(ctx, RevokedPrefix+"tenant/live", []byte(time.Now().UTC().Format(time.RFC3339))); err != nil { // watch event
		t.Fatal(err)
	}
	waitForMirror(t, b, "tenant/live", true)

	// bid lands in the archive; live metric counts the publish
	if err := b.PublishBid("local", "tenant/live", &afepv1.Bid{
		TaskId: "task-1", AgentId: "tenant/live", Score: 1,
	}); err != nil {
		t.Fatalf("publish bid: %v", err)
	}
	bs, err := b.js.Stream(ctx, BidStreamName)
	if err != nil {
		t.Fatalf("bid stream: %v", err)
	}
	binfo, err := bs.Info(ctx)
	if err != nil {
		t.Fatalf("bid info: %v", err)
	}
	if binfo.State.Msgs == 0 {
		t.Fatal("bid archive empty: archive publish missing")
	}

	// connection-change notification fires for the healthy connect
	notified := make(chan bool, 4)
	b.NotifyConnChanges(func(up bool) { notified <- up })
	b.kvL.RLock()
	onConn := b.onConn
	b.kvL.RUnlock()
	if onConn == nil {
		t.Fatal("NotifyConnChanges did not register")
	}

	// persisted identity round-trip (used by -quic-ca pinning)
	dir := t.TempDir()
	if _, err := quictransport.LoadOrCreateCert(dir+"/id.crt", dir+"/id.key"); err != nil {
		t.Fatalf("LoadOrCreateCert: %v", err)
	}
	if _, err := quictransport.LoadOrCreateCert(dir+"/id.crt", dir+"/id.key"); err != nil {
		t.Fatalf("LoadOrCreateCert reload: %v", err)
	}
	t.Logf("LIVE OK: allowDirect=%v compression=%v deliver=%v ackwait=%v backoff=%d cap=%d bidMsgs=%d",
		info.Config.AllowDirect, info.Config.Compression,
		ci.Config.DeliverPolicy, ci.Config.AckWait, len(ci.Config.BackOff),
		ci.Config.MaxAckPending, binfo.State.Msgs)
}

func waitForMirror(t *testing.T, b *Bus, id string, wantRevoked bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		b.kvL.RLock()
		var hit bool
		if wantRevoked {
			hit = b.revokedCache[id]
		} else {
			_, hit = b.cardCache[id]
		}
		b.kvL.RUnlock()
		if hit {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("mirror did not repopulate (revoked=%v) for %s within 5s", wantRevoked, id)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
