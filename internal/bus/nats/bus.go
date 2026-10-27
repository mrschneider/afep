package natsbus

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/metrics"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

// Stream and KV bucket names shared across federated sites.
const (
	StreamName    = "AFEP_TASKS"
	BidStreamName = "AFEP_BIDS"
	CardBucket    = "afep_cards"
	// RevokedPrefix namespaces KV keys holding revoked card ids. NATS KV
	// forbids ":" in keys; "/" is valid (card ids already contain it).
	RevokedPrefix = "revoked/"
)

// Bus is the JetStream-backed message mesh.
type Bus struct {
	nc *nats.Conn
	js jetstream.JetStream
	// kv is created ONCE in Dial: CreateKeyValue runs CreateStream
	// server-side (kv.go:540), so the old per-call handle cost a
	// metadata roundtrip on EVERY card read/write.
	kv  jetstream.KeyValue
	kvL sync.RWMutex
	// cardCache mirrors the KV bucket fed by WatchAll: issuer lookups
	// on the per-envelope verify path read it with zero RTT.
	cardCache map[string][]byte
	kvStop    context.CancelFunc
	// statsCons (set at subscribe) feeds the observability ticker.
	statsCons jetstream.Consumer
	// revokedCache mirrors kv keys "revoked/<id>" (fail-closed check
	// with authoritative kv fallback on miss).
	revokedCache map[string]bool
	onMu         sync.RWMutex
	onConn       func(bool)
}

// Dial connects to NATS and (re)creates the AFEP stream.
func Dial(ctx context.Context, url string, extra ...nats.Option) (*Bus, error) {
	var b *Bus
	nc, err := nats.Connect(url, append([]nats.Option{
		nats.Name("afep"),
		// Explicit reconnect policy + observability: a federation daemon
		// must survive broker restarts loudly, never silently.
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Printf("natsbus: disconnected: %v", err)
			notifyConn(b, false)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			log.Printf("natsbus: reconnected")
			notifyConn(b, true)
		}),
		nats.ClosedHandler(func(c *nats.Conn) {
			if err := c.LastError(); err != nil {
				log.Printf("natsbus: connection closed: %v", err)
			} else {
				log.Printf("natsbus: connection closed")
			}
			notifyConn(b, false)
		}),
	}, extra...)...)
	if err != nil {
		return nil, fmt.Errorf("afep/natsbus: connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("afep/natsbus: jetstream: %w", err)
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        StreamName,
		Subjects:    []string{"afep.*.*.envelopes"},
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.LimitsPolicy,
		MaxAge:      24 * time.Hour,
		MaxBytes:    1 << 30,   // 1 GiB cap: an inundation must not fill the disk
		MaxMsgs:     1_000_000, // third cut dimension beside age and bytes
		Discard:     jetstream.DiscardOld,
		Compression: jetstream.S2Compression, // proto payloads compress well
		AllowDirect: true,                    // ops reads by sequence without a consumer
		Duplicates:  2 * time.Minute,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("afep/natsbus: stream %w", err)
	}
	// Bid archive: the live path below stays ephemeral core NATS, this
	// stream only makes ranks reconstructable after a restart.
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        BidStreamName,
		Subjects:    []string{"afep.*.*.bids"},
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.LimitsPolicy,
		MaxAge:      24 * time.Hour,
		MaxBytes:    256 << 20,
		MaxMsgs:     500_000,
		Discard:     jetstream.DiscardOld,
		Compression: jetstream.S2Compression,
		AllowDirect: true,
		Duplicates:  2 * time.Minute,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("afep/natsbus: bid stream %w", err)
	}
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: CardBucket,
		TTL:    7 * 24 * time.Hour,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("afep/natsbus: kv bucket: %w", err)
	}
	b = &Bus{
		nc: nc, js: js, kv: kv,
		cardCache:    make(map[string][]byte),
		revokedCache: make(map[string]bool),
	}
	b.startCardWatcher(ctx)
	return b, nil
}

// Close tears down the NATS connection.
func (b *Bus) Close() {
	if b.nc == nil {
		return
	}
	if b.kvStop != nil {
		b.kvStop()
	}
	// Drain flushes buffered publishes and unsubscribes consumers so a
	// shutdown does not drop messages already handed to the client;
	// bounded so a wedged connection cannot hang the daemon.
	done := make(chan struct{})
	go func() {
		_ = b.nc.Drain()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		b.nc.Close()
	}
}

// startCardWatcher mirrors the card bucket into cardCache. WatchAll
// pushes current values first (nil entry ends the snapshot), then every
// update — issuer verification on the envelope path stops paying RTTs.
func (b *Bus) startCardWatcher(ctx context.Context) {
	wctx, cancel := context.WithCancel(ctx)
	b.kvStop = cancel
	go func() {
		for {
			w, err := b.kv.WatchAll(wctx)
			if err != nil {
				if wctx.Err() != nil {
					return
				}
				log.Printf("natsbus: card watch: %v (retrying)", err)
				select {
				case <-wctx.Done():
					return
				case <-time.After(2 * time.Second):
				}
				continue
			}
			for entry := range w.Updates() {
				if entry == nil {
					continue // end of the initial snapshot
				}
				key := entry.Key()
				revoked := strings.HasPrefix(key, RevokedPrefix)
				if revoked {
					key = strings.TrimPrefix(key, RevokedPrefix)
				}
				if entry.Operation() == jetstream.KeyValueDelete ||
					entry.Operation() == jetstream.KeyValuePurge {
					b.kvL.Lock()
					if revoked {
						delete(b.revokedCache, key)
					} else {
						delete(b.cardCache, key)
					}
					b.kvL.Unlock()
					continue
				}
				b.kvL.Lock()
				if revoked {
					b.revokedCache[key] = true
				} else {
					b.cardCache[key] = append([]byte(nil), entry.Value()...)
				}
				b.kvL.Unlock()
			}
			if wctx.Err() != nil {
				return
			}
		}
	}()
}

// AuthOptionsFromEnv builds NATS auth/TLS options from the environment
// (secrets stay out of argv and out of chat): AFEP_NATS_TOKEN,
// AFEP_NATS_USER + AFEP_NATS_PASS, AFEP_NATS_CREDS (signed .creds file),
// AFEP_NATS_TLSCA (PEM that signs the broker certificate).
func AuthOptionsFromEnv() ([]nats.Option, error) {
	var opts []nats.Option
	if tok := os.Getenv("AFEP_NATS_TOKEN"); tok != "" {
		opts = append(opts, nats.Token(tok))
	}
	user, pass := os.Getenv("AFEP_NATS_USER"), os.Getenv("AFEP_NATS_PASS")
	if user != "" && pass != "" {
		opts = append(opts, nats.UserInfo(user, pass))
	}
	if creds := os.Getenv("AFEP_NATS_CREDS"); creds != "" {
		opts = append(opts, nats.UserCredentials(creds))
	}
	if ca := os.Getenv("AFEP_NATS_TLSCA"); ca != "" {
		opts = append(opts, nats.RootCAs(ca))
	}
	return opts, nil
}

// NotifyConnChanges reports mesh connectivity transitions (true = connected).
// Readiness probes subscribe here so health reflects the broker, not just
// the process.
func (b *Bus) NotifyConnChanges(f func(bool)) {
	b.onMu.Lock()
	b.onConn = f
	b.onMu.Unlock()
}

func notifyConn(b *Bus, up bool) {
	if b == nil {
		return
	}
	b.onMu.RLock()
	f := b.onConn
	b.onMu.RUnlock()
	if f != nil {
		f(up)
	}
}

// RevokeCard marks a card id as revoked (kv "revoked/<id>"); the watcher
// mirrors it so verifiers see it with zero RTT.
func (b *Bus) RevokeCard(ctx context.Context, id string) error {
	if b.kv == nil {
		return errors.New("afep/natsbus: kv not initialized")
	}
	if _, err := b.kv.Put(ctx, RevokedPrefix+id, []byte(time.Now().UTC().Format(time.RFC3339))); err != nil {
		return fmt.Errorf("afep/natsbus: revoke: %w", err)
	}
	b.kvL.Lock()
	b.revokedCache[id] = true
	b.kvL.Unlock()
	return nil
}

// IsRevoked reports whether a card id was revoked: mirror first, then an
// authoritative kv read (fail closed on kv errors when the caller treats
// err != nil as transient).
func (b *Bus) IsRevoked(ctx context.Context, id string) (bool, error) {
	if b.kv == nil {
		return false, errors.New("afep/natsbus: kv not initialized")
	}
	b.kvL.RLock()
	rev := b.revokedCache[id]
	b.kvL.RUnlock()
	if rev {
		return true, nil
	}
	if _, err := b.kv.Get(ctx, RevokedPrefix+id); err == nil {
		b.kvL.Lock()
		b.revokedCache[id] = true
		b.kvL.Unlock()
		return true, nil
	} else if err == jetstream.ErrKeyNotFound {
		return false, nil
	} else {
		return false, fmt.Errorf("afep/natsbus: revocation lookup: %w", err)
	}
}

// LogStatsEvery logs stream and consumer counters: pending lag and
// redeliveries were previously invisible in the daemon.
func (b *Bus) LogStatsEvery(ctx context.Context, every time.Duration) {
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if st, err := b.js.Stream(sctx, StreamName); err == nil {
				if info, ierr := st.Info(sctx); ierr == nil {
					log.Printf("jetstream: %s msgs=%d bytes=%d consumers=%d",
						StreamName, info.State.Msgs, info.State.Bytes, info.State.Consumers)
				}
			}
			if b.statsCons != nil {
				if ci, cerr := b.statsCons.Info(sctx); cerr == nil {
					log.Printf("jetstream: consumer pending=%d ack-pending=%d redelivered=%d",
						ci.NumPending, ci.NumAckPending, ci.NumRedelivered)
				}
			}
			cancel()
		}
	}()
}

// PublishEnvelope publishes with at-least-once semantics; JetStream
// de-duplicates redeliveries keyed by task id.
func (b *Bus) PublishEnvelope(ctx context.Context, domain, agent string, env *afepv1.TaskEnvelope) error {
	subject, err := Subject(domain, agent, VerbEnvelopes)
	if err != nil {
		return err
	}
	body, err := proto.MarshalOptions{Deterministic: true}.Marshal(env)
	if err != nil {
		return fmt.Errorf("afep/natsbus: marshal: %w", err)
	}
	msg := nats.NewMsg(subject)
	if msg.Header == nil {
		msg.Header = make(nats.Header)
	}
	msg.Data = body
	// Dedup key must distinguish lifecycle hops: a probe and its escalation
	// share the task id, and JetStream de-dups MsgId stream-wide (a plain
	// task id would silently swallow the escalation as a "duplicate").
	msg.Header.Set(nats.MsgIdHdr,
		env.GetTaskId()+":"+env.GetState().String()+":"+strconv.FormatUint(uint64(env.GetHops()), 10))
	msg.Header.Set("AFEP-State", env.GetState().String())
	if _, err := b.js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("afep/natsbus: publish: %w", err)
	}
	return nil
}

// ErrTerm returned by an EnvelopeHandler marks the message as poison: it
// is discarded instead of redelivered forever.
var ErrTerm = errors.New("afep/natsbus: term")

// redeliveryDelay spreads out NAK retries (1s, 2s, 4s, ... capped 30s).
func redeliveryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 6 {
		return 30 * time.Second
	}
	d := time.Second << (attempt - 1)
	if d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}

// EnvelopeHandler returns nil to ack the message or an error to redeliver.
type EnvelopeHandler func(*afepv1.TaskEnvelope) error

// SubscribeEnvelopes consumes afep.<domain>.*.envelopes on a durable
// consumer: the queue survives disconnects and replays after recovery.
func (b *Bus) SubscribeEnvelopes(ctx context.Context, domain, durable string, h EnvelopeHandler) (jetstream.ConsumeContext, error) {
	filter, err := Subject(domain, Wildcard, VerbEnvelopes)
	if err != nil {
		return nil, err
	}
	cfg := jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: filter,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    5,
		// Fresh durables start at the tail: the DeliverAll default would
		// replay the whole retained stream (up to 24h/1GiB) as new work
		// on a first boot.
		DeliverPolicy: jetstream.DeliverNewPolicy,
		// Explicit ack window covering slow handlers; server-side BackOff
		// is the safety net for SILENT handlers (neither Ack nor Nak) —
		// explicit failures still use NakWithDelay in the loop below.
		AckWait: 60 * time.Second,
		BackOff: []time.Duration{60 * time.Second, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 30 * time.Minute},
		// Single-threaded handler: bound the unacked work in flight.
		MaxAckPending: 100,
	}
	cons, err := b.js.CreateOrUpdateConsumer(ctx, StreamName, cfg)
	if err != nil {
		// deliver_policy is immutable: a pre-existing durable created
		// with the DeliverAll default must be recreated at the tail
		// (logged; any backlog older than this restart is skipped).
		if derr := b.js.DeleteConsumer(ctx, StreamName, durable); derr != nil {
			return nil, fmt.Errorf("afep/natsbus: consumer: %w", err)
		}
		log.Printf("natsbus: consumer %s recreated at tail (deliver_policy migration)", durable)
		cons, err = b.js.CreateOrUpdateConsumer(ctx, StreamName, cfg)
		if err != nil {
			return nil, fmt.Errorf("afep/natsbus: consumer recreate: %w", err)
		}
	}
	b.statsCons = cons
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		// A handler panic must not take the whole daemon down: term the
		// message, log the stack, keep the consumer alive.
		defer func() {
			if r := recover(); r != nil {
				log.Printf("natsbus: handler panic (term): %v\n%s", r, debug.Stack())
				metrics.EnvelopePanic.Inc()
				_ = msg.Term()
			}
		}()
		env := &afepv1.TaskEnvelope{}
		if err := proto.Unmarshal(msg.Data(), env); err != nil {
			metrics.EnvelopeTerm.Inc()
			_ = msg.Term() // poison message: stop redelivering it
			return
		}
		_ = msg.InProgress() // extend the ack window across handler execution
		if err := h(env); err != nil {
			if errors.Is(err, ErrTerm) {
				metrics.EnvelopeTerm.Inc()
				_ = msg.Term()
			} else {
				metrics.EnvelopeNak.Inc()
				// Backoff instead of a hot redelivery loop: MaxDeliver=5
				// bounds attempts, the delay spreads them out.
				delay := 2 * time.Second
				if meta, err := msg.Metadata(); err == nil {
					delay = redeliveryDelay(int(meta.NumDelivered))
				}
				_ = msg.NakWithDelay(delay)
			}
			return
		}
		metrics.EnvelopeAck.Inc()
		_ = msg.Ack()
	})
	if err != nil {
		return nil, fmt.Errorf("afep/natsbus: consume: %w", err)
	}
	return cc, nil
}

// PutCard stores a signed card in the KV bucket.
func (b *Bus) PutCard(ctx context.Context, card *afepv1.AgentCard) error {
	if b.kv == nil {
		return errors.New("afep/natsbus: kv not initialized")
	}
	body, err := proto.MarshalOptions{Deterministic: true}.Marshal(card)
	if err != nil {
		return fmt.Errorf("afep/natsbus: marshal: %w", err)
	}
	if _, err := b.kv.Put(ctx, card.GetId(), body); err != nil {
		return fmt.Errorf("afep/natsbus: kv put: %w", err)
	}
	// instant mirror hit; the watcher will confirm the same bytes
	b.kvL.Lock()
	b.cardCache[card.GetId()] = body
	b.kvL.Unlock()
	return nil
}

// GetCard loads a card from KV; ok is false when absent.
func (b *Bus) GetCard(ctx context.Context, id string) (*afepv1.AgentCard, bool, error) {
	if b.kv == nil {
		return nil, false, errors.New("afep/natsbus: kv not initialized")
	}
	b.kvL.RLock()
	raw, hit := b.cardCache[id]
	b.kvL.RUnlock()
	if !hit {
		// watch missed it (cold start / rotation): authoritative read
		entry, err := b.kv.Get(ctx, id)
		if err != nil {
			if err == jetstream.ErrKeyNotFound {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("afep/natsbus: kv get: %w", err)
		}
		raw = entry.Value()
	}
	card := &afepv1.AgentCard{}
	if err := proto.Unmarshal(raw, card); err != nil {
		return nil, false, fmt.Errorf("afep/natsbus: unmarshal: %w", err)
	}
	return card, true, nil
}

// PublishBid fires an ephemeral signed bid over core NATS (at-most-once):
// bids are request-scoped and re-probing beats redelivery.
func (b *Bus) PublishBid(domain, agent string, bid *afepv1.Bid) error {
	subject, err := Subject(domain, agent, VerbBids)
	if err != nil {
		return err
	}
	body, err := proto.MarshalOptions{Deterministic: true}.Marshal(bid)
	if err != nil {
		return fmt.Errorf("afep/natsbus: marshal bid: %w", err)
	}
	msg := &nats.Msg{Subject: subject, Data: body, Header: make(nats.Header)}
	msg.Header.Set(nats.MsgIdHdr, bid.GetTaskId()+":"+bid.GetAgentId())
	if err := b.nc.PublishMsg(msg); err != nil {
		return fmt.Errorf("afep/natsbus: publish bid: %w", err)
	}
	metrics.BidPublished.Inc()
	// Best-effort JetStream archive (live path above is the contract):
	// same payload + MsgId, so replays stay idempotent per task+agent.
	actx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	am := &nats.Msg{Subject: subject, Data: body, Header: make(nats.Header)}
	am.Header.Set(nats.MsgIdHdr, bid.GetTaskId()+":"+bid.GetAgentId())
	if _, err := b.js.PublishMsg(actx, am); err != nil {
		metrics.BidArchiveFailed.Inc()
		log.Printf("natsbus: bid archive: %v", err) // non-fatal
	}
	return nil
}

// BidHandler receives a bid from afep.<domain>.*.bids.
type BidHandler func(domain, agent string, bid *afepv1.Bid)

// SubscribeBids returns an ephemeral subscription for bid collection;
// core NATS, so only live responders are seen (re-probe recovers misses).
func (b *Bus) SubscribeBids(domain string, h BidHandler) (*nats.Subscription, error) {
	filter, err := Subject(domain, Wildcard, VerbBids)
	if err != nil {
		return nil, err
	}
	return b.nc.Subscribe(filter, func(m *nats.Msg) {
		d, a, _, err := ParseSubject(m.Subject)
		if err != nil {
			return
		}
		bid := &afepv1.Bid{}
		if err := proto.Unmarshal(m.Data, bid); err != nil {
			return
		}
		h(d, a, bid)
	})
}
