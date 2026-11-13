package natsbus

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
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
	// on the per-envelope verify path read it with zero RTT. It holds the
	// UNMARSHALED card, not the raw bytes: GetCard then returns a stable
	// pointer, so the identity layer's pointer-keyed card-digest cache hits
	// on every envelope and a re-unmarshal per verify disappears too.
	cardCache map[string]*afepv1.AgentCard
	kvStop    context.CancelFunc
	// statsCons (set at subscribe) feeds the observability ticker.
	statsCons jetstream.Consumer
	// revokedCache mirrors kv keys "revoked/<id>" (fail-closed check
	// with authoritative kv fallback on miss).
	revokedCache map[string]bool
	onMu         sync.RWMutex
	onConn       func(bool)
	// bidArchiveCh decouples the best-effort JetStream bid archive from
	// the live bid path: PublishBid enqueues and returns, a single worker
	// drains it. Bounded so a bid flood cannot grow memory or goroutines
	// without limit; when full the archive is dropped (counted) and the
	// live bid — the actual contract — is never blocked by the archive.
	bidArchiveCh   chan *nats.Msg
	bidArchiveStop context.CancelFunc
	bidArchiveDone chan struct{}
	// envelopeWorkers sizes the SubscribeEnvelopes worker pool (0 =
	// defaultEnvelopeWorkers).
	envelopeWorkers int
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
		cardCache:    make(map[string]*afepv1.AgentCard),
		revokedCache: make(map[string]bool),
		bidArchiveCh: make(chan *nats.Msg, 1024),
	}
	b.startCardWatcher(ctx)
	b.startBidArchiver(ctx)
	return b, nil
}

// startBidArchiver drains bidArchiveCh with a single worker: the JetStream
// archive becomes best-effort background work instead of a synchronous
// roundtrip on the bid path.
func (b *Bus) startBidArchiver(ctx context.Context) {
	wctx, cancel := context.WithCancel(ctx)
	b.bidArchiveStop = cancel
	b.bidArchiveDone = make(chan struct{})
	go func() {
		defer close(b.bidArchiveDone)
		for {
			select {
			case <-wctx.Done():
				return
			case msg := <-b.bidArchiveCh:
				pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
				if _, err := b.js.PublishMsg(pctx, msg); err != nil {
					metrics.BidArchiveFailed.Inc()
					log.Printf("natsbus: bid archive: %v", err) // non-fatal
				}
				pcancel()
			}
		}
	}()
}

// Close tears down the NATS connection.
func (b *Bus) Close() {
	if b.nc == nil {
		return
	}
	if b.kvStop != nil {
		b.kvStop()
	}
	// Stop the bid archive worker and give it a bounded window to drain
	// what is already queued; the live path is unaffected either way.
	if b.bidArchiveStop != nil {
		b.bidArchiveStop()
		select {
		case <-b.bidArchiveDone:
		case <-time.After(2 * time.Second):
		}
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
					// Unmarshal ONCE here (watch event) instead of per
					// GetCard (envelope verify): the stored pointer is
					// stable, which is what makes the identity layer's
					// pointer-keyed digest cache hit on every envelope.
					card := &afepv1.AgentCard{}
					if uerr := proto.Unmarshal(entry.Value(), card); uerr != nil {
						log.Printf("natsbus: card watch: unmarshal %q: %v", key, uerr)
						b.kvL.Unlock()
						continue
					}
					b.cardCache[key] = card
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
	// Wire payloads do NOT need deterministic bytes: the dedup key is the
	// MsgId header below, and receivers unmarshal structurally.
	body, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("afep/natsbus: marshal: %w", err)
	}
	stateStr := env.GetState().String()
	msg := nats.NewMsg(subject)
	if msg.Header == nil {
		msg.Header = make(nats.Header)
	}
	msg.Data = body
	// Dedup key must distinguish lifecycle hops: a probe and its escalation
	// share the task id, and JetStream de-dups MsgId stream-wide (a plain
	// task id would silently swallow the escalation as a "duplicate").
	msg.Header.Set(nats.MsgIdHdr,
		env.GetTaskId()+":"+stateStr+":"+strconv.FormatUint(uint64(env.GetHops()), 10))
	msg.Header.Set("AFEP-State", stateStr)
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

// defaultEnvelopeWorkers sizes the envelope worker pool: envelope
// handling is CPU-bound (ed25519), so the pool tracks the cores — with a
// floor of 4 for the single/dual-core daemons where GOMaxProcs < 4 would
// serialize the verify path.
func defaultEnvelopeWorkers() int {
	if n := runtime.GOMAXPROCS(0); n > 4 {
		return n
	}
	return 4
}

// SetEnvelopeWorkers resizes the envelope consumer's worker pool; call it
// BEFORE SubscribeEnvelopes. A value <= 0 keeps the default.
func (b *Bus) SetEnvelopeWorkers(n int) {
	if n > 0 {
		b.envelopeWorkers = n
	}
}

// SubscribeEnvelopes consumes afep.<domain>.*.envelopes on a durable
// consumer: the queue survives disconnects and replays after recovery.
//
// The consumer runs a single dispatcher goroutine (one caller of
// MessagesContext.Next) that fans out to a small worker pool. Envelope
// handling is CPU-bound (ed25519 verify) and parallelizes well; the pool
// bounds concurrency so MaxAckPending (server-side) stays the work-in-
// flight ceiling and a slow handler cannot wedge the dispatcher.
// Envelope order is NOT preserved by design: tasks are independent and
// dedup/audit order is stream-qualified by (task, state, hops).
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
		// explicit failures still use NakWithDelay in the worker below.
		AckWait: 60 * time.Second,
		BackOff: []time.Duration{60 * time.Second, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 30 * time.Minute},
		// The dispatcher pulls at most this many messages into the pool
		// at once; the pool size below is what parallelizes the handler.
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
	mc, err := cons.Messages()
	if err != nil {
		return nil, fmt.Errorf("afep/natsbus: messages: %w", err)
	}
	cc, err := b.consumeWithPool(ctx, mc, h)
	if err != nil {
		return nil, fmt.Errorf("afep/natsbus: consume: %w", err)
	}
	return cc, nil
}

// consumeWithPool wires a dispatcher (single Next caller) to a worker pool.
// It returns a ConsumeContext whose Stop/Drain shuts the dispatcher down
// and whose Closed channel closes once dispatcher and workers have joined.
func (b *Bus) consumeWithPool(ctx context.Context, mc jetstream.MessagesContext, h EnvelopeHandler) (jetstream.ConsumeContext, error) {
	workers := b.envelopeWorkers
	if workers <= 0 {
		workers = defaultEnvelopeWorkers()
	}
	jobs := make(chan jetstream.Msg, workers)
	done := make(chan struct{})

	// Dispatcher: the ONLY caller of mc.Next. It pulls messages and
	// hands them to the pool; when the iterator stops (Stop/Drain,
	// context cancellation) it closes the job channel so workers can
	// drain and exit.
	go func() {
		defer close(jobs)
		for {
			msg, err := mc.Next()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("natsbus: envelope consumer stopped: %v", err)
				return
			}
			jobs <- msg
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range jobs {
				b.processEnvelope(msg, h)
			}
		}()
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	return &envelopeConsumeCtx{mc: mc, done: done}, nil
}

// envelopeConsumeCtx adapts a MessagesContext to the ConsumeContext
// surface the caller uses (Stop/Drain/Closed).
type envelopeConsumeCtx struct {
	mc   jetstream.MessagesContext
	done chan struct{}
}

func (c *envelopeConsumeCtx) Stop()                   { c.mc.Stop() }
func (c *envelopeConsumeCtx) Drain()                  { c.mc.Drain() }
func (c *envelopeConsumeCtx) Closed() <-chan struct{} { return c.done }

// processEnvelope runs one message through the handler with the same
// ack/nak/term/panic semantics the previous single-threaded loop had.
func (b *Bus) processEnvelope(msg jetstream.Msg, h EnvelopeHandler) {
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
}

// PutCard stores a signed card in the KV bucket.
func (b *Bus) PutCard(ctx context.Context, card *afepv1.AgentCard) error {
	if b.kv == nil {
		return errors.New("afep/natsbus: kv not initialized")
	}
	body, err := proto.Marshal(card)
	if err != nil {
		return fmt.Errorf("afep/natsbus: marshal: %w", err)
	}
	if _, err := b.kv.Put(ctx, card.GetId(), body); err != nil {
		return fmt.Errorf("afep/natsbus: kv put: %w", err)
	}
	// instant mirror hit; the watcher will confirm the same bytes.
	// Store the SAME pointer the caller signed (a card is an immutable
	// self-signed artifact), so the verify path re-uses it directly.
	b.kvL.Lock()
	b.cardCache[card.GetId()] = card
	b.kvL.Unlock()
	return nil
}

// GetCard loads a card from KV; ok is false when absent. The returned
// pointer is STABLE across calls for the same id: the mirror is built by
// the watcher (one unmarshal per card event) and this only falls back to
// an authoritative read when the mirror misses, so the identity layer's
// pointer-keyed digest cache — and the caller's own re-use of the card —
// both hit without re-parsing the bytes.
func (b *Bus) GetCard(ctx context.Context, id string) (*afepv1.AgentCard, bool, error) {
	if b.kv == nil {
		return nil, false, errors.New("afep/natsbus: kv not initialized")
	}
	b.kvL.RLock()
	card, hit := b.cardCache[id]
	b.kvL.RUnlock()
	if hit {
		return card, true, nil
	}
	// watch missed it (cold start / rotation): authoritative read
	entry, err := b.kv.Get(ctx, id)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("afep/natsbus: kv get: %w", err)
	}
	card = &afepv1.AgentCard{}
	if err := proto.Unmarshal(entry.Value(), card); err != nil {
		return nil, false, fmt.Errorf("afep/natsbus: unmarshal: %w", err)
	}
	// Publish the unmarshaled pointer so the NEXT GetCard (and the digest
	// cache) hit; a concurrent watcher write for the same key simply
	// replaces it with an equal card.
	b.kvL.Lock()
	if cur, ok := b.cardCache[id]; ok {
		b.kvL.Unlock()
		return cur, true, nil
	}
	b.cardCache[id] = card
	b.kvL.Unlock()
	return card, true, nil
}

// PublishBid fires an ephemeral signed bid over core NATS (at-most-once):
// bids are request-scoped and re-probing beats redelivery.
func (b *Bus) PublishBid(domain, agent string, bid *afepv1.Bid) error {
	subject, err := Subject(domain, agent, VerbBids)
	if err != nil {
		return err
	}
	body, err := proto.Marshal(bid)
	if err != nil {
		return fmt.Errorf("afep/natsbus: marshal bid: %w", err)
	}
	// Compute the dedup key once; the live and archive messages share it.
	msgId := bid.GetTaskId() + ":" + bid.GetAgentId()
	header := nats.Header{nats.MsgIdHdr: []string{msgId}}
	msg := &nats.Msg{Subject: subject, Data: body, Header: header}
	if err := b.nc.PublishMsg(msg); err != nil {
		return fmt.Errorf("afep/natsbus: publish bid: %w", err)
	}
	metrics.BidPublished.Inc()
	// Best-effort JetStream archive (live path above is the contract):
	// same payload + MsgId, so replays stay idempotent per task+agent.
	// Enqueued for the background worker — the bid must not pay a
	// JetStream roundtrip on the hot path. A full queue drops the
	// archive (counted); the live bid is never blocked by it.
	// The archive message reuses the live message's header map (read-only
	// from here on) to save one allocation per bid.
	am := &nats.Msg{Subject: subject, Data: body, Header: header}
	select {
	case b.bidArchiveCh <- am:
	default:
		metrics.BidArchiveFailed.Inc()
		log.Printf("natsbus: bid archive: queue full, dropped")
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
