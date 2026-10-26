package natsbus

import (
	"context"
	"fmt"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

// Stream and KV bucket names shared across federated sites.
const (
	StreamName = "AFEP_TASKS"
	CardBucket = "afep_cards"
)

// Bus is the JetStream-backed message mesh.
type Bus struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// Dial connects to NATS and (re)creates the AFEP stream.
func Dial(ctx context.Context, url string) (*Bus, error) {
	nc, err := nats.Connect(url, nats.Name("afep"))
	if err != nil {
		return nil, fmt.Errorf("afep/natsbus: connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("afep/natsbus: jetstream: %w", err)
	}
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:       StreamName,
		Subjects:   []string{"afep.>"},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		MaxAge:     24 * time.Hour,
		Duplicates: 2 * time.Minute,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("afep/natsbus: stream %w", err)
	}
	return &Bus{nc: nc, js: js}, nil
}

// Close tears down the NATS connection.
func (b *Bus) Close() {
	if b.nc != nil {
		b.nc.Close()
	}
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
	msg.Header.Set(nats.MsgIdHdr, env.GetTaskId())
	msg.Header.Set("AFEP-State", env.GetState().String())
	if _, err := b.js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("afep/natsbus: publish: %w", err)
	}
	return nil
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
	cons, err := b.js.CreateOrUpdateConsumer(ctx, StreamName, jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: filter,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    5,
	})
	if err != nil {
		return nil, fmt.Errorf("afep/natsbus: consumer: %w", err)
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		env := &afepv1.TaskEnvelope{}
		if err := proto.Unmarshal(msg.Data(), env); err != nil {
			_ = msg.Term() // poison message: stop redelivering it
			return
		}
		if err := h(env); err != nil {
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
	})
	if err != nil {
		return nil, fmt.Errorf("afep/natsbus: consume: %w", err)
	}
	return cc, nil
}

// PutCard stores a signed card in the KV bucket.
func (b *Bus) PutCard(ctx context.Context, card *afepv1.AgentCard) error {
	kv, err := b.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: CardBucket,
		TTL:    7 * 24 * time.Hour,
	})
	if err != nil {
		return fmt.Errorf("afep/natsbus: kv bucket: %w", err)
	}
	body, err := proto.MarshalOptions{Deterministic: true}.Marshal(card)
	if err != nil {
		return fmt.Errorf("afep/natsbus: marshal: %w", err)
	}
	if _, err := kv.Put(ctx, card.GetId(), body); err != nil {
		return fmt.Errorf("afep/natsbus: kv put: %w", err)
	}
	return nil
}

// GetCard loads a card from KV; ok is false when absent.
func (b *Bus) GetCard(ctx context.Context, id string) (*afepv1.AgentCard, bool, error) {
	kv, err := b.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: CardBucket, TTL: 7 * 24 * time.Hour})
	if err != nil {
		return nil, false, fmt.Errorf("afep/natsbus: kv bucket: %w", err)
	}
	entry, err := kv.Get(ctx, id)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("afep/natsbus: kv get: %w", err)
	}
	card := &afepv1.AgentCard{}
	if err := proto.Unmarshal(entry.Value(), card); err != nil {
		return nil, false, fmt.Errorf("afep/natsbus: unmarshal: %w", err)
	}
	return card, true, nil
}
