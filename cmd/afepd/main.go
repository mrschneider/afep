// afepd is the AFEP reference daemon: direct gRPC transport plus an
// optional NATS JetStream mesh for NATed or federated sites.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	natsbus "github.com/mrschneider/afep/internal/bus/nats"
	"github.com/mrschneider/afep/internal/identity"
	"github.com/mrschneider/afep/internal/registry"
	"github.com/mrschneider/afep/internal/transport/grpcsvc"
	"google.golang.org/grpc"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:7700", "gRPC listen address")
	natsURL := flag.String("nats", "nats://127.0.0.1:4222", "NATS URL; empty disables the mesh")
	agentID := flag.String("id", "local/afepd", "agent id for the self card")
	domain := flag.String("domain", "local", "NATS domain segment of the subject prefix")
	keyPath := flag.String("key", defaultKeyPath(), "ed25519 seed file (0600, created if missing)")
	flag.Parse()

	kp, err := loadOrCreateKey(*keyPath)
	if err != nil {
		log.Fatalf("identity: %v", err)
	}

	self, err := identity.NewCard(*agentID, "afepd", kp, func(c *afepv1.AgentCard) {
		c.Endpoint = &afepv1.Endpoint{
			GrpcAddress:       *listen,
			NatsSubjectPrefix: "afep." + *domain + "." + sanitize(*agentID),
		}
		c.Capabilities = []*afepv1.Capability{{Name: "demo.echo", Version: "0.1"}}
		c.SecurityClearances = []string{"domain:" + *domain}
	})
	if err != nil {
		log.Fatalf("card: %v", err)
	}

	reg := registry.New()
	if err := reg.Put(self); err != nil {
		log.Fatalf("registry: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Mesh is optional per site: without NATS the daemon still serves gRPC.
	if *natsURL != "" {
		bus, err := natsbus.Dial(ctx, *natsURL)
		if err != nil {
			log.Printf("nats: mesh disabled (%v)", err)
		} else {
			defer bus.Close()
			if err := bus.PutCard(ctx, self); err != nil {
				log.Printf("nats: card publish: %v", err)
			}
			durable := "afepd-" + sanitize(*agentID)
			if _, err := bus.SubscribeEnvelopes(ctx, *domain, durable, func(env *afepv1.TaskEnvelope) error {
				log.Printf("mesh: task=%s state=%s cap=%s", env.GetTaskId(), env.GetState(), env.GetCapability())
				return nil
			}); err != nil {
				log.Printf("nats: subscribe: %v", err)
			} else {
				log.Printf("mesh: consuming afep.%s.*.%s (durable=%s)", *domain, natsbus.VerbEnvelopes, durable)
			}
		}
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	afepv1.RegisterAgentTransportServiceServer(grpcServer, grpcsvc.New(self, reg))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("grpc: %v", err)
		}
	}()
	log.Printf("afepd: gRPC on %s (id=%s key=%s)", *listen, *agentID, *keyPath)

	<-ctx.Done()
	log.Print("afepd: shutting down")
	done := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		grpcServer.Stop()
	}
}

func sanitize(id string) string {
	return strings.NewReplacer("/", "-", " ", "-").Replace(id)
}

func defaultKeyPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "agent.seed"
	}
	return filepath.Join(home, ".config", "afep", "agent.seed")
}

// loadOrCreateKey reads the ed25519 seed (0600) or creates one.
func loadOrCreateKey(path string) (*identity.KeyPair, error) {
	seed, err := os.ReadFile(path)
	if err == nil {
		return identity.FromSeed(seed)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	kp, err := identity.Generate()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, kp.Private.Seed(), 0o600); err != nil {
		return nil, err
	}
	log.Printf("identity: new seed at %s", path)
	return kp, nil
}
