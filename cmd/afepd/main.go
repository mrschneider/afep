// afepd is the AFEP reference daemon: gRPC (TCP or QUIC), optional NATS
// JetStream mesh with bid serving and cross-domain escalation, A2A
// well-known discovery, durable task state.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/a2a"
	natsbus "github.com/mrschneider/afep/internal/bus/nats"
	"github.com/mrschneider/afep/internal/identity"
	"github.com/mrschneider/afep/internal/mesh"
	"github.com/mrschneider/afep/internal/quictransport"
	"github.com/mrschneider/afep/internal/registry"
	"github.com/mrschneider/afep/internal/router"
	"github.com/mrschneider/afep/internal/taskstore"
	"github.com/mrschneider/afep/internal/transport/grpcsvc"
	"google.golang.org/grpc"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:7700", "gRPC TCP listen address")
	listenQUIC := flag.String("listen-quic", "", "gRPC-over-QUIC listen address (empty disables)")
	httpListen := flag.String("http-listen", "127.0.0.1:7701", "HTTP listener for the A2A well-known card (empty disables)")
	natsURL := flag.String("nats", "nats://127.0.0.1:4222", "NATS URL; empty disables the mesh")
	agentID := flag.String("id", "local/afepd", "agent id for the self card")
	domain := flag.String("domain", "local", "NATS domain segment of the subject prefix")
	keyPath := flag.String("key", defaultKeyPath(), "ed25519 seed file (0600, created if missing)")
	statePath := flag.String("state", "", "durable task store file (default: next to the key)")
	clearances := flag.String("clearances", "tenant:*", "comma-separated security clearances for the self card")
	capability := flag.String("capability", "demo.echo", "comma-separated capabilities advertised by this node")
	bidScore := flag.Uint64("bid-score", 100, "score stamped on this node's bids")
	peers := flag.String("peers", "", "comma-separated peer domains for cross-domain escalation")
	pruneAge := flag.Duration("prune-age", 24*time.Hour, "prune terminal tasks older than this (0 disables)")
	flag.Parse()

	kp, err := loadOrCreateKey(*keyPath)
	if err != nil {
		log.Fatalf("identity: %v", err)
	}
	if *statePath == "" {
		*statePath = filepath.Join(filepath.Dir(*keyPath), "tasks.json")
	}

	self, err := identity.NewCard(*agentID, "afepd", kp, func(c *afepv1.AgentCard) {
		c.Endpoint = &afepv1.Endpoint{
			GrpcAddress:       *listen,
			NatsSubjectPrefix: "afep." + *domain + "." + sanitize(*agentID),
		}
		for _, name := range splitCSV(*capability) {
			c.Capabilities = append(c.Capabilities, &afepv1.Capability{Name: name, Version: "0.1"})
		}
		c.SecurityClearances = splitCSV(*clearances)
	})
	if err != nil {
		log.Fatalf("card: %v", err)
	}

	reg := registry.New()
	if err := reg.Put(self); err != nil {
		log.Fatalf("registry: %v", err)
	}
	store, err := taskstore.Open(*statePath)
	if err != nil {
		log.Fatalf("taskstore: %v", err)
	}
	if *pruneAge > 0 {
		if n, err := store.PruneTerminal(*pruneAge, time.Now()); err != nil {
			log.Printf("taskstore prune: %v", err)
		} else if n > 0 {
			log.Printf("taskstore: pruned %d stale terminal tasks at boot", n)
		}
	}
	svc, err := grpcsvc.New(self, reg, kp,
		grpcsvc.WithBidScore(*bidScore),
		grpcsvc.WithTaskStore(store))
	if err != nil {
		log.Fatalf("service: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- optional mesh -------------------------------------------------
	var bus *natsbus.Bus
	if *natsURL != "" {
		b, err := natsbus.Dial(ctx, *natsURL)
		if err != nil {
			log.Printf("nats: mesh disabled (%v)", err)
		} else {
			bus = b
			defer bus.Close()
			if err := bus.PutCard(ctx, self); err != nil {
				log.Printf("nats: card publish: %v", err)
			}
			var rt *router.Router
			if peerList := splitCSV(*peers); len(peerList) > 0 {
				if rt, err = router.New(*domain, peerList, 0); err != nil {
					log.Fatalf("router: %v", err)
				}
				log.Printf("mesh: escalation peers %v (max %d hops)", rt.Peers(), router.DefaultMaxHops)
			}
			handler, err := mesh.New(mesh.Config{
				Domain: *domain, Self: self, Key: kp, Score: *bidScore,
				Router: rt, Bids: bus, Relay: bus,
				Issuers: mesh.CardResolverFunc(bus.GetCard),
			})
			if err != nil {
				log.Fatalf("mesh: %v", err)
			}
			durable := "afepd-" + sanitize(*agentID)
			if _, err := bus.SubscribeEnvelopes(ctx, *domain, durable, func(env *afepv1.TaskEnvelope) error {
				if err := handler.OnEnvelope(ctx, env); err != nil {
					if errors.Is(err, mesh.ErrRejected) {
						return natsbus.ErrTerm // poison: stop redelivering
					}
					return err // transient: Nak + redelivery
				}
				log.Printf("mesh: handled task=%s state=%s cap=%s hops=%d",
					env.GetTaskId(), env.GetState(), env.GetCapability(), env.GetHops())
				return nil
			}); err != nil {
				log.Printf("nats: subscribe: %v", err)
			} else {
				log.Printf("mesh: consuming afep.%s.*.%s (durable=%s)", *domain, natsbus.VerbEnvelopes, durable)
			}
		}
	}

	// --- gRPC over TCP -------------------------------------------------
	servers := []*grpc.Server{}

	tcpLn, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen %s: %v", *listen, err)
	}
	tcpSrv := grpc.NewServer()
	afepv1.RegisterAgentTransportServiceServer(tcpSrv, svc)
	servers = append(servers, tcpSrv)
	go func() {
		if err := tcpSrv.Serve(tcpLn); err != nil {
			log.Printf("grpc/tcp: %v", err)
		}
	}()
	log.Printf("afepd: gRPC/TCP on %s (id=%s)", *listen, *agentID)

	// --- gRPC over QUIC (optional) ------------------------------------
	if *listenQUIC != "" {
		qln, err := quictransport.Listen(*listenQUIC)
		if err != nil {
			log.Fatalf("quic listen: %v", err)
		}
		qSrv := grpc.NewServer()
		afepv1.RegisterAgentTransportServiceServer(qSrv, svc)
		servers = append(servers, qSrv)
		go func() {
			if err := qSrv.Serve(qln); err != nil {
				log.Printf("grpc/quic: %v", err)
			}
		}()
		log.Printf("afepd: gRPC/QUIC on %s (ALPN %s)", *listenQUIC, quictransport.ALPN)
	}

	// --- A2A well-known card (optional) --------------------------------
	var httpSrv *http.Server
	if *httpListen != "" {
		mux := http.NewServeMux()
		mux.Handle("/", a2a.Handler(self))
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
		httpSrv = &http.Server{Addr: *httpListen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("http: %v", err)
			}
		}()
		log.Printf("afepd: A2A card at http://%s%s", *httpListen, a2a.WellKnownPath)
	}

	if *pruneAge > 0 {
		go pruneLoop(ctx, store, *pruneAge)
	}
	log.Printf("afepd: ready (key=%s state=%s score=%d prune=%s)", *keyPath, *statePath, *bidScore, *pruneAge)

	<-ctx.Done()
	log.Print("afepd: shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if httpSrv != nil {
		_ = httpSrv.Shutdown(shutdownCtx)
	}
	for _, s := range servers {
		done := make(chan struct{})
		go func(s *grpc.Server) { s.GracefulStop(); close(done) }(s)
		select {
		case <-done:
		case <-shutdownCtx.Done():
			s.Stop()
		}
	}
	log.Print("afepd: stopped")
}

// pruneLoop prunes terminal tasks hourly until the context ends.
func pruneLoop(ctx context.Context, store *taskstore.Store, age time.Duration) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n, err := store.PruneTerminal(age, time.Now()); err != nil {
				log.Printf("taskstore prune: %v", err)
			} else if n > 0 {
				log.Printf("taskstore: pruned %d terminal tasks", n)
			}
		}
	}
}

func sanitize(id string) string {
	return strings.NewReplacer("/", "-", " ", "-", "\\", "-").Replace(id)
}

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
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
