// Command k8s-agent is InfraHub's in-cluster Kubernetes agent: install it
// once (see deploy/manifest.yaml) inside the cluster you want to monitor,
// give it a backend URL and a bearer token (both shown once when you
// connect the cluster in InfraHub's Kubernetes Clusters page), and it
// dials OUT to InfraHub over a WebSocket -- InfraHub never needs inbound
// network access to your cluster's API server, and never sees a
// kubeconfig or any other cluster-admin credential. The agent
// authenticates every request it makes to the cluster's own API server
// using its own ServiceAccount (see deploy/manifest.yaml's RBAC, which
// grants exactly get/list/watch on pods and pods/log -- nothing that can
// change anything).
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	backendURL := os.Getenv("INFRAHUB_BACKEND_URL")
	token := os.Getenv("INFRAHUB_AGENT_TOKEN")
	if backendURL == "" || token == "" {
		log.Fatal("INFRAHUB_BACKEND_URL and INFRAHUB_AGENT_TOKEN must both be set")
	}
	log.Printf("starting k8s-agent: backend=%s token=provided", backendURL)

	k8s, err := newK8sClient()
	if err != nil {
		log.Fatalf("kubernetes client: %v", err)
	}
	log.Print("kubernetes client initialized")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	const maxBackoff = 30 * time.Second
	backoff := time.Second
	for ctx.Err() == nil {
		connectedAt := time.Now()
		if err := runOnce(ctx, backendURL, token, k8s); err != nil {
			log.Printf("connection ended: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
		// A connection that stayed up for a while is treated as healthy --
		// reset the backoff rather than let one brief, long-ago blip keep
		// slowing every future reconnect.
		if time.Since(connectedAt) > maxBackoff {
			backoff = time.Second
		}
		log.Printf("reconnecting in %s", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
}

// pongWait bounds how long this agent waits without hearing anything from
// InfraHub (a real command, or its own ping being answered -- see
// pingPeriod below) before treating the connection as dead. A NAT/proxy/
// load balancer between here and the backend can silently drop an idle
// TCP connection without either side ever seeing a FIN/RST; without an
// active liveness check like this, a silently-dead connection would leave
// this agent blocked forever in ReadJSON, never reconnecting, while
// InfraHub still shows this cluster as connected.
//
// pingPeriod is deliberately a small fraction of pongWait (several ping
// attempts per window, not just one near the deadline) -- a single ping
// close to the deadline gives one round trip only a few seconds of margin,
// which a slow or bursty network path (e.g. a NAT hop like Docker
// Desktop's host.docker.internal) can miss even on an otherwise-healthy
// connection, causing a false-positive reconnect. Multiple attempts per
// window tolerate that jitter while still bounding worst-case detection
// time to pongWait.
const pongWait = 90 * time.Second
const pingPeriod = 15 * time.Second

// runOnce holds one connection open until it fails or ctx is cancelled,
// dispatching every inbound command to its own goroutine (server_version/
// list_pods/fetch_logs_since answer once and return; stream_logs runs
// until stop_stream or the connection closes).
func runOnce(ctx context.Context, backendURL, token string, k8s *k8sClient) error {
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	log.Printf("connecting to InfraHub at %s", backendURL)
	conn, _, err := dialer.DialContext(ctx, backendURL, http.Header{"Authorization": {"Bearer " + token}})
	if err != nil {
		return err
	}
	defer conn.Close()
	log.Println("connected to InfraHub")

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-connCtx.Done()
		_ = conn.Close()
	}()

	var writeMu sync.Mutex
	var activeStreams sync.Map // command ID -> context.CancelFunc, for in-flight stream_logs commands

	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	go pingLoop(connCtx, conn, &writeMu, cancel)

	for {
		var cmd Command
		if err := conn.ReadJSON(&cmd); err != nil {
			// Every stream this connection was running dies along with it --
			// cancel them so their goroutines don't leak past the connection
			// they were writing to.
			activeStreams.Range(func(_, v any) bool {
				v.(context.CancelFunc)()
				return true
			})
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		go handleCommand(connCtx, conn, &writeMu, k8s, cmd, &activeStreams)
	}
}

// pingLoop is this connection's active liveness check -- InfraHub's
// default WebSocket ping handling replies with a pong automatically
// (see backend/internal/services/k8s_agent_hub.go's readLoop for the
// override that also extends ITS OWN deadline), which resets our own
// SetPongHandler's deadline above. If a ping write itself fails, the
// connection is already dead -- cancel so runOnce's read loop unblocks
// and the outer reconnect loop in main() takes over immediately rather
// than waiting out the rest of pongWait.
func pingLoop(ctx context.Context, conn *websocket.Conn, writeMu *sync.Mutex, cancel context.CancelFunc) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			writeMu.Lock()
			err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second))
			writeMu.Unlock()
			if err != nil {
				cancel()
				return
			}
		}
	}
}

func handleCommand(ctx context.Context, conn *websocket.Conn, writeMu *sync.Mutex, k8s *k8sClient, cmd Command, activeStreams *sync.Map) {
	switch cmd.Type {
	case CmdServerVersion:
		version, err := k8s.ServerVersion(ctx)
		if err != nil {
			log.Printf("server_version query failed: %v", err)
		} else {
			log.Printf("server_version: %s", version)
		}
		sendResult(conn, writeMu, cmd.ID, ServerVersionResult{Version: version}, err)

	case CmdListPods:
		pods, err := k8s.ListPods(ctx, cmd.Namespace)
		if err != nil {
			log.Printf("list_pods failed (namespace=%q): %v", cmd.Namespace, err)
		}
		sendResult(conn, writeMu, cmd.ID, pods, err)

	case CmdListNodes:
		nodes, err := k8s.ListNodes(ctx)
		if err != nil {
			log.Printf("list_nodes failed: %v", err)
		}
		sendResult(conn, writeMu, cmd.ID, nodes, err)

	case CmdClusterResourceSummary:
		summary, err := k8s.ClusterResourceSummary(ctx)
		if err != nil {
			log.Printf("cluster_resource_summary failed: %v", err)
		}
		sendResult(conn, writeMu, cmd.ID, summary, err)

	case CmdFetchLogsSince:
		var since time.Time
		if cmd.Since != "" {
			since, _ = time.Parse(time.RFC3339Nano, cmd.Since)
		}
		output, err := k8s.FetchLogsSince(ctx, cmd.Namespace, cmd.PodName, since)
		if err != nil {
			log.Printf("fetch_logs_since failed for pod %s/%s: %v", cmd.Namespace, cmd.PodName, err)
		} else {
			log.Printf("fetch_logs_since: sent %d lines for pod %s/%s", countLines(output), cmd.Namespace, cmd.PodName)
		}
		sendResult(conn, writeMu, cmd.ID, map[string]string{"output": output}, err)

	case CmdStreamLogs:
		streamCtx, cancel := context.WithCancel(ctx)
		activeStreams.Store(cmd.ID, cancel)
		defer func() {
			activeStreams.Delete(cmd.ID)
			cancel()
		}()
		log.Printf("log stream started for pod %s/%s", cmd.Namespace, cmd.PodName)
		lines := 0
		err := k8s.StreamLogs(streamCtx, cmd.Namespace, cmd.PodName, func(line string) {
			lines++
			writeMu.Lock()
			_ = conn.WriteJSON(Message{ID: cmd.ID, Type: MsgLogLine, Line: line})
			writeMu.Unlock()
		})
		writeMu.Lock()
		if err != nil && streamCtx.Err() == nil {
			_ = conn.WriteJSON(Message{ID: cmd.ID, Type: MsgError, Message: "log stream ended: " + err.Error()})
		} else {
			_ = conn.WriteJSON(Message{ID: cmd.ID, Type: MsgDone})
		}
		writeMu.Unlock()
		if err != nil && streamCtx.Err() == nil {
			log.Printf("log stream ended for pod %s/%s: forwarded %d lines, error: %v", cmd.Namespace, cmd.PodName, lines, err)
		} else {
			log.Printf("log stream ended for pod %s/%s: forwarded %d lines", cmd.Namespace, cmd.PodName, lines)
		}

	case CmdStopStream:
		if v, ok := activeStreams.Load(cmd.ID); ok {
			v.(context.CancelFunc)()
			activeStreams.Delete(cmd.ID)
			log.Printf("log stream stopped by request: command %s", cmd.ID)
		}

	default:
		// Mirrors docker-agent/main.go's handling: an unrecognized command
		// means this agent binary predates a backend that's added new
		// commands. Unlike docker-agent/vm-agent this protocol has no
		// established "unknown command" error response, so this only logs
		// locally rather than changing the wire behavior.
		log.Printf("received unknown command %q -- ignoring", cmd.Type)
	}
}

// countLines returns the number of newline-terminated lines in a
// FetchLogsSince-shaped blob, for a compact "sent N lines" log line --
// purely for this agent's own stdout, never sent over the wire.
func countLines(s string) int {
	if s == "" {
		return 0
	}
	return strings.Count(s, "\n")
}

func sendResult(conn *websocket.Conn, writeMu *sync.Mutex, id string, data any, err error) {
	writeMu.Lock()
	defer writeMu.Unlock()
	if err != nil {
		_ = conn.WriteJSON(Message{ID: id, Type: MsgError, Message: err.Error()})
		return
	}
	raw, marshalErr := json.Marshal(data)
	if marshalErr != nil {
		_ = conn.WriteJSON(Message{ID: id, Type: MsgError, Message: "internal error encoding response"})
		return
	}
	_ = conn.WriteJSON(Message{ID: id, Type: MsgResult, Data: raw})
}
