// Package main provides the HTTP server for the NovaStor scheduler plugin.
// The server exposes health check and scoring endpoints for Kubernetes integration.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/piwi3910/novastor/internal/scheduler"
)

// Server runs the HTTP server for the scheduler plugin.
type Server struct {
	plugin *scheduler.DataLocalityPlugin
	client kubernetes.Interface
	port   int
}

// NewServer creates a new Server.
func NewServer(plugin *scheduler.DataLocalityPlugin, client kubernetes.Interface, port int) *Server {
	return &Server{
		plugin: plugin,
		client: client,
		port:   port,
	}
}

// Run starts the HTTP server.
func (s *Server) Run() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/score", s.handleScore)

	addr := fmt.Sprintf(":%d", s.port)
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	logger.Info("starting HTTP server", zap.String("addr", addr))
	return server.ListenAndServe()
}

// handleHealth handles health check requests.
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// ScoreRequest is the request body for the /score endpoint.
type ScoreRequest struct {
	PodName      string `json:"podName"`
	PodNamespace string `json:"podNamespace"`
	NodeName     string `json:"nodeName"`
}

// ScoreResponse is the response body for the /score endpoint.
type ScoreResponse struct {
	Score int64 `json:"score"`
}

// handleScore handles scoring requests.
func (s *Server) handleScore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ScoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}

	// Get the pod.
	pod, err := s.client.CoreV1().Pods(req.PodNamespace).Get(context.Background(), req.PodName, metav1.GetOptions{})
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get pod: %v", err), http.StatusInternalServerError)
		return
	}

	// Score the node.
	score, err := s.plugin.Score(context.Background(), pod, req.NodeName)
	if err != nil {
		logger.Error("failed to score node",
			zap.String("pod", req.PodName),
			zap.String("namespace", req.PodNamespace),
			zap.String("node", req.NodeName),
			zap.Error(err),
		)
		http.Error(w, fmt.Sprintf("failed to score: %v", err), http.StatusInternalServerError)
		return
	}

	response := ScoreResponse{Score: score}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logger.Error("failed to encode response", zap.Error(err))
	}
}
