package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/tjstebbing/piperdb/pkg/db"
	"github.com/tjstebbing/piperdb/pkg/types"
)

// Server wraps a PiperDB instance with HTTP handlers
type Server struct {
	db   db.PiperDB
	mux  *http.ServeMux
	addr string
}

// New creates a new Server
func New(database db.PiperDB, addr string) *Server {
	s := &Server{
		db:   database,
		mux:  http.NewServeMux(),
		addr: addr,
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	// Lists
	s.mux.HandleFunc("POST /lists", s.createList)
	s.mux.HandleFunc("GET /lists", s.listAllLists)
	s.mux.HandleFunc("GET /lists/{id}", s.getList)
	s.mux.HandleFunc("DELETE /lists/{id}", s.deleteList)
	s.mux.HandleFunc("GET /lists/{id}/schema", s.getSchema)
	s.mux.HandleFunc("GET /lists/{id}/stats", s.getStats)

	// Items
	s.mux.HandleFunc("POST /lists/{id}/items", s.addItems)
	s.mux.HandleFunc("GET /lists/{id}/items", s.getItems)
	s.mux.HandleFunc("GET /lists/{id}/items/{itemId}", s.getItem)
	s.mux.HandleFunc("PUT /lists/{id}/items/{itemId}", s.updateItem)
	s.mux.HandleFunc("DELETE /lists/{id}/items/{itemId}", s.deleteItem)

	// Query
	s.mux.HandleFunc("POST /lists/{id}/query", s.query)
	s.mux.HandleFunc("POST /query/validate", s.validateQuery)
}

// Start begins listening and serving HTTP requests
func (s *Server) Start() error {
	srv := &http.Server{
		Addr:         s.addr,
		Handler:      s.logging(s.mux),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	log.Printf("PiperDB daemon listening on %s", s.addr)
	return srv.ListenAndServe()
}

// StartWithContext starts the server with graceful shutdown support
func (s *Server) StartWithContext(ctx context.Context) error {
	srv := &http.Server{
		Addr:         s.addr,
		Handler:      s.logging(s.mux),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		log.Println("Shutting down...")
		srv.Shutdown(shutdownCtx)
	}()

	log.Printf("PiperDB daemon listening on %s", s.addr)
	return srv.ListenAndServe()
}

// --- Middleware ---

func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %v", r.Method, r.URL.Path, time.Since(start))
	})
}

// --- List handlers ---

func (s *Server) createList(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.ID == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}

	if err := s.db.CreateList(r.Context(), req.ID); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{"id": req.ID})
}

func (s *Server) listAllLists(w http.ResponseWriter, r *http.Request) {
	lists, err := s.db.ListAllLists(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if lists == nil {
		lists = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"lists": lists})
}

func (s *Server) getList(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	info, err := s.db.GetListInfo(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, info)
}

func (s *Server) deleteList(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if err := s.db.DeleteList(r.Context(), id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getSchema(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	schema, err := s.db.GetSchema(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, schema)
}

func (s *Server) getStats(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	stats, err := s.db.GetStats(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

// --- Item handlers ---

func (s *Server) addItems(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	// Decode body — accept a single object or an array of objects
	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	var items []map[string]interface{}

	// Try array first
	if err := json.Unmarshal(raw, &items); err != nil {
		// Try single object
		var single map[string]interface{}
		if err := json.Unmarshal(raw, &single); err != nil {
			writeError(w, http.StatusBadRequest, "body must be a JSON object or array of objects")
			return
		}
		items = []map[string]interface{}{single}
	}

	ids, err := s.db.AddItems(r.Context(), id, items)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{"ids": ids})
}

func (s *Server) getItems(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	opts := &types.QueryOptions{}
	if v := r.URL.Query().Get("limit"); v != "" {
		fmt.Sscanf(v, "%d", &opts.Limit)
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		fmt.Sscanf(v, "%d", &opts.Offset)
	}

	result, err := s.db.GetItems(r.Context(), id, opts)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) getItem(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	itemId := r.PathValue("itemId")

	item, err := s.db.GetItem(r.Context(), id, itemId)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, item)
}

func (s *Server) updateItem(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	itemId := r.PathValue("itemId")

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if err := s.db.UpdateItem(r.Context(), id, itemId, data); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"id": itemId})
}

func (s *Server) deleteItem(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	itemId := r.PathValue("itemId")

	if err := s.db.DeleteItem(r.Context(), id, itemId); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// --- Query handlers ---

func (s *Server) query(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var req struct {
		Pipe string `json:"pipe"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Pipe == "" {
		writeError(w, http.StatusBadRequest, "pipe expression is required")
		return
	}

	result, err := s.db.ExecutePipe(r.Context(), id, req.Pipe, nil)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) validateQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Pipe string `json:"pipe"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Pipe == "" {
		writeError(w, http.StatusBadRequest, "pipe expression is required")
		return
	}

	if err := s.db.ValidatePipe(r.Context(), req.Pipe); err != nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"valid": false,
			"error": err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"valid": true,
	})
}

// --- Response helpers ---

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
