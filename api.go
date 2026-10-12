package main

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
)

func registerAPI(mux *http.ServeMux, q *Queue) {
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
			http.Error(w, `body must be {"url": "..."}`, http.StatusBadRequest)
			return
		}
		id := uuid.NewString()
		if err := q.Enqueue(r.Context(), id, req.URL); err != nil {
			http.Error(w, "enqueue failed", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"id": id})
	})

	mux.HandleFunc("GET /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		res, err := q.Status(r.Context(), r.PathValue("id"))
		if err != nil {
			http.Error(w, "lookup failed", http.StatusInternalServerError)
			return
		}
		if len(res) == 0 {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(res)
	})

}
