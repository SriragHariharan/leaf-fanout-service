package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func main() {
	//gorilla mux router
	router := mux.NewRouter()
	router.HandleFunc("/test", testHandler).Methods("GET")
	//start the server
	log.Fatal(http.ListenAndServe(":4041", router))
}

//test handler
func testHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Fanout service running!"})
}