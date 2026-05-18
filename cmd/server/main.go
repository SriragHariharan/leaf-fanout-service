package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"github.com/sriraghariharan/leaf-fanout-service/internal/db"
	"github.com/sriraghariharan/leaf-fanout-service/internal/kafka"
	"github.com/sriraghariharan/leaf-fanout-service/internal/kafka/consumers"
	"github.com/sriraghariharan/leaf-fanout-service/internal/repo"
	"github.com/sriraghariharan/leaf-fanout-service/internal/service"
)

func main() {
	loadDotEnv()

	//connect to database
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	_, err := db.Connect(ctx)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("database close: %v", err)
		}
	}()

	//connect to kafka
	err = kafka.ConnectKafka()
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer func() {
		kafka.CloseKafka()
	}()

	postRepo := repo.NewPostRepo(db.DB)
	fanoutRepo := repo.NewFanoutRepo(db.DB)
	fanoutSvc := service.NewService(postRepo, fanoutRepo)
	consumers.RunConsumers(ctx, "fanout-service-posts", fanoutSvc)

	fmt.Println("Hello, Welcome to the Fanout Service!")

	
	//gorilla mux router
	router := mux.NewRouter()
	//routes
	router.HandleFunc("/test", testHandler).Methods("GET")
	port := os.Getenv("PORT")
	if port == "" {
		port = "2005"
	}
	log.Fatal(http.ListenAndServe(":"+port, router))
}

//test handler
func testHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Fanout service running!"})
}

// loadDotEnv loads the first existing .env from cwd or parent dirs (e.g. module root when
// running `go run .` from cmd/server). Go does not read .env files without an explicit load.
func loadDotEnv() {
	candidates := []string{
		".env",
		filepath.Join("..", ".env"),
		filepath.Join("..", "..", ".env"),
		filepath.Join("..", "..", "..", ".env"),
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err != nil {
			continue
		}
		if err := godotenv.Load(p); err != nil {
			log.Printf("env: load %s: %v", p, err)
			return
		}
		return
	}
}
