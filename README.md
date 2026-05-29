# Fanout Service

## Service Name & Overview

The Fanout Service is an event-driven worker that distributes posts to follower feeds. It consumes `post.events` from Kafka, resolves each author's top friends via HTTP, and writes fanout records to CockroachDB/PostgreSQL shared with feed-service-go.

**Tech Stack**

- **Language:** Go 1.26.2
- **Framework:** Gorilla Mux (health endpoint only)
- **ORM:** GORM (PostgreSQL / CockroachDB driver)
- **Key libraries:** segmentio/kafka-go, gobreaker (circuit breaker), godotenv

## Architecture & Dependencies

### Internal Dependencies

| Dependency | Purpose |
|---|---|
| **CockroachDB / PostgreSQL** | Fanout and post tables via GORM (shared schema with feed-service-go) |
| **Kafka** | Consumes post lifecycle events |

### Event Contracts

See [`../KAFKA_TOPICS.txt`](../KAFKA_TOPICS.txt) for the platform topic list.

| Direction | Topic | Consumer group | Events |
|---|---|---|---|
| **Produces** | — | — | None (Kafka Writer is initialized but unused) |
| **Consumes** | `post.events` | `fanout-service-posts` | `post.created`, `post.edited`, `post.deleted` |

**Payload shapes:**

| eventType | Fields |
|---|---|
| `post.created` | `postID`, `imageURL`, `content`, `ownerID` |
| `post.edited` | `postID`, `imageURL`, `content`, `ownerID` |
| `post.deleted` | `postID` |

### External APIs

| Target | Env variable | Endpoint |
|---|---|---|
| **friends-service** | `FRIEND_IDS_FETCH_URL` | `GET /api/v1/friends/top-friend-ids?userId=` |

Called with a circuit breaker (`gobreaker`) to fetch ranked friend IDs for feed distribution.

## Environment Variables

```bash
# --- Server ---
PORT=2005

# --- Database (CockroachDB / PostgreSQL) ---
# COCKROACHDB_* are documentation helpers; runtime uses DATABASE_URL only
COCKROACHDB_PASSWORD=your-cockroach-password
COCKROACHDB_USER=your-cockroach-user
DATABASE_URL=postgresql://user:password@host:26257/defaultdb?sslmode=verify-full

# --- Kafka (local — plaintext Docker) ---
KAFKA_MODE=local
KAFKA_BROKERS=localhost:9092

# --- Kafka (Aiven — uncomment and set KAFKA_MODE=aiven) ---
# KAFKA_MODE=aiven
# KAFKA_BROKERS=your-service.a.aivencloud.com:12345
# KAFKA_SASL_USERNAME=your-aiven-username
# KAFKA_SASL_PASSWORD=your-aiven-password
# KAFKA_SASL_MECHANISM=scram-sha-256
# KAFKA_SSL_CA_PATH=./ca.pem
# KAFKA_SSL_CA=

# --- Inter-service ---
FRIEND_IDS_FETCH_URL=http://localhost:2003/api/v1/friends/top-friend-ids

# --- Optional: connection pool tuning ---
# DB_MAX_OPEN_CONNS=25
# DB_MAX_IDLE_CONNS=10
# DB_CONN_MAX_LIFETIME=30m
# DB_CONN_MAX_IDLE_TIME=5m
# GORM_LOG_LEVEL=warn
```

## Getting Started

### Prerequisites

- **Go** 1.26.2+
- **CockroachDB** or **PostgreSQL** (same database as feed-service-go)
- **Kafka** from parent docker-compose
- **friends-service** running on port 2003
- **post-service** running (publishes `post.events`)

### Local Infrastructure

```bash
# From the parent repo root (d:\main PROJECTS\leaf\)
docker compose up -d kafka
```

Ensure friends-service and post-service are running before fanout-service.

### Install & Run

```bash
cd fanout-service
cp .env.example .env
# Edit .env with your DATABASE_URL and FRIEND_IDS_FETCH_URL
go mod download
go run ./cmd/server
```

Verify: `curl http://localhost:2005/test` → `Fanout service running!`

## Available Scripts

This service has no `package.json`. Use Go commands directly:

| Command | Description |
|---|---|
| `go run ./cmd/server` | Start Kafka consumers and health endpoint |
| `go build -o fanout-service ./cmd/server` | Build a production binary |
| `go test ./...` | Run all tests |

## API / Event Interface

This service is **primarily an event-driven worker**. It is not exposed through the API gateway and has no public business HTTP endpoints.

### HTTP (health only)

| Method | Path | Description |
|---|---|---|
| `GET` | `/test` | Health check |

### Event-Driven Processing

On startup, the service connects to the database and Kafka, then runs a background consumer on `post.events`:

1. **`post.created`** — fetches top friend IDs from friends-service, writes fanout entries to each friend's feed
2. **`post.edited`** — updates existing fanout post content
3. **`post.deleted`** — removes fanout entries for the deleted post

All processing happens asynchronously via Kafka. The HTTP port exists only for health checks and graceful shutdown signaling.
