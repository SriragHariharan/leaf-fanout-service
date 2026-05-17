// Package friends calls friends-service to get friend IDs for fanout.
package friends

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sony/gobreaker"
)

const envFriendIDsFetchURL = "FRIEND_IDS_FETCH_URL"

// These are set once on the first call (after main loads .env).
var (
	setupOnce  sync.Once
	setupErr   error
	baseURL    string
	httpClient *http.Client
	breaker    *gobreaker.CircuitBreaker
)

func setup() {
	setupOnce.Do(func() {
		baseURL = strings.TrimSpace(os.Getenv(envFriendIDsFetchURL))
		if baseURL == "" {
			setupErr = fmt.Errorf("%s must be set in .env", envFriendIDsFetchURL)
			return
		}

		httpClient = &http.Client{Timeout: 10 * time.Second}

		breaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:        "friends-service",
			MaxRequests: 3,
			Timeout:     30 * time.Second,
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				return counts.ConsecutiveFailures >= 5
			},
		})
	})
}

// FetchTopFriendIDs calls GET /top-friend-ids?userId=... and returns a list of friend IDs.
func FetchTopFriendIDs(ctx context.Context, userID string) ([]string, error) {
	setup()
	if setupErr != nil {
		return nil, setupErr
	}

	// Circuit breaker runs the HTTP call. If friends-service fails too often, it stops trying.
	result, err := breaker.Execute(func() (interface{}, error) {
		return doGET(ctx, userID)
	})
	if err != nil {
		if errors.Is(err, gobreaker.ErrOpenState) {
			return nil, fmt.Errorf("friends-service is down (circuit open)")
		}
		return nil, err
	}

	friendIDs, ok := result.([]string)
	if !ok {
		return nil, fmt.Errorf("unexpected response from friends-service")
	}
	return friendIDs, nil
}

// doGET performs the actual HTTP request.
func doGET(ctx context.Context, userID string) ([]string, error) {
	fullURL := baseURL + "?userId=" + url.QueryEscape(userID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("friends-service returned status %d", resp.StatusCode)
	}

	var friendIDs []string
	if err := json.NewDecoder(resp.Body).Decode(&friendIDs); err != nil {
		return nil, fmt.Errorf("could not read response: %w", err)
	}

	return friendIDs, nil
}
