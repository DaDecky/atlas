package sensor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/sony/gobreaker/v2"
)

// ContractError carries the raw body of a response that breaks the contract, so it can be parked.
type ContractError struct {
	Raw    []byte
	Reason string
}

// Client calls the sensor through one breaker. It holds its own HTTP client, so the
// sensor's slow calls cannot use the connections of other sources.
type Client struct {
	origin, token string
	http          *http.Client
	timeout       time.Duration
	breaker       *gobreaker.TwoStepCircuitBreaker[struct{}]
}

const (
	// Five failures in a row open the breaker, then one probe every 2 s caps the load at 0.5 requests per second.
	tripAfter    = 5
	probeDelay   = 2 * time.Second
	maxBodyBytes = 4 << 20
)

// ErrOpen means the breaker refused the call, so no request reached the sensor.
var ErrOpen = errors.New("sensor breaker is open")

func (e *ContractError) Error() string { return "sensor contract violation: " + e.Reason }

func NewClient(origin, token string, client *http.Client, timeout time.Duration, onChange func(from, to string)) *Client {
	settings := gobreaker.Settings{
		Name:        "sensor-pantai",
		MaxRequests: 1,
		Timeout:     probeDelay,
		ReadyToTrip: func(counts gobreaker.Counts) bool { return counts.ConsecutiveFailures >= tripAfter },
		OnStateChange: func(_ string, from, to gobreaker.State) {
			onChange(from.String(), to.String())
		},
		IsExcluded: func(err error) bool { return errors.Is(err, context.Canceled) },
	}

	return &Client{origin: origin, token: token, http: client, timeout: timeout, breaker: gobreaker.NewTwoStepCircuitBreaker[struct{}](settings)}
}

func (c *Client) Observations(ctx context.Context, cursor string, limit int) (Page, error) {
	query := url.Values{"limit": {strconv.Itoa(limit)}}
	if cursor != "" {
		query.Set("cursor", cursor)
	}

	return call(ctx, c, "/v1/observations?"+query.Encode(), DecodePage)
}

func (c *Client) Stations(ctx context.Context) (map[string]Station, error) {
	return call(ctx, c, "/v1/stations", DecodeStations)
}

// call counts a broken body as a failure, because a sick sensor can answer 200 with bad data.
func call[T any](ctx context.Context, c *Client, path string, decode func([]byte) (T, error)) (T, error) {
	var value T

	done, err := c.breaker.Allow()
	if err != nil {
		return value, ErrOpen
	}

	body, err := c.get(ctx, path)
	if err == nil {
		value, err = decode(body)
		if errors.Is(err, ErrContract) {
			err = &ContractError{Raw: body, Reason: err.Error()}
		}
	}

	done(err)

	return value, err
}

func (c *Client) get(ctx context.Context, path string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.origin+path, nil)
	if err != nil {
		return nil, fmt.Errorf("build sensor request: %w", err)
	}

	request.Header.Set("Authorization", "Bearer "+c.token)

	response, err := c.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("sensor request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sensor status %d", response.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, maxBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read sensor body: %w", err)
	}

	if len(body) > maxBodyBytes {
		return nil, errors.New("sensor body larger than 4 MiB")
	}

	return body, nil
}
