// Command desire-store-transport-spike measures the wire bytes per operation
// for the HYPERFLEET-1743 transport evaluation: partition reads and ReadDesire
// status writes against an in-memory Desire Store stand-in. The evaluation
// itself is arithmetic documented in the architecture repo
// (spike-desire-store-transport.md); this tool only supplies the measurements.
//
// Run from the repository root:
//
//	go run ./hack/spike/HYPERFLEET-1743
package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultConfigPath = "hack/spike/HYPERFLEET-1743/scenarios.json"
	resultsFileName   = "results.json"
	desiresPath       = "/desires"
	statusPathSuffix  = "/status"
	contentTypeJSON   = "application/json"
	clientTimeout     = 5 * time.Second
	maxWriteBodyBytes = 1 << 20
	workersPerOp      = 64
	dispatchTick      = time.Millisecond
	// Sized so transient runtime stalls delay open-loop dispatches instead of
	// dropping them; delayed dispatches surface as latency, not lost requests.
	jobsBuffer = 32768
)

type config struct {
	Fixtures struct {
		PartitionRead string `json:"partition_read"`
		StatusWrite   string `json:"status_write"`
	} `json:"fixtures"`
	Scenarios []scenario `json:"scenarios"`
	Settings  struct {
		WarmupSeconds   int `json:"warmup_seconds"`
		DurationSeconds int `json:"duration_seconds"`
		Repetitions     int `json:"repetitions"`
	} `json:"settings"`
	PartitionRead struct {
		PageSize int `json:"page_size"`
	} `json:"partition_read"`
}

type scenario struct {
	Name                         string  `json:"name"`
	ReadRequestsPerSecond        float64 `json:"read_requests_per_second"`
	StatusWriteRequestsPerSecond float64 `json:"status_write_requests_per_second"`
}

// statusWriteRequest mirrors the status-write fixture so the server fully
// unmarshals each request body on the measured path.
type statusWriteRequest struct {
	DesireID string       `json:"desire_id"`
	Status   desireStatus `json:"status"`
}

type desireStatus struct {
	Conditions         []statusCondition `json:"conditions"`
	Observed           observedStatus    `json:"observed"`
	ObservedGeneration int               `json:"observed_generation"`
}

type statusCondition struct {
	Message string `json:"message"`
	Reason  string `json:"reason"`
	Status  string `json:"status"`
	Type    string `json:"type"`
}

type observedStatus struct {
	Object            map[string]any `json:"object"`
	Replicas          int            `json:"replicas"`
	ReadyReplicas     int            `json:"ready_replicas"`
	AvailableReplicas int            `json:"available_replicas"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dir := filepath.Dir(defaultConfigPath)
	cfg, err := loadConfig(defaultConfigPath)
	if err != nil {
		return err
	}
	fx, err := loadFixtures(dir, cfg)
	if err != nil {
		return err
	}

	server, addr, err := startServer(fx.readValue)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := server.Close(); closeErr != nil {
			log.Printf("close server: %v", closeErr)
		}
	}()

	readCounter := &byteCounter{}
	writeCounter := &byteCounter{}
	readClient := newClient(readCounter)
	writeClient := newClient(writeCounter)
	defer readClient.CloseIdleConnections()
	defer writeClient.CloseIdleConnections()

	baseURL := "https://" + addr
	readOp := &opConfig{
		client:     readClient,
		method:     http.MethodGet,
		targetURL:  fmt.Sprintf("%s%s?limit=%d", baseURL, desiresPath, cfg.PartitionRead.PageSize),
		wantStatus: http.StatusOK,
		workers:    workersPerOp,
	}
	writeOp := &opConfig{
		client:     writeClient,
		method:     http.MethodPut,
		targetURL:  fmt.Sprintf("%s%s/%s%s", baseURL, desiresPath, url.PathEscape(fx.desireID), statusPathSuffix),
		body:       fx.writeBody,
		wantStatus: http.StatusNoContent,
		workers:    workersPerOp,
	}

	out := results{
		Scenarios:   nil,
		Go:          runtime.Version(),
		NumCPU:      runtime.NumCPU(),
		GeneratedAt: time.Now().UTC(),
	}
	for _, sc := range cfg.Scenarios {
		log.Printf("scenario %s: %.0f reads/s + %.0f status writes/s",
			sc.Name, sc.ReadRequestsPerSecond, sc.StatusWriteRequestsPerSecond)
		readOp.rate = sc.ReadRequestsPerSecond
		writeOp.rate = sc.StatusWriteRequestsPerSecond
		out.Scenarios = append(out.Scenarios,
			runScenario(cfg, sc, readOp, writeOp, readCounter, writeCounter))
	}

	outPath := filepath.Join(dir, resultsFileName)
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal results: %w", err)
	}
	if err := os.WriteFile(outPath, data, 0o600); err != nil {
		return fmt.Errorf("write results: %w", err)
	}
	printSummary(out, outPath)
	return nil
}

func loadConfig(path string) (config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return config{}, fmt.Errorf("read config: %w", err)
	}
	var cfg config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return config{}, fmt.Errorf("parse config: %w", err)
	}
	if cfg.Settings.WarmupSeconds <= 0 {
		cfg.Settings.WarmupSeconds = 15
	}
	if cfg.Settings.DurationSeconds <= 0 {
		cfg.Settings.DurationSeconds = 60
	}
	if cfg.Settings.Repetitions <= 0 {
		cfg.Settings.Repetitions = 3
	}
	return cfg, nil
}

type fixtures struct {
	readValue any
	desireID  string
	writeBody []byte
}

func loadFixtures(dir string, cfg config) (fixtures, error) {
	readRaw, err := os.ReadFile(filepath.Join(dir, cfg.Fixtures.PartitionRead))
	if err != nil {
		return fixtures{}, fmt.Errorf("read partition fixture: %w", err)
	}
	var readValue any
	if err = json.Unmarshal(readRaw, &readValue); err != nil {
		return fixtures{}, fmt.Errorf("parse partition fixture: %w", err)
	}

	writeRaw, err := os.ReadFile(filepath.Join(dir, cfg.Fixtures.StatusWrite))
	if err != nil {
		return fixtures{}, fmt.Errorf("read status-write fixture: %w", err)
	}
	var writeFixture statusWriteRequest
	if err = json.Unmarshal(writeRaw, &writeFixture); err != nil {
		return fixtures{}, fmt.Errorf("parse status-write fixture: %w", err)
	}

	// Send the compacted form: a real client marshals compact JSON, and the
	// pretty-printed fixture file would inflate request bytes by ~50%.
	var compact bytes.Buffer
	if err = json.Compact(&compact, writeRaw); err != nil {
		return fixtures{}, fmt.Errorf("compact status-write fixture: %w", err)
	}
	return fixtures{
		readValue: readValue,
		desireID:  writeFixture.DesireID,
		writeBody: compact.Bytes(),
	}, nil
}

// startServer runs the in-memory Desire Store stand-in on a random loopback
// port with a self-signed certificate and HTTP/1.1 pinned via ALPN.
func startServer(readFixture any) (*http.Server, string, error) {
	cert, err := selfSignedCertificate()
	if err != nil {
		return nil, "", err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", fmt.Errorf("listen on loopback: %w", err)
	}
	server := &http.Server{
		Handler: newHandler(readFixture),
		// An empty, non-nil TLSNextProto disables HTTP/2 so the benchmark
		// measures HTTP/1.1, the worst case for per-request header overhead.
		TLSNextProto:      make(map[string]func(*http.Server, *tls.Conn, http.Handler)),
		ReadHeaderTimeout: 10 * time.Second,
	}
	tlsListener := tls.NewListener(listener, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"http/1.1"},
	})
	go func() {
		if serveErr := server.Serve(tlsListener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Printf("server exited: %v", serveErr)
		}
	}()
	return server, listener.Addr().String(), nil
}

func selfSignedCertificate() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate serial: %w", err)
	}
	template := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "desire-store-transport-spike"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create certificate: %w", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

func newHandler(readFixture any) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == desiresPath:
			servePartitionRead(w, readFixture)
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, statusPathSuffix):
			serveStatusWrite(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}

func servePartitionRead(w http.ResponseWriter, readFixture any) {
	// Marshal per request so serialization stays on the measured path.
	body, err := json.Marshal(readFixture)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentTypeJSON)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(body); err != nil {
		// The client is gone; the failure surfaces client-side as an error.
		return
	}
}

func serveStatusWrite(w http.ResponseWriter, r *http.Request) {
	// Unmarshal per request so deserialization stays on the measured path.
	body, err := io.ReadAll(io.LimitReader(r.Body, maxWriteBodyBytes))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var payload statusWriteRequest
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// byteCounter tracks application-level bytes (after the TLS handshake, so TLS
// record overhead is excluded) for one operation's connection pool. Written
// bytes are client requests (server ingress); read bytes are server responses
// (server egress).
type byteCounter struct {
	written atomic.Int64
	read    atomic.Int64
}

func (c *byteCounter) snapshot() (written, read int64) {
	return c.written.Load(), c.read.Load()
}

type countingConn struct {
	net.Conn
	counter *byteCounter
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.counter.read.Add(int64(n))
	}
	return n, err
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.counter.written.Add(int64(n))
	}
	return n, err
}

func newClient(counter *byteCounter) *http.Client {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{"http/1.1"},
		// Loopback-only spike: the server certificate is self-signed and
		// generated in-process; verification is irrelevant to the decision.
		InsecureSkipVerify: true, // #nosec G402 -- loopback spike, self-signed in-process certificate
	}
	dialer := &net.Dialer{Timeout: clientTimeout}
	transport := &http.Transport{
		ForceAttemptHTTP2:   false,
		MaxConnsPerHost:     workersPerOp,
		MaxIdleConns:        workersPerOp,
		MaxIdleConnsPerHost: workersPerOp,
		DialTLSContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			rawConn, err := dialer.DialContext(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}
			tlsConn := tls.Client(rawConn, tlsConfig.Clone())
			// The handshake happens before the counting wrapper is applied,
			// so only post-handshake application bytes are counted.
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				return nil, errors.Join(err, tlsConn.Close())
			}
			return &countingConn{Conn: tlsConn, counter: counter}, nil
		},
	}
	return &http.Client{Transport: transport, Timeout: clientTimeout}
}

type opConfig struct {
	client     *http.Client
	targetURL  string
	method     string
	body       []byte
	wantStatus int
	rate       float64
	workers    int
}

type measurement struct {
	latency time.Duration
	failed  bool
}

type phaseResult struct {
	latencies  []time.Duration
	dispatched int64
	completed  int64
	missed     int64
	errors     int64
}

// runPhase drives one operation open-loop at a fixed rate for the given
// duration. Requests are dispatched on a fixed tick regardless of response
// time; when workers cannot keep up, dispatches are dropped and counted as
// missed instead of queueing, so latency and achieved rate stay honest.
func runPhase(op *opConfig, duration time.Duration) phaseResult {
	jobs := make(chan struct{}, jobsBuffer)
	outcomes := make([][]measurement, op.workers)

	var wg sync.WaitGroup
	for i := range outcomes {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			outcomes[idx] = make([]measurement, 0, len(outcomes))
			for range jobs {
				outcomes[idx] = append(outcomes[idx], doRequest(op))
			}
		}(i)
	}

	var dispatched, missed int64
	start := time.Now()
	ticker := time.NewTicker(dispatchTick)
	defer ticker.Stop()

	tryDispatch := func(need int64) {
		for i := int64(0); i < need; i++ {
			select {
			case jobs <- struct{}{}:
				dispatched++
			default:
				missed += need - i
				return
			}
		}
	}

	for now := range ticker.C {
		elapsed := now.Sub(start)
		if elapsed >= duration {
			break
		}
		tryDispatch(int64(elapsed.Seconds()*op.rate) - dispatched - missed)
	}
	close(jobs)
	wg.Wait()

	result := phaseResult{dispatched: dispatched, missed: missed}
	for _, local := range outcomes {
		result.completed += int64(len(local))
		for _, m := range local {
			if m.failed {
				result.errors++
			}
			result.latencies = append(result.latencies, m.latency)
		}
	}
	return result
}

func doRequest(op *opConfig) measurement {
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()

	start := time.Now()
	var body io.Reader
	if op.body != nil {
		body = bytes.NewReader(op.body)
	}
	req, err := http.NewRequestWithContext(ctx, op.method, op.targetURL, body)
	if err != nil {
		return measurement{latency: time.Since(start), failed: true}
	}
	if op.body != nil {
		req.Header.Set("Content-Type", contentTypeJSON)
	}
	// #nosec G704 -- the target URL is derived from the in-process loopback
	// listener and the version-controlled fixture, not from user input.
	resp, err := op.client.Do(req)
	if err != nil {
		return measurement{latency: time.Since(start), failed: true}
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		closeBody(resp.Body)
		return measurement{latency: time.Since(start), failed: true}
	}
	closeBody(resp.Body)
	return measurement{latency: time.Since(start), failed: resp.StatusCode != op.wantStatus}
}

func closeBody(body io.ReadCloser) {
	if err := body.Close(); err != nil {
		// The body was already drained; close errors are not actionable here.
		log.Printf("close response body: %v", err)
	}
}

// runCombined runs both operations concurrently for one phase. Warmup and
// measurement phases both use combined load.
func runCombined(readOp, writeOp *opConfig, duration time.Duration) (phaseResult, phaseResult) {
	readDone := make(chan phaseResult, 1)
	writeDone := make(chan phaseResult, 1)
	go func() { readDone <- runPhase(readOp, duration) }()
	go func() { writeDone <- runPhase(writeOp, duration) }()
	return <-readDone, <-writeDone
}

func runScenario(
	cfg config,
	sc scenario,
	readOp, writeOp *opConfig,
	readCounter, writeCounter *byteCounter,
) scenarioResult {
	result := scenarioResult{Name: sc.Name, Reps: nil}
	warmup := time.Duration(cfg.Settings.WarmupSeconds) * time.Second
	duration := time.Duration(cfg.Settings.DurationSeconds) * time.Second
	seconds := duration.Seconds()

	for rep := 1; rep <= cfg.Settings.Repetitions; rep++ {
		log.Printf("scenario %s: rep %d/%d (warmup %s, measure %s)",
			sc.Name, rep, cfg.Settings.Repetitions, warmup, duration)
		// Warmup establishes the keep-alive connection pools; results discarded.
		runCombined(readOp, writeOp, warmup)

		readBeforeWritten, readBeforeRead := readCounter.snapshot()
		writeBeforeWritten, writeBeforeRead := writeCounter.snapshot()
		readRes, writeRes := runCombined(readOp, writeOp, duration)
		readAfterWritten, readAfterRead := readCounter.snapshot()
		writeAfterWritten, writeAfterRead := writeCounter.snapshot()

		readReq := readAfterWritten - readBeforeWritten
		readResp := readAfterRead - readBeforeRead
		writeReq := writeAfterWritten - writeBeforeWritten
		writeResp := writeAfterRead - writeBeforeRead

		result.Reps = append(result.Reps, repResult{
			Read:        buildOpResult(readRes, readReq, readResp, seconds),
			StatusWrite: buildOpResult(writeRes, writeReq, writeResp, seconds),
			IngressMbps: mbps(readReq+writeReq, seconds),
			EgressMbps:  mbps(readResp+writeResp, seconds),
		})
	}
	return result
}

func buildOpResult(res phaseResult, reqBytes, respBytes int64, seconds float64) opResult {
	median, p95 := latencyPercentiles(res.latencies)
	return opResult{
		Completed:         res.completed,
		Errors:            res.errors,
		Missed:            res.missed,
		RequestBytes:      reqBytes,
		ResponseBytes:     respBytes,
		RequestBytesEach:  reqBytes / max(res.completed, 1),
		ResponseBytesEach: respBytes / max(res.completed, 1),
		AchievedRPS:       float64(res.completed) / seconds,
		MedianLatencyMs:   float64(median) / float64(time.Millisecond),
		P95LatencyMs:      float64(p95) / float64(time.Millisecond),
	}
}

func latencyPercentiles(latencies []time.Duration) (median, p95 time.Duration) {
	if len(latencies) == 0 {
		return 0, 0
	}
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := func(quantile float64) int {
		idx := int(math.Ceil(quantile * float64(len(sorted))))
		if idx < 1 {
			idx = 1
		}
		if idx > len(sorted) {
			idx = len(sorted)
		}
		return idx - 1
	}
	return sorted[index(0.5)], sorted[index(0.95)]
}

func mbps(bytes int64, seconds float64) float64 {
	if seconds <= 0 {
		return 0
	}
	return float64(bytes) * 8 / (seconds * 1e6)
}

type opResult struct {
	RequestBytes      int64   `json:"request_bytes"`
	ResponseBytes     int64   `json:"response_bytes"`
	RequestBytesEach  int64   `json:"request_bytes_each"`
	ResponseBytesEach int64   `json:"response_bytes_each"`
	Completed         int64   `json:"completed"`
	Errors            int64   `json:"errors"`
	Missed            int64   `json:"missed_dispatches"`
	AchievedRPS       float64 `json:"achieved_rps"`
	MedianLatencyMs   float64 `json:"median_latency_ms"`
	P95LatencyMs      float64 `json:"p95_latency_ms"`
}

type repResult struct {
	Read        opResult `json:"read"`
	StatusWrite opResult `json:"status_write"`
	IngressMbps float64  `json:"ingress_mbps"`
	EgressMbps  float64  `json:"egress_mbps"`
}

type scenarioResult struct {
	Name string      `json:"name"`
	Reps []repResult `json:"reps"`
}

type results struct {
	GeneratedAt time.Time        `json:"generated_at"`
	Go          string           `json:"go"`
	Scenarios   []scenarioResult `json:"scenarios"`
	NumCPU      int              `json:"num_cpu"`
}

func printSummary(out results, outPath string) {
	fmt.Printf("results written to %s\n", outPath)
	for _, sc := range out.Scenarios {
		for _, rep := range sc.Reps {
			fmt.Printf("scenario %-8s: read %.0f rps, %d B req / %d B resp, p95 %.2f ms | "+
				"write %.0f rps, %d B req / %d B resp, p95 %.2f ms | "+
				"egress %.1f Mbps, errors %d\n",
				sc.Name,
				rep.Read.AchievedRPS, rep.Read.RequestBytesEach, rep.Read.ResponseBytesEach,
				rep.Read.P95LatencyMs,
				rep.StatusWrite.AchievedRPS, rep.StatusWrite.RequestBytesEach,
				rep.StatusWrite.ResponseBytesEach, rep.StatusWrite.P95LatencyMs,
				rep.EgressMbps, rep.Read.Errors+rep.StatusWrite.Errors)
		}
	}
}
