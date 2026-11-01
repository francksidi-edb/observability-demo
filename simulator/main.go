// obs-simulator: generates a realistic stream of logs and metrics for a fleet of
// application services and Postgres clusters and publishes it to Kafka.
//
//	./obs-simulator -brokers localhost:9092 -rate 50000
//	./obs-simulator -dry-run /tmp/sample -duration 20s      (writes NDJSON, no Kafka)
//
// A small control API (default :8090) lets the dashboard start, stop, change the rate and
// trigger the "bad deploy" incident on the payments service.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ------------------------------------------------------------------ configuration
type config struct {
	brokers        string
	logsTopic      string
	metricsTopic   string
	partitions     int
	rate           int64
	workers        int
	scale          int
	metricInterval time.Duration
	httpAddr       string
	dryRun         string
	duration       time.Duration
	autostart      bool
	seed           uint64
}

// ------------------------------------------------------------------ shared state
type state struct {
	running      atomic.Bool
	rate         atomic.Int64
	sentLogs     atomic.Uint64
	sentMetrics  atomic.Uint64
	sentBytes    atomic.Uint64
	errors       atomic.Uint64
	incidentSvc  atomic.Value // string ("" = none)
	incidentFrom atomic.Int64 // unix ms
	incidentTill atomic.Int64 // unix ms, 0 = until switched off
	startedAt    time.Time

	mu       sync.Mutex
	lastSent uint64
	lastAt   time.Time
	eps      float64
}

var st state

func incident(svc string) bool {
	cur, _ := st.incidentSvc.Load().(string)
	if cur == "" || cur != svc {
		return false
	}
	till := st.incidentTill.Load()
	if till != 0 && time.Now().UnixMilli() > till {
		st.incidentSvc.Store("")
		return false
	}
	return true
}

// ------------------------------------------------------------------ sinks
type sink interface {
	emit(topic string, payload []byte)
	close()
}

// ------------------------------------------------------------------ fleet
type service struct {
	name     string
	weight   float64
	hosts    []string
	baseMs   float64 // median request latency
	errRate  float64
	warnRate float64
}

type fleet struct {
	apps     []*service
	pg       []*service // one entry per Postgres cluster, name "postgres"
	pgCl     []string   // cluster names parallel to pg
	cum      []float64  // cumulative weights over apps+pg
	all      []*service
	regions  []string
}

func hostsFor(prefix string, n int) []string {
	h := make([]string, n)
	for i := range h {
		h[i] = fmt.Sprintf("%s-%02d", prefix, i+1)
	}
	return h
}

func newFleet(scale int) *fleet {
	f := &fleet{regions: []string{"eu-west-1", "us-east-1", "ap-south-1"}}
	f.apps = []*service{
		{"checkout", 0.20, hostsFor("checkout", 40*scale), 80, 0.020, 0.10},
		{"payments", 0.15, hostsFor("payments", 30*scale), 120, 0.015, 0.08},
		{"auth", 0.14, hostsFor("auth", 24*scale), 30, 0.010, 0.06},
		{"search", 0.20, hostsFor("search", 48*scale), 60, 0.020, 0.10},
		{"inventory", 0.09, hostsFor("inventory", 24*scale), 45, 0.012, 0.07},
		{"notifications", 0.04, hostsFor("notifications", 16*scale), 25, 0.015, 0.06},
	}
	clusters := []struct {
		name string
		n    int
	}{{"orders", 6}, {"payments", 6}, {"users", 4}, {"inventory", 4}, {"analytics", 3}}
	for _, c := range clusters {
		f.pg = append(f.pg, &service{"postgres", 0.12 / float64(len(clusters)), hostsFor("pg-"+c.name, c.n*scale), 3, 0.004, 0.05})
		f.pgCl = append(f.pgCl, c.name)
	}
	f.all = append(append([]*service{}, f.apps...), f.pg...)
	total := 0.0
	for _, s := range f.all {
		total += s.weight
		f.cum = append(f.cum, total)
	}
	for i := range f.cum {
		f.cum[i] /= total
	}
	return f
}

func (f *fleet) pick(r *rand.Rand) (int, *service) {
	x := r.Float64()
	for i, c := range f.cum {
		if x <= c {
			return i, f.all[i]
		}
	}
	return len(f.all) - 1, f.all[len(f.all)-1]
}

// ------------------------------------------------------------------ event generation
var endpoints = map[string][]string{
	"checkout":      {"/cart", "/cart/items", "/checkout/start", "/checkout/confirm", "/shipping/options"},
	"payments":      {"/payments/authorize", "/payments/capture", "/payments/refund", "/payments/methods"},
	"auth":          {"/login", "/token/refresh", "/logout", "/session"},
	"search":        {"/search", "/suggest", "/filters", "/search/similar"},
	"inventory":     {"/stock/check", "/stock/reserve", "/stock/release", "/warehouses"},
	"notifications": {"/email/send", "/push/send", "/sms/send"},
}
var upstream = []string{"payments-gateway", "inventory-api", "auth-service", "search-index", "postgres-primary", "redis-cache"}
var queries = []string{
	"SELECT * FROM orders WHERE customer_id = $1 ORDER BY created_at DESC LIMIT 20",
	"UPDATE payments SET status = 'captured' WHERE id = $1",
	"INSERT INTO audit_log (actor, action, at) VALUES ($1, $2, now())",
	"SELECT count(*) FROM sessions WHERE expires_at > now()",
	"UPDATE inventory SET reserved = reserved + $1 WHERE sku = $2",
	"SELECT p.* FROM products p JOIN stock s ON s.sku = p.sku WHERE s.qty > 0",
}
var versions = []string{"v2.41.0", "v2.41.1", "v2.40.7"}

func logNormal(r *rand.Rand, median, sigma float64) float64 {
	return median * math.Exp(sigma*r.NormFloat64())
}

func appendQ(b []byte, s string) []byte {
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"' || c == '\\':
			b = append(b, '\\', c)
		case c < 0x20:
			b = append(b, ' ')
		default:
			b = append(b, c)
		}
	}
	return append(b, '"')
}

func uuid(r *rand.Rand, b []byte) []byte {
	const hex = "0123456789abcdef"
	x, y := r.Uint64(), r.Uint64()
	var raw [16]byte
	for i := 0; i < 8; i++ {
		raw[i] = byte(x >> (8 * i))
		raw[8+i] = byte(y >> (8 * i))
	}
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	for i, v := range raw {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			b = append(b, '-')
		}
		b = append(b, hex[v>>4], hex[v&15])
	}
	return b
}

type logEvent struct {
	sev, msg, ver, db string
	status, dur       int
}

func genApp(r *rand.Rand, f *fleet, s *service, host string, inc bool, newVer bool) logEvent {
	ep := endpoints[s.name]
	path := ep[r.IntN(len(ep))]
	ver := versions[r.IntN(len(versions))]
	errRate, warnRate, mult := s.errRate, s.warnRate, 1.0
	if inc && newVer {
		ver = "v2.42.0"
		errRate, warnRate, mult = 0.38, 0.30, 7.0
	} else if inc {
		mult = 1.4
	}
	dur := int(logNormal(r, s.baseMs*mult, 0.55))
	x := r.Float64()
	switch {
	case x < errRate*0.04:
		return logEvent{"FATAL", "out of memory: worker killed by supervisor, restarting", ver, "", 500, dur}
	case x < errRate:
		switch r.IntN(4) {
		case 0:
			d := dur + 2000 + r.IntN(3000)
			if inc && newVer && s.name == "payments" {
				return logEvent{"ERROR", fmt.Sprintf("payment authorization failed: timeout acquiring database lock after %d ms (txn %d)", d, r.IntN(900000)), ver, "", 500, d}
			}
			return logEvent{"ERROR", fmt.Sprintf("upstream timeout calling %s after %d ms", upstream[r.IntN(len(upstream))], d), ver, "", 504, d}
		case 1:
			return logEvent{"ERROR", fmt.Sprintf("upstream connection refused by %s on attempt %d", upstream[r.IntN(len(upstream))], 1+r.IntN(3)), ver, "", 502, dur}
		case 2:
			return logEvent{"ERROR", fmt.Sprintf("unhandled exception in handler %s: NullPointerException at line %d", path, 10+r.IntN(400)), ver, "", 500, dur}
		default:
			return logEvent{"ERROR", fmt.Sprintf("request %s failed with status 503: service temporarily unavailable", path), ver, "", 503, dur}
		}
	case x < errRate+warnRate:
		if r.IntN(2) == 0 {
			return logEvent{"WARN", fmt.Sprintf("slow request %s took %d ms (threshold 500 ms)", path, dur+400), ver, "", 200, dur + 400}
		}
		return logEvent{"WARN", fmt.Sprintf("retrying call to %s, attempt %d of 3", upstream[r.IntN(len(upstream))], 1+r.IntN(3)), ver, "", 200, dur}
	default:
		return logEvent{"INFO", fmt.Sprintf("%s %s %d %d ms", []string{"GET", "POST", "PUT"}[r.IntN(3)], path, 200+[]int{0, 0, 0, 1, 4}[r.IntN(5)], dur), ver, "", 200, dur}
	}
}

func genPG(r *rand.Rand, cluster string, inc bool) logEvent {
	db := cluster
	hot := inc && cluster == "payments"
	dur := int(logNormal(r, 3, 0.8))
	x := r.Float64()
	if hot {
		dur = int(logNormal(r, 180, 0.9))
		switch {
		case x < 0.03:
			return logEvent{"FATAL", "remaining connection slots are reserved for non-replication superuser connections", "", db, 0, 0}
		case x < 0.14:
			return logEvent{"ERROR", fmt.Sprintf("canceling statement due to lock timeout while updating tuple (0,%d) in relation \"payments\"", r.IntN(500)), "", db, 0, 5000}
		case x < 0.22:
			return logEvent{"ERROR", fmt.Sprintf("deadlock detected: process %d waits for ShareLock on transaction %d", 1000+r.IntN(9000), r.IntN(900000)), "", db, 0, 1000 + r.IntN(2000)}
		case x < 0.55:
			return logEvent{"WARN", fmt.Sprintf("process %d still waiting for ShareLock on transaction %d after 1000.%03d ms", 1000+r.IntN(9000), r.IntN(900000), r.IntN(1000)), "", db, 0, 1000}
		}
	}
	switch {
	case x < 0.002:
		return logEvent{"ERROR", fmt.Sprintf("could not receive data from client: Connection reset by peer (process %d)", 1000+r.IntN(9000)), "", db, 0, 0}
	case x < 0.03:
		return logEvent{"WARN", fmt.Sprintf("checkpoints are occurring too frequently (%d seconds apart)", 5+r.IntN(25)), "", db, 0, 0}
	case x < 0.06:
		n := 2000 + r.IntN(40000)
		return logEvent{"INFO", fmt.Sprintf("checkpoint complete: wrote %d buffers (%.1f%%); write=%.3f s, sync=%.3f s", n, float64(n)/1310.72, r.Float64()*30, r.Float64()), "", db, 0, 0}
	case x < 0.15:
		return logEvent{"INFO", fmt.Sprintf("connection authorized: user=app database=%s application_name=%s-api", db, db), "", db, 0, 0}
	default:
		return logEvent{"INFO", fmt.Sprintf("duration: %d.%03d ms statement: %s", dur, r.IntN(1000), queries[r.IntN(len(queries))]), "", db, 0, dur}
	}
}

func buildLog(r *rand.Rand, f *fleet, now time.Time, buf []byte) []byte {
	idx, s := f.pick(r)
	host := s.hosts[r.IntN(len(s.hosts))]
	var ev logEvent
	if s.name == "postgres" {
		cl := f.pgCl[idx-len(f.apps)]
		ev = genPG(r, cl, incident("payments"))
	} else {
		// 60% of payments hosts run the faulty release while the incident is active.
		newVer := (hostHash(host) % 10) < 6
		ev = genApp(r, f, s, host, incident(s.name), newVer)
	}
	buf = append(buf, `{"ts":"`...)
	buf = now.UTC().AppendFormat(buf, "2006-01-02 15:04:05.000")
	buf = append(buf, `","service":`...)
	buf = appendQ(buf, s.name)
	buf = append(buf, `,"host":`...)
	buf = appendQ(buf, host)
	buf = append(buf, `,"severity":`...)
	buf = appendQ(buf, ev.sev)
	buf = append(buf, fmt.Sprintf(`,"status":%d,"duration_ms":%d,"trace_id":"`, ev.status, ev.dur)...)
	buf = uuid(r, buf)
	buf = append(buf, `","message":`...)
	buf = appendQ(buf, ev.msg)
	buf = append(buf, `,"attrs":{"region":`...)
	buf = appendQ(buf, f.regions[hostHash(host)%3])
	if s.name == "postgres" {
		buf = append(buf, `,"cluster":`...)
		buf = appendQ(buf, ev.db)
	} else {
		buf = append(buf, `,"version":`...)
		buf = appendQ(buf, ev.ver)
	}
	return append(buf, "}}"...)
}

func hostHash(h string) uint32 {
	var x uint32 = 2166136261
	for i := 0; i < len(h); i++ {
		x = (x ^ uint32(h[i])) * 16777619
	}
	return x
}

// ------------------------------------------------------------------ metrics
func emitMetrics(r *rand.Rand, f *fleet, sk sink, topic string, now time.Time) uint64 {
	ts := now.UTC().Format("2006-01-02 15:04:05")
	var n uint64
	put := func(host, svc, metric string, v float64) {
		b := make([]byte, 0, 128)
		b = append(b, `{"ts":"`...)
		b = append(b, ts...)
		b = append(b, `","host":`...)
		b = appendQ(b, host)
		b = append(b, `,"service":`...)
		b = appendQ(b, svc)
		b = append(b, `,"metric":`...)
		b = appendQ(b, metric)
		b = append(b, fmt.Sprintf(`,"value":%.3f}`, v)...)
		sk.emit(topic, b)
		n++
	}
	inc := incident("payments")
	for _, s := range f.apps {
		for _, h := range s.hosts {
			bad := incident(s.name) && hostHash(h)%10 < 6
			cpu, lat := 35+r.Float64()*15, logNormal(r, s.baseMs*2.2, 0.2)
			if bad {
				cpu, lat = 78+r.Float64()*18, logNormal(r, s.baseMs*14, 0.25)
			}
			put(h, s.name, "cpu_pct", cpu)
			put(h, s.name, "mem_pct", 50+r.Float64()*12)
			put(h, s.name, "req_per_s", 80+r.Float64()*40)
			put(h, s.name, "latency_p99_ms", lat)
		}
	}
	for i, s := range f.pg {
		cl := f.pgCl[i]
		hot := inc && cl == "payments"
		for _, h := range s.hosts {
			conns, tps, locks, lag, hit := 80+r.Float64()*40, 900+r.Float64()*300, r.Float64()*2, r.Float64()*5, 99.2+r.Float64()*0.7
			if hot {
				conns, tps, locks, lag, hit = 300+r.Float64()*80, 250+r.Float64()*100, 40+r.Float64()*30, 80+r.Float64()*120, 96+r.Float64()*2
			}
			put(h, "postgres", "pg_connections", conns)
			put(h, "postgres", "pg_tps", tps)
			put(h, "postgres", "pg_lock_waits", locks)
			put(h, "postgres", "pg_repl_lag_ms", lag)
			put(h, "postgres", "pg_cache_hit_pct", hit)
		}
	}
	return n
}

// ------------------------------------------------------------------ run loops
func logWorker(ctx context.Context, id int, cfg config, f *fleet, sk sink) {
	r := rand.New(rand.NewPCG(cfg.seed, uint64(id)+1))
	const tick = 20 * time.Millisecond
	t := time.NewTicker(tick)
	defer t.Stop()
	var acc float64
	buf := make([]byte, 0, 512)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if !st.running.Load() {
			continue
		}
		acc += float64(st.rate.Load()) / float64(cfg.workers) * tick.Seconds()
		n := int(acc)
		acc -= float64(n)
		now := time.Now()
		var bytes uint64
		for i := 0; i < n; i++ {
			buf = buildLog(r, f, now, buf[:0])
			p := make([]byte, len(buf))
			copy(p, buf)
			sk.emit(cfg.logsTopic, p)
			bytes += uint64(len(p))
		}
		st.sentLogs.Add(uint64(n))
		st.sentBytes.Add(bytes)
	}
}

func metricLoop(ctx context.Context, cfg config, f *fleet, sk sink) {
	r := rand.New(rand.NewPCG(cfg.seed, 9999))
	t := time.NewTicker(cfg.metricInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if st.running.Load() {
			st.sentMetrics.Add(emitMetrics(r, f, sk, cfg.metricsTopic, time.Now()))
		}
	}
}

func epsLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cur := st.sentLogs.Load()
			st.mu.Lock()
			now := time.Now()
			if !st.lastAt.IsZero() {
				st.eps = float64(cur-st.lastSent) / now.Sub(st.lastAt).Seconds()
			}
			st.lastSent, st.lastAt = cur, now
			st.mu.Unlock()
		}
	}
}

// ------------------------------------------------------------------ control API
func serveAPI(cfg config, f *fleet) {
	hosts := 0
	for _, s := range f.all {
		hosts += len(s.hosts)
	}
	mux := http.NewServeMux()
	reply := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(v)
	}
	status := func() map[string]any {
		st.mu.Lock()
		eps := st.eps
		st.mu.Unlock()
		svc, _ := st.incidentSvc.Load().(string)
		active := svc != "" && incident(svc)
		return map[string]any{
			"running": st.running.Load(), "target_rate": st.rate.Load(), "eps": math.Round(eps),
			"sent_logs": st.sentLogs.Load(), "sent_metrics": st.sentMetrics.Load(), "sent_bytes": st.sentBytes.Load(),
			"errors": st.errors.Load(), "hosts": hosts, "brokers": cfg.brokers, "dry_run": cfg.dryRun != "",
			"incident": map[string]any{"active": active, "service": svc, "since_ms": st.incidentFrom.Load()},
			"uptime_s": int(time.Since(st.startedAt).Seconds()),
		}
	}
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) { reply(w, status()) })
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		if v := r.URL.Query().Get("rate"); v != "" {
			var n int64
			fmt.Sscan(v, &n)
			if n > 0 {
				st.rate.Store(n)
			}
		}
		st.running.Store(true)
		reply(w, status())
	})
	mux.HandleFunc("/stop", func(w http.ResponseWriter, r *http.Request) { st.running.Store(false); reply(w, status()) })
	mux.HandleFunc("/rate", func(w http.ResponseWriter, r *http.Request) {
		var n int64
		fmt.Sscan(r.URL.Query().Get("eps"), &n)
		if n < 1 || n > 2000000 {
			http.Error(w, "eps must be 1..2000000", 400)
			return
		}
		st.rate.Store(n)
		reply(w, status())
	})
	mux.HandleFunc("/incident", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("on") == "0" {
			st.incidentSvc.Store("")
			reply(w, status())
			return
		}
		svc := q.Get("service")
		if svc == "" {
			svc = "payments"
		}
		st.incidentSvc.Store(svc)
		st.incidentFrom.Store(time.Now().UnixMilli())
		var mins int64
		fmt.Sscan(q.Get("minutes"), &mins)
		if mins > 0 {
			st.incidentTill.Store(time.Now().Add(time.Duration(mins) * time.Minute).UnixMilli())
		} else {
			st.incidentTill.Store(0)
		}
		reply(w, status())
	})
	go func() { log.Println("control API on", cfg.httpAddr); log.Fatal(http.ListenAndServe(cfg.httpAddr, mux)) }()
}

// ------------------------------------------------------------------ main
func main() {
	var cfg config
	flag.StringVar(&cfg.brokers, "brokers", "localhost:9092", "Kafka bootstrap brokers, comma separated")
	flag.StringVar(&cfg.logsTopic, "logs-topic", "logs", "topic for log events")
	flag.StringVar(&cfg.metricsTopic, "metrics-topic", "metrics", "topic for metric samples")
	flag.IntVar(&cfg.partitions, "partitions", 8, "partitions used when the simulator creates the topics")
	flag.Int64Var(&cfg.rate, "rate", 20000, "initial log events per second")
	flag.IntVar(&cfg.workers, "workers", 8, "generator goroutines")
	flag.IntVar(&cfg.scale, "scale", 1, "fleet size multiplier (1 = about 215 hosts)")
	flag.DurationVar(&cfg.metricInterval, "metric-interval", 5*time.Second, "metric sampling interval per host")
	flag.StringVar(&cfg.httpAddr, "http", ":8090", "control API listen address")
	flag.StringVar(&cfg.dryRun, "dry-run", "", "write NDJSON to <path>.logs.ndjson and <path>.metrics.ndjson instead of Kafka")
	flag.DurationVar(&cfg.duration, "duration", 0, "stop after this long (0 = run until killed)")
	flag.BoolVar(&cfg.autostart, "autostart", true, "start generating immediately")
	flag.Uint64Var(&cfg.seed, "seed", uint64(time.Now().UnixNano()), "random seed")
	incOn := flag.String("incident", "", "start with an incident on this service, e.g. payments")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cfg.duration > 0 {
		var c context.CancelFunc
		ctx, c = context.WithTimeout(ctx, cfg.duration)
		defer c()
	}

	var sk sink
	var err error
	if cfg.dryRun != "" {
		sk, err = newFileSink(cfg)
	} else {
		sk, err = newKafkaSink(ctx, cfg)
	}
	if err != nil {
		log.Fatalf("sink: %v", err)
	}
	defer sk.close()

	f := newFleet(cfg.scale)
	st.startedAt = time.Now()
	st.rate.Store(cfg.rate)
	st.incidentSvc.Store("")
	if *incOn != "" {
		st.incidentSvc.Store(*incOn)
		st.incidentFrom.Store(time.Now().UnixMilli())
	}
	st.running.Store(cfg.autostart)
	serveAPI(cfg, f)
	go epsLoop(ctx)
	go metricLoop(ctx, cfg, f, sk)
	var wg sync.WaitGroup
	for i := 0; i < cfg.workers; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); logWorker(ctx, i, cfg, f, sk) }(i)
	}
	log.Printf("simulator running: %d events/s, %d workers, brokers=%s dry-run=%q", cfg.rate, cfg.workers, cfg.brokers, cfg.dryRun)
	wg.Wait()
	log.Printf("done: %d logs, %d metrics, %s", st.sentLogs.Load(), st.sentMetrics.Load(), strings.TrimSpace(fmt.Sprintf("%d errors", st.errors.Load())))
}
