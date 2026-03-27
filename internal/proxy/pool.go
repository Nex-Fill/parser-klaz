package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

type ProxyState struct {
	URL         string
	Alive       bool
	Latency     time.Duration
	SuccessRate float64
	TotalReqs   int64
	FailedReqs  int64
	LastUsed    time.Time
	LastCheck   time.Time
}

type Pool struct {
	mu       sync.RWMutex
	proxies  []*ProxyState
	alive    []*ProxyState
	idx      atomic.Uint64
	fileURL  string
	filePath string

	updateInterval  time.Duration
	healthInterval  time.Duration
	minPoolSize     int
	cancel          context.CancelFunc
	lastReloadAt    time.Time
	lastReloadCount int
}

func (p *Pool) SetSourceURL(url string) {
	p.mu.Lock()
	p.fileURL = url
	p.mu.Unlock()
}

func (p *Pool) SetUpdateInterval(d time.Duration) {
	p.mu.Lock()
	p.updateInterval = d
	p.mu.Unlock()
}

func NewPool(fileURL, filePath string, updateInterval, healthInterval time.Duration, minPool int) *Pool {
	return &Pool{
		fileURL:        fileURL,
		filePath:       filePath,
		updateInterval: updateInterval,
		healthInterval: healthInterval,
		minPoolSize:    minPool,
	}
}

func (p *Pool) Start(ctx context.Context) error {
	ctx, p.cancel = context.WithCancel(ctx)

	if err := p.Reload(ctx); err != nil {
		log.Warn().Err(err).Msg("initial proxy load failed, will retry")
	}

	go p.updateLoop(ctx)
	go p.healthLoop(ctx)

	return nil
}

func (p *Pool) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
}

func (p *Pool) Get() string {
	p.mu.RLock()
	alive := p.alive
	p.mu.RUnlock()

	if len(alive) == 0 {
		return ""
	}
	idx := p.idx.Add(1) % uint64(len(alive))
	return alive[idx].URL
}

func (p *Pool) GetRandom() string {
	p.mu.RLock()
	alive := p.alive
	p.mu.RUnlock()

	if len(alive) == 0 {
		return ""
	}
	return alive[rand.Intn(len(alive))].URL
}

func (p *Pool) GetWeighted() string {
	p.mu.RLock()
	alive := p.alive
	p.mu.RUnlock()

	if len(alive) == 0 {
		return ""
	}

	var best *ProxyState
	bestScore := -1.0

	candidates := alive
	if len(candidates) > 10 {
		candidates = make([]*ProxyState, 10)
		for i := range candidates {
			candidates[i] = alive[rand.Intn(len(alive))]
		}
	}

	for _, proxy := range candidates {
		score := atomic.LoadInt64(&proxy.TotalReqs)
		failed := atomic.LoadInt64(&proxy.FailedReqs)
		successRate := 1.0
		if score > 0 {
			successRate = float64(score-failed) / float64(score)
		}
		s := successRate * 100
		if proxy.Latency > 0 {
			s -= float64(proxy.Latency.Milliseconds()) / 10
		}
		if s > bestScore {
			bestScore = s
			best = proxy
		}
	}

	if best != nil {
		return best.URL
	}
	return alive[0].URL
}

func (p *Pool) ReportSuccess(proxyURL string, latency time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, proxy := range p.proxies {
		if proxy.URL == proxyURL {
			atomic.AddInt64(&proxy.TotalReqs, 1)
			proxy.Latency = (proxy.Latency + latency) / 2
			proxy.LastUsed = time.Now()
			total := float64(atomic.LoadInt64(&proxy.TotalReqs))
			failed := float64(atomic.LoadInt64(&proxy.FailedReqs))
			if total > 0 {
				proxy.SuccessRate = (total - failed) / total
			}
			return
		}
	}
}

func (p *Pool) ReportFailure(proxyURL string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, proxy := range p.proxies {
		if proxy.URL == proxyURL {
			atomic.AddInt64(&proxy.TotalReqs, 1)
			atomic.AddInt64(&proxy.FailedReqs, 1)
			proxy.LastUsed = time.Now()
			total := float64(atomic.LoadInt64(&proxy.TotalReqs))
			failed := float64(atomic.LoadInt64(&proxy.FailedReqs))
			if total > 0 {
				proxy.SuccessRate = (total - failed) / total
			}
			if proxy.SuccessRate < 0.2 && total > 5 {
				proxy.Alive = false
				p.rebuildAlive()
			}
			return
		}
	}
}

func (p *Pool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.alive)
}

func (p *Pool) Stats() []ProxyState {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := make([]ProxyState, len(p.proxies))
	for i, proxy := range p.proxies {
		stats[i] = *proxy
	}
	return stats
}

func (p *Pool) Reload(ctx context.Context) error {
	var rawProxies []string
	var err error

	if p.filePath != "" {
		rawProxies, err = loadFromFile(p.filePath)
	} else if p.fileURL != "" {
		rawProxies, err = loadFromURL(ctx, p.fileURL)
	}

	if err != nil {
		return err
	}

	if len(rawProxies) == 0 {
		return fmt.Errorf("no proxies loaded")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	existing := make(map[string]*ProxyState)
	for _, proxy := range p.proxies {
		existing[proxy.URL] = proxy
	}

	var newProxies []*ProxyState
	seen := make(map[string]bool)
	for _, raw := range rawProxies {
		normalized := normalize(raw)
		if normalized == "" || seen[normalized] {
			continue
		}
		seen[normalized] = true

		if ex, ok := existing[normalized]; ok {
			newProxies = append(newProxies, ex)
		} else {
			newProxies = append(newProxies, &ProxyState{
				URL:         normalized,
				Alive:       true,
				SuccessRate: 1.0,
			})
		}
	}

	p.proxies = newProxies
	p.rebuildAlive()
	p.lastReloadAt = time.Now()
	p.lastReloadCount = len(newProxies)

	log.Info().Int("total", len(p.proxies)).Int("alive", len(p.alive)).Msg("proxies loaded")
	return nil
}

func (p *Pool) rebuildAlive() {
	p.alive = p.alive[:0]
	for _, proxy := range p.proxies {
		if proxy.Alive {
			p.alive = append(p.alive, proxy)
		}
	}
}

func (p *Pool) updateLoop(ctx context.Context) {
	ticker := time.NewTicker(p.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.Reload(ctx); err != nil {
				log.Warn().Err(err).Msg("proxy reload failed")
			}
		}
	}
}

func (p *Pool) healthLoop(ctx context.Context) {
	ticker := time.NewTicker(p.healthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.checkHealth(ctx)
		}
	}
}

func (p *Pool) checkHealth(ctx context.Context) {
	p.mu.RLock()
	proxies := make([]*ProxyState, len(p.proxies))
	copy(proxies, p.proxies)
	p.mu.RUnlock()

	sem := make(chan struct{}, 20)
	var wg sync.WaitGroup

	for _, proxy := range proxies {
		if time.Since(proxy.LastCheck) < p.healthInterval/2 {
			continue
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(ps *ProxyState) {
			defer wg.Done()
			defer func() { <-sem }()

			alive, latency := checkProxy(ctx, ps.URL)
			ps.Alive = alive
			ps.LastCheck = time.Now()
			if alive {
				ps.Latency = latency
			}
		}(proxy)
	}

	wg.Wait()

	p.mu.Lock()
	p.rebuildAlive()
	p.mu.Unlock()
}

func checkProxy(ctx context.Context, proxyURL string) (bool, time.Duration) {
	parsedURL, err := url.Parse(proxyURL)
	if err != nil {
		return false, 0
	}

	client := &http.Client{
		Transport: &http.Transport{Proxy: http.ProxyURL(parsedURL)},
		Timeout:   5 * time.Second,
	}

	start := time.Now()
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://api.kleinanzeigen.de/api/ads.json?size=1", nil)
	resp, err := client.Do(req)
	if err != nil {
		return false, 0
	}
	resp.Body.Close()

	return resp.StatusCode < 500, time.Since(start)
}

func normalize(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.HasPrefix(raw, "#") {
		return ""
	}

	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") ||
		strings.HasPrefix(raw, "socks5://") || strings.HasPrefix(raw, "socks4://") {
		return raw
	}

	parts := strings.Split(raw, ":")
	switch len(parts) {
	case 4:
		return fmt.Sprintf("http://%s:%s@%s:%s", parts[2], parts[3], parts[0], parts[1])
	case 2:
		return fmt.Sprintf("http://%s", raw)
	default:
		return ""
	}
}

func loadFromFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readLines(f)
}

func loadFromURL(ctx context.Context, rawURL string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("proxy url returned %d", resp.StatusCode)
	}

	return readLines(resp.Body)
}

func readLines(r io.Reader) ([]string, error) {
	var lines []string
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines, scanner.Err()
}
