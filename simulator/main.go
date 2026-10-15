// Command simulator streams telemetry from a fleet of simulated satellites
// into Kafka. Each asset runs in its own goroutine on its own ticker, so the
// load profile looks like many independent, always-on devices rather than one
// batch job.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type config struct {
	brokers    []string
	topic      string
	assetCount int
	interval   time.Duration
	faultRate  float64 // per-asset, per-tick probability of starting a fault
	timeScale  float64 // simulated seconds per real second
}

func loadConfig() config {
	return config{
		brokers:    strings.Split(env("KAFKA_BROKERS", "localhost:19092"), ","),
		topic:      env("KAFKA_TOPIC", "telemetry"),
		assetCount: envInt("ASSET_COUNT", 500),
		interval:   time.Duration(envInt("INTERVAL_MS", 1000)) * time.Millisecond,
		faultRate:  envFloat("FAULT_RATE", 0.0005),
		timeScale:  envFloat("TIME_SCALE", 20),
	}
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.brokers...),
		kgo.DefaultProduceTopic(cfg.topic),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerLinger(20*time.Millisecond),
		// Consumers must have the matching codec installed (aiokafka[snappy] in ingest).
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		log.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var sent, failed atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < cfg.assetCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runAsset(ctx, client, cfg, i, start, &sent, &failed, log)
		}(i)
	}

	log.Info("simulator started", "assets", cfg.assetCount, "interval", cfg.interval.String(), "brokers", cfg.brokers)
	go reportThroughput(ctx, &sent, &failed, log)

	wg.Wait()
	if err := client.Flush(context.Background()); err != nil {
		log.Error("flush", "err", err)
	}
	log.Info("simulator stopped", "sent", sent.Load(), "failed", failed.Load())
}

func runAsset(ctx context.Context, client *kgo.Client, cfg config, i int, start time.Time,
	sent, failed *atomic.Int64, log *slog.Logger) {
	rng := rand.New(rand.NewSource(int64(i) + time.Now().UnixNano()))
	asset := NewAsset(i, rng)

	// Stagger start so 10k assets don't all fire on the same millisecond.
	select {
	case <-time.After(time.Duration(rng.Int63n(int64(cfg.interval)))):
	case <-ctx.Done():
		return
	}

	ticker := time.NewTicker(cfg.interval)
	defer ticker.Stop()
	simDt := time.Duration(float64(cfg.interval) * cfg.timeScale)

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if asset.Fault() == FaultNone && rng.Float64() < cfg.faultRate {
				f := Fault(1 + rng.Intn(3))
				asset.InjectFault(f, 30+rng.Intn(60))
				log.Info("fault injected", "asset", asset.ID, "fault", f.String())
			}

			simSeconds := now.Sub(start).Seconds() * cfg.timeScale
			t, ok := asset.Step(now, simDt, simSeconds, rng)
			if !ok {
				continue
			}
			payload, err := json.Marshal(t)
			if err != nil {
				failed.Add(1)
				continue
			}
			// Keying by asset ID keeps each asset's messages ordered within a partition.
			client.Produce(ctx, &kgo.Record{Key: []byte(t.AssetID), Value: payload},
				func(_ *kgo.Record, err error) {
					if err != nil {
						failed.Add(1)
						return
					}
					sent.Add(1)
				})
		}
	}
}

func reportThroughput(ctx context.Context, sent, failed *atomic.Int64, log *slog.Logger) {
	const every = 10 * time.Second
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	var last int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cur := sent.Load()
			log.Info("throughput", "msgs_per_sec", float64(cur-last)/every.Seconds(), "sent_total", cur, "failed_total", failed.Load())
			last = cur
		}
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil {
		return v
	}
	return def
}
