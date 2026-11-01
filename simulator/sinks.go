package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ------------------------------------------------------------------ Kafka
type kafkaSink struct{ cl *kgo.Client }

func newKafkaSink(ctx context.Context, cfg config) (sink, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ProducerLinger(25*time.Millisecond),
		kgo.ProducerBatchMaxBytes(4<<20),
		kgo.MaxBufferedRecords(1_000_000), // Produce blocks beyond this: natural backpressure
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		return nil, err
	}
	adm := kadm.NewClient(cl)
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if _, err := adm.CreateTopics(cctx, int32(cfg.partitions), 1, nil, cfg.logsTopic, cfg.metricsTopic); err != nil {
		return nil, fmt.Errorf("create topics: %w", err)
	}
	return &kafkaSink{cl: cl}, nil
}

func (k *kafkaSink) emit(topic string, payload []byte) {
	k.cl.Produce(context.Background(), &kgo.Record{Topic: topic, Value: payload}, func(_ *kgo.Record, err error) {
		if err != nil {
			st.errors.Add(1)
		}
	})
}

func (k *kafkaSink) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	k.cl.Flush(ctx)
	k.cl.Close()
}

// ------------------------------------------------------------------ file (dry run)
type fileSink struct {
	mu         sync.Mutex
	logs, mets *bufio.Writer
	lf, mf     *os.File
}

func newFileSink(cfg config) (sink, error) {
	lf, err := os.Create(cfg.dryRun + ".logs.ndjson")
	if err != nil {
		return nil, err
	}
	mf, err := os.Create(cfg.dryRun + ".metrics.ndjson")
	if err != nil {
		return nil, err
	}
	return &fileSink{logs: bufio.NewWriterSize(lf, 1<<20), mets: bufio.NewWriterSize(mf, 1<<20), lf: lf, mf: mf}, nil
}

func (f *fileSink) emit(topic string, p []byte) {
	f.mu.Lock()
	w := f.logs
	if strings.HasSuffix(topic, "metrics") {
		w = f.mets
	}
	w.Write(p)
	w.WriteByte('\n')
	f.mu.Unlock()
}

func (f *fileSink) close() {
	f.logs.Flush()
	f.mets.Flush()
	f.lf.Close()
	f.mf.Close()
}
