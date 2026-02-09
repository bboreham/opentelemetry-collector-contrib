// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spanpruningprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/spanpruningprocessor"

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/spanpruningprocessor/internal/metadata"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	ottrace "go.opentelemetry.io/otel/trace"
)

// Defaults for BatchSpanProcessorOptions.
const (
	DefaultMaxQueueSize = 2048
	// DefaultScheduleDelay is the delay interval between two consecutive exports, in milliseconds.
	DefaultScheduleDelay = 5000
	// DefaultExportTimeout is the duration after which an export is cancelled, in milliseconds.
	DefaultExportTimeout      = 30000
	DefaultMaxExportBatchSize = 512
)

// BatchSpanProcessorOption configures a BatchSpanProcessor.
type BatchSpanProcessorOption func(o *BatchSpanProcessorOptions)

// BatchSpanProcessorOptions is configuration settings for a
// BatchSpanProcessor.
type BatchSpanProcessorOptions struct {
	// MaxQueueSize is the maximum queue size to buffer spans for delayed processing. If the
	// queue gets full it drops the spans. Use BlockOnQueueFull to change this behavior.
	// The default value of MaxQueueSize is 2048.
	MaxQueueSize int

	// BatchTimeout is the maximum duration for constructing a batch. Processor
	// forcefully sends available spans when timeout is reached.
	// The default value of BatchTimeout is 5000 msec.
	BatchTimeout time.Duration

	// ExportTimeout specifies the maximum duration for exporting spans. If the timeout
	// is reached, the export will be cancelled.
	// The default value of ExportTimeout is 30000 msec.
	ExportTimeout time.Duration

	// MaxExportBatchSize is the maximum number of spans to process in a single batch.
	// If there are more than one batch worth of spans then it processes multiple batches
	// of spans one batch after the other without any delay.
	// The default value of MaxExportBatchSize is 512.
	MaxExportBatchSize int

	// BlockOnQueueFull blocks onEnd() and onStart() method if the queue is full
	// AND if BlockOnQueueFull is set to true.
	// Blocking option should be used carefully as it can severely affect the performance of an
	// application.
	BlockOnQueueFull bool
}

// batchSpanProcessor is a SpanProcessor that batches asynchronously-received
// spans and sends them to a trace.Exporter when complete.
type batchSpanProcessor struct {
	e trace.SpanExporter
	s *spanPruningProcessor
	o BatchSpanProcessorOptions

	queue   chan trace.ReadOnlySpan
	dropped uint32

	batch      []trace.ReadOnlySpan
	batchMutex sync.Mutex
	timer      *time.Timer
	stopWait   sync.WaitGroup
	stopOnce   sync.Once
	stopCh     chan struct{}
	stopped    atomic.Bool
}

var _ trace.SpanProcessor = (*batchSpanProcessor)(nil)

func NewBatchSpanProcessor(exporter trace.SpanExporter, groupByAttributes []string, options ...BatchSpanProcessorOption) trace.SpanProcessor {
	maxQueueSize := DefaultMaxQueueSize
	maxExportBatchSize := DefaultMaxExportBatchSize

	if maxExportBatchSize > maxQueueSize {
		maxExportBatchSize = min(DefaultMaxExportBatchSize, maxQueueSize)
	}

	o := BatchSpanProcessorOptions{
		BatchTimeout:       time.Duration(DefaultScheduleDelay) * time.Millisecond,
		ExportTimeout:      time.Duration(DefaultExportTimeout) * time.Millisecond,
		MaxQueueSize:       maxQueueSize,
		MaxExportBatchSize: maxExportBatchSize,
	}
	for _, opt := range options {
		opt(&o)
	}

	cfg := &Config{
		MaxParentDepth:             -1,
		GroupByAttributes:          groupByAttributes,
		MinSpansToAggregate:        2,
		AggregationAttributePrefix: "agg.",
	}

	set := processortest.NewNopSettings(metadata.Type)

	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		panic(err)
	}

	s, err := newSpanPruningProcessor(set, cfg, telemetryBuilder)
	if err != nil {
		panic(err)
	}

	bsp := &batchSpanProcessor{
		e:      exporter,
		s:      s,
		o:      o,
		batch:  make([]trace.ReadOnlySpan, 0, o.MaxExportBatchSize),
		timer:  time.NewTimer(o.BatchTimeout),
		queue:  make(chan trace.ReadOnlySpan, o.MaxQueueSize),
		stopCh: make(chan struct{}),
	}

	bsp.stopWait.Add(1)
	go func() {
		defer bsp.stopWait.Done()
		bsp.processQueue()
		bsp.drainQueue()
	}()

	return bsp
}

// OnStart method does nothing.
func (*batchSpanProcessor) OnStart(context.Context, trace.ReadWriteSpan) {}

// OnEnd method enqueues a ReadOnlySpan for later processing.
func (bsp *batchSpanProcessor) OnEnd(s trace.ReadOnlySpan) {
	// Do not enqueue spans after Shutdown.
	if bsp.stopped.Load() {
		return
	}

	// Do not enqueue spans if we are just going to drop them.
	if bsp.e == nil {
		return
	}
	bsp.enqueue(s)
}

// Shutdown flushes the queue and waits until all spans are processed.
// It only executes once. Subsequent call does nothing.
func (bsp *batchSpanProcessor) Shutdown(ctx context.Context) error {
	var err error
	bsp.stopOnce.Do(func() {
		bsp.stopped.Store(true)
		wait := make(chan struct{})
		go func() {
			close(bsp.stopCh)
			bsp.stopWait.Wait()
			if bsp.e != nil {
				if err := bsp.e.Shutdown(ctx); err != nil {
					otel.Handle(err)
				}
			}
			close(wait)
		}()
		// Wait until the wait group is done or the context is cancelled
		select {
		case <-wait:
		case <-ctx.Done():
			err = ctx.Err()
		}
	})
	return err
}

type forceFlushSpan struct {
	trace.ReadOnlySpan
	flushed chan struct{}
}

func (forceFlushSpan) SpanContext() ottrace.SpanContext {
	return ottrace.NewSpanContext(ottrace.SpanContextConfig{TraceFlags: ottrace.FlagsSampled})
}

// ForceFlush exports all ended spans that have not yet been exported.
func (bsp *batchSpanProcessor) ForceFlush(ctx context.Context) error {
	// Interrupt if context is already canceled.
	if err := ctx.Err(); err != nil {
		return err
	}

	// Do nothing after Shutdown.
	if bsp.stopped.Load() {
		return nil
	}

	var err error
	if bsp.e != nil {
		flushCh := make(chan struct{})
		if bsp.enqueueBlockOnQueueFull(ctx, forceFlushSpan{flushed: flushCh}) {
			select {
			case <-bsp.stopCh:
				// The batchSpanProcessor is Shutdown.
				return nil
			case <-flushCh:
				// Processed any items in queue prior to ForceFlush being called
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		wait := make(chan error, 1)
		go func() {
			wait <- bsp.exportSpans(ctx)
		}()
		// Wait until the export is finished or the context is cancelled/timed out
		select {
		case err = <-wait:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	return err
}

// WithMaxQueueSize returns a BatchSpanProcessorOption that configures the
// maximum queue size allowed for a BatchSpanProcessor.
func WithMaxQueueSize(size int) BatchSpanProcessorOption {
	return func(o *BatchSpanProcessorOptions) {
		o.MaxQueueSize = size
	}
}

// exportSpans is a subroutine of processing and draining the queue.
func (bsp *batchSpanProcessor) exportSpans(ctx context.Context) error {
	bsp.timer.Reset(bsp.o.BatchTimeout)

	bsp.batchMutex.Lock()
	defer bsp.batchMutex.Unlock()

	if bsp.o.ExportTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(ctx, bsp.o.ExportTimeout, errors.New("processor export timeout"))
		defer cancel()
	}

	if l := len(bsp.batch); l > 0 {
		//global.Debug("exporting spans", "count", len(bsp.batch), "total_dropped", atomic.LoadUint32(&bsp.dropped))
		spans, err := bsp.s.processSpans(ctx, bsp.batch)
		if err == nil {
			err = bsp.e.ExportSpans(ctx, spans)
		}

		// A new batch is always created after exporting, even if the batch failed to be exported.
		//
		// It is up to the exporter to implement any type of retry logic if a batch is failing
		// to be exported, since it is specific to the protocol and backend being sent to.
		clear(bsp.batch) // Erase elements to let GC collect objects
		bsp.batch = bsp.batch[:0]

		if err != nil {
			return err
		}
	}
	return nil
}

// processQueue removes spans from the `queue` channel until processor
// is shut down. It calls the exporter in batches of up to MaxExportBatchSize
// waiting up to BatchTimeout to form a batch.
func (bsp *batchSpanProcessor) processQueue() {
	defer bsp.timer.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		select {
		case <-bsp.stopCh:
			return
		case <-bsp.timer.C:
			if err := bsp.exportSpans(ctx); err != nil {
				otel.Handle(err)
			}
		case sd := <-bsp.queue:
			if ffs, ok := sd.(forceFlushSpan); ok {
				close(ffs.flushed)
				continue
			}
			bsp.batchMutex.Lock()
			bsp.batch = append(bsp.batch, sd)
			shouldExport := len(bsp.batch) >= bsp.o.MaxExportBatchSize
			bsp.batchMutex.Unlock()
			if shouldExport {
				if !bsp.timer.Stop() {
					// Handle both GODEBUG=asynctimerchan=[0|1] properly.
					select {
					case <-bsp.timer.C:
					default:
					}
				}
				if err := bsp.exportSpans(ctx); err != nil {
					otel.Handle(err)
				}
			}
		}
	}
}

// drainQueue awaits the any caller that had added to bsp.stopWait
// to finish the enqueue, then exports the final batch.
func (bsp *batchSpanProcessor) drainQueue() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		select {
		case sd := <-bsp.queue:
			if _, ok := sd.(forceFlushSpan); ok {
				// Ignore flush requests as they are not valid spans.
				continue
			}

			bsp.batchMutex.Lock()
			bsp.batch = append(bsp.batch, sd)
			shouldExport := len(bsp.batch) == bsp.o.MaxExportBatchSize
			bsp.batchMutex.Unlock()

			if shouldExport {
				if err := bsp.exportSpans(ctx); err != nil {
					otel.Handle(err)
				}
			}
		default:
			// There are no more enqueued spans. Make final export.
			if err := bsp.exportSpans(ctx); err != nil {
				otel.Handle(err)
			}
			return
		}
	}
}

func (bsp *batchSpanProcessor) enqueue(sd trace.ReadOnlySpan) {
	ctx := context.TODO()
	if bsp.o.BlockOnQueueFull {
		bsp.enqueueBlockOnQueueFull(ctx, sd)
	} else {
		bsp.enqueueDrop(ctx, sd)
	}
}

func (bsp *batchSpanProcessor) enqueueBlockOnQueueFull(ctx context.Context, sd trace.ReadOnlySpan) bool {
	if !sd.SpanContext().IsSampled() {
		return false
	}

	select {
	case bsp.queue <- sd:
		return true
	case <-ctx.Done():
		return false
	}
}

func (bsp *batchSpanProcessor) enqueueDrop(ctx context.Context, sd trace.ReadOnlySpan) bool {
	if !sd.SpanContext().IsSampled() {
		return false
	}

	select {
	case bsp.queue <- sd:
		return true
	default:
		atomic.AddUint32(&bsp.dropped, 1)
	}
	return false
}

// MarshalLog is the marshaling function used by the logging system to represent this Span Processor.
func (bsp *batchSpanProcessor) MarshalLog() any {
	return struct {
		Type         string
		SpanExporter trace.SpanExporter
		Config       BatchSpanProcessorOptions
	}{
		Type:         "BatchSpanProcessor",
		SpanExporter: bsp.e,
		Config:       bsp.o,
	}
}

type spanWrapper struct {
	span               ptrace.Span
	scopeSpans         ptrace.ScopeSpans
	trace.ReadOnlySpan // Only here to stop complaining about the private member.
}

func (s spanWrapper) Attributes() []attribute.KeyValue {
	return pcommonMapToKeyValues(s.span.Attributes())
}

func (s spanWrapper) Events() []trace.Event {
	ret := []trace.Event{}
	for _, e := range s.span.Events().All() {
		ret = append(ret, trace.Event{
			Name:       e.Name(),
			Attributes: pcommonMapToKeyValues(e.Attributes()),
			Time:       e.Timestamp().AsTime(),
		})
	}
	return ret
}
func (s spanWrapper) Name() string { return s.span.Name() }
func (s spanWrapper) SpanContext() ottrace.SpanContext {
	return ottrace.NewSpanContext(ottrace.SpanContextConfig{
		TraceID:    ottrace.TraceID(s.span.TraceID()),
		SpanID:     ottrace.SpanID(s.span.SpanID()),
		TraceFlags: ottrace.TraceFlags(s.span.Flags()),
	})
}
func (s spanWrapper) Parent() ottrace.SpanContext {
	return ottrace.NewSpanContext(ottrace.SpanContextConfig{
		TraceID:    ottrace.TraceID(s.span.TraceID()),
		SpanID:     ottrace.SpanID(s.span.ParentSpanID()),
		TraceFlags: ottrace.TraceFlags(s.span.Flags()),
	})
}
func (s spanWrapper) SpanKind() ottrace.SpanKind { return ottrace.SpanKind(s.span.Kind()) }
func (s spanWrapper) Status() trace.Status {
	return trace.Status{Code: codes.Code(s.span.Status().Code())}
}
func (s spanWrapper) Links() []trace.Link          { return nil } // FIXME
func (s spanWrapper) ChildSpanCount() int          { return 0 }   // FIXME
func (s spanWrapper) DroppedAttributes() int       { return 0 }
func (s spanWrapper) DroppedLinks() int            { return 0 }
func (s spanWrapper) DroppedEvents() int           { return 0 }
func (s spanWrapper) StartTime() time.Time         { return s.span.StartTimestamp().AsTime() }
func (s spanWrapper) EndTime() time.Time           { return s.span.EndTimestamp().AsTime() }
func (s spanWrapper) Resource() *resource.Resource { return nil } //FIXME
func (s spanWrapper) InstrumentationScope() instrumentation.Scope {
	return instrumentation.Scope{
		Name:       s.scopeSpans.Scope().Name(),
		Version:    s.scopeSpans.Scope().Version(),
		Attributes: *attribute.EmptySet(), // TODO
	}
}
func (s spanWrapper) InstrumentationLibrary() instrumentation.Library {
	return s.InstrumentationScope()
}

func pcommonMapToKeyValues(a pcommon.Map) []attribute.KeyValue {
	ret := []attribute.KeyValue{}
	for k, v := range a.All() {
		ret = append(ret, attribute.KeyValue{Key: attribute.Key(k), Value: pcommonValueToAttributeValue(v)})
	}
	return ret

}

func pcommonValueToAttributeValue(v pcommon.Value) attribute.Value {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return attribute.StringValue(v.Str())
	case pcommon.ValueTypeBool:
		return attribute.BoolValue(v.Bool())
	case pcommon.ValueTypeInt:
		return attribute.Int64Value(v.Int())
	case pcommon.ValueTypeDouble:
		return attribute.Float64Value(v.Double())
	case pcommon.ValueTypeSlice:
		var vals []string // Only string slice supported.  TODO: Better error detection.
		for _, s := range v.Slice().All() {
			vals = append(vals, s.Str())
		}
		return attribute.StringSliceValue(vals)
	default:
		return attribute.Value{}
	}
}
