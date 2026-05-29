package monitor

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/performance"
	"github.com/chromedp/chromedp"
)

type RollingStore struct {
	mu      sync.Mutex
	limit   int
	history []*HTTPCall
}

type HTTPCall struct {
	URL      string
	Method   string
	Status   int64
	ReqBody  string
	RespBody string
}

func NewRollingStore(limit int) *RollingStore {
	return &RollingStore{
		limit:   limit,
		history: make([]*HTTPCall, 0, limit),
	}
}

func (s *RollingStore) Add(call *HTTPCall) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.history = append(s.history, call)

	if len(s.history) > s.limit {
		s.history = s.history[1:]
	}
}

func truncate(s string, max int) string {
	if len(s) > max {
		return s[:max-3] + "..."
	}
	return s
}

func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func ChromiumMonitor() {
	fmt.Println("Starting Unified CDP Browser Monitor (Network + Performance)...")

	// Connect to Chrome on port 9222, create connection context
	allocatorCtx, cancel := chromedp.NewRemoteAllocator(context.Background(), "ws://127.0.0.1:9222/")
	defer cancel()

	// Isolate the context of the connection and commands to this tab
	ctx, cancel := chromedp.NewContext(allocatorCtx)
	defer cancel()

	completedRequests := NewRollingStore(100)
	var tracker sync.Map

	chromedp.ListenTarget(ctx, func(ev interface{}) {
		switch e := ev.(type) {

		case *network.EventRequestWillBeSent:
			call := &HTTPCall{
				URL:    e.Request.URL,
				Method: e.Request.Method,
			}

			if e.Request.HasPostData {
				var bodyBuilder strings.Builder
				for _, entry := range e.Request.PostDataEntries {
					bodyBuilder.WriteString(entry.Bytes)
				}
				call.ReqBody = bodyBuilder.String()
			}

			tracker.Store(e.RequestID, call)

			fmt.Printf("\033[36m[NET OUT]\033[0m %-4s %s\n", call.Method, truncate(call.URL, 80))

		case *network.EventResponseReceived:
			if val, ok := tracker.Load(e.RequestID); ok {
				call := val.(*HTTPCall)
				call.Status = e.Response.Status
				tracker.Store(e.RequestID, call)
			}

		case *network.EventLoadingFinished:
			if val, ok := tracker.Load(e.RequestID); ok {
				call := val.(*HTTPCall)

				// fetch body asynchronously so CDP listener isn't blocked
				go func(reqID network.RequestID, completedCall *HTTPCall) {
					var bodyBytes []byte
					err := chromedp.Run(ctx, chromedp.ActionFunc(func(c context.Context) error {
						var err error
						bodyBytes, err = network.GetResponseBody(reqID).Do(c)
						return err
					}))

					if err == nil {
						completedCall.RespBody = string(bodyBytes)
					} else {
						completedCall.RespBody = fmt.Sprintf("[Error fetching body: %v]", err)
					}

					completedRequests.Add(completedCall)
					tracker.Delete(reqID)

					fmt.Printf("\033[32m[NET IN ]\033[0m %-4s %s (Status: %d)\n", completedCall.Method, truncate(completedCall.URL, 80), completedCall.Status)
				}(e.RequestID, call)
			}

		case *network.EventWebSocketCreated:
			fmt.Printf("\033[33m[WS CREATED]\033[0m URL: %s\n", e.URL)
		case *network.EventWebSocketFrameSent:
			fmt.Printf("\033[33m[WS OUT]\033[0m %s\n", truncate(e.Response.PayloadData, 80))
		case *network.EventWebSocketFrameReceived:
			fmt.Printf("\033[33m[WS IN]\033[0m %s\n", truncate(e.Response.PayloadData, 80))
		case *network.EventWebSocketClosed:
			fmt.Printf("\033[33m[WS CLOSED]\033[0m\n")
		}
	})

	// explicitly enable network domain monitoring on attachment
	err := chromedp.Run(ctx, network.Enable())
	if err != nil {
		log.Fatalf("Failed to attach and enable network on browser: %v", err)
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		var metrics []*performance.Metric

		err := chromedp.Run(ctx, chromedp.ActionFunc(func(c context.Context) error {
			var err error
			_ = performance.Enable().Do(c)

			metrics, err = performance.GetMetrics().Do(c)
			return err
		}))

		if err != nil {
			log.Printf("Error pulling metrics: %v\n", err)
			continue
		}

		var heapUsed, domNodes, layoutCount, layoutDuration, recalcStyleCount, recalcDuration float64

		for _, m := range metrics {
			switch m.Name {
			case "JSHeapUsedSize":
				heapUsed = m.Value
			case "Nodes":
				domNodes = m.Value
			case "LayoutCount":
				layoutCount = m.Value
			case "LayoutDuration":
				layoutDuration = m.Value
			case "RecalcStyleCount":
				recalcStyleCount = m.Value
			case "RecalcStyleDuration":
				recalcDuration = m.Value
			}
		}

		fmt.Printf("\033[35m[SYS]\033[0m Heap: %-8s | DOM: %-5.0f | Layouts: %.0f (%.1fms) | Style Recalcs: %.0f (%.1fms)\n",
			formatBytes(uint64(heapUsed)),
			domNodes,
			layoutCount, layoutDuration*1000,
			recalcStyleCount, recalcDuration*1000,
		)
	}
}
