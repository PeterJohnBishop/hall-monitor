package monitor

import (
	"bytes"
	"context"
	"fmt"
	"log"
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
	ReqBody  []byte
	RespBody []byte
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

func ChromiumMonitor(db *DBStore) {
	fmt.Println("Starting Unified CDP Browser Monitor (SQLite Enabled)...")
	fmt.Println("Press Ctrl+C to stop.")

	allocatorCtx, cancel := chromedp.NewRemoteAllocator(context.Background(), "ws://127.0.0.1:9222/")
	defer cancel()

	ctx, cancel := chromedp.NewContext(allocatorCtx)
	defer cancel()

	var tracker sync.Map

	chromedp.ListenTarget(ctx, func(ev interface{}) {
		switch e := ev.(type) {

		case *network.EventRequestWillBeSent:
			call := &HTTPCall{
				URL:    e.Request.URL,
				Method: e.Request.Method,
			}

			if e.Request.HasPostData {
				var bodyBuilder bytes.Buffer
				for _, entry := range e.Request.PostDataEntries {
					bodyBuilder.WriteString(entry.Bytes)
				}
				call.ReqBody = bodyBuilder.Bytes()
			}

			tracker.Store(e.RequestID, call)
			fmt.Printf("\033[36m[NET OUT]\033[0m %-4s %s\n", call.Method, truncate(call.URL, 80))

		case *network.EventResponseReceived:
			if val, ok := tracker.Load(e.RequestID); ok {
				if call, isHTTP := val.(*HTTPCall); isHTTP {
					call.Status = e.Response.Status
					tracker.Store(e.RequestID, call)
				}
			}

		case *network.EventLoadingFinished:
			if val, ok := tracker.Load(e.RequestID); ok {
				if call, isHTTP := val.(*HTTPCall); isHTTP {
					go func(reqID network.RequestID, completedCall *HTTPCall) {
						var bodyBytes []byte
						err := chromedp.Run(ctx, chromedp.ActionFunc(func(c context.Context) error {
							var err error
							bodyBytes, err = network.GetResponseBody(reqID).Do(c)
							return err
						}))

						if err == nil {
							completedCall.RespBody = bodyBytes
						} else {
							completedCall.RespBody = []byte(fmt.Sprintf("[Error fetching body: %v]", err))
						}

						db.Add(completedCall)
						tracker.Delete(reqID)

						fmt.Printf("\033[32m[NET IN ]\033[0m %-4s %s (Status: %d)\n", completedCall.Method, truncate(completedCall.URL, 80), completedCall.Status)
					}(e.RequestID, call)
				}
			}

		case *network.EventWebSocketCreated:
			tracker.Store(e.RequestID, e.URL)
			fmt.Printf("\033[33m[WS CREATED]\033[0m URL: %s\n", e.URL)

		case *network.EventWebSocketFrameSent:
			url := "unknown-ws-url"
			if val, ok := tracker.Load(e.RequestID); ok {
				if u, isStr := val.(string); isStr {
					url = u
				}
			}

			call := &HTTPCall{
				URL:     url,
				Method:  "WS_OUT",
				Status:  101, //standard HTTP code for WS
				ReqBody: []byte(e.Response.PayloadData),
			}
			db.Add(call)

			fmt.Printf("\033[33m[WS OUT]\033[0m %s\n", truncate(e.Response.PayloadData, 80))

		case *network.EventWebSocketFrameReceived:
			url := "unknown-ws-url"
			if val, ok := tracker.Load(e.RequestID); ok {
				if u, isStr := val.(string); isStr {
					url = u
				}
			}

			call := &HTTPCall{
				URL:      url,
				Method:   "WS_IN",
				Status:   101,
				RespBody: []byte(e.Response.PayloadData),
			}
			db.Add(call)

			fmt.Printf("\033[33m[WS IN]\033[0m %s\n", truncate(e.Response.PayloadData, 80))

		case *network.EventWebSocketClosed:
			tracker.Delete(e.RequestID)
			fmt.Printf("\033[33m[WS CLOSED]\033[0m\n")
		}
	})

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

		var heapUsed float64
		for _, m := range metrics {
			if m.Name == "JSHeapUsedSize" {
				heapUsed = m.Value
			}
		}

		fmt.Printf("\033[35m[SYS]\033[0m Heap: %-8s\n", formatBytes(uint64(heapUsed)))
	}
}
