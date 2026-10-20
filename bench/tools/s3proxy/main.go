// s3proxy: a counting proxy for benchmarking against S3.
//
// Default mode is a TCP proxy in front of a local S3-compatible store (RustFS). It delays the first response byte
// of every HTTP request by a configurable time-to-first-byte (modelling S3 request latency), optionally caps each
// response's throughput (S3 streams one request at roughly 50–90 MiB/s), and counts requests and response bytes.
//
//	s3proxy -listen :59100 -upstream localhost:59000 -admin :59101 -ttfb 20ms -mibps 50
//	curl localhost:59101/stats            -> {"requests":N,"bytes":B,"ttfb_ms":20,"mibps":50}
//	curl -X POST 'localhost:59101/ttfb?ms=0'
//	curl -X POST 'localhost:59101/mibps?v=0'   # 0: no cap
//
// -listen-uncounted adds a second listener with the same latency and throughput cap whose requests aren't
// counted: readers that run alongside a measured phase (probes while a system ingests) use it, so the counts stay
// the phase's own.
//
// With -aws-bucket it proxies to real S3 or S3 Express One Zone instead (aws.go), and /stats also reports
// requests by kind.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	requests atomic.Int64
	bytesOut atomic.Int64
	ttfbMs   atomic.Int64
	mibps    atomic.Int64
)

// wait pays the time to first byte; tests replace it to count the waits.
var wait = time.Sleep

func main() {
	listen := flag.String("listen", ":59100", "proxy listen address")
	uncounted := flag.String("listen-uncounted", "", "listen address whose requests aren't counted (optional)")
	upstream := flag.String("upstream", "localhost:59000", "S3 endpoint")
	admin := flag.String("admin", ":59101", "stats/admin listen address")
	ttfb := flag.Duration("ttfb", 20*time.Millisecond, "time to first byte added per request")
	rate := flag.Int64("mibps", 0, "throughput cap per response in MiB/s (0: none)")
	awsBucket := flag.String("aws-bucket", "", "real S3 or S3 Express bucket (AWS mode)")
	awsRegion := flag.String("aws-region", "us-east-2", "region of -aws-bucket")
	bucket := flag.String("bucket", "bench", "bucket name clients use (AWS mode)")
	flag.Parse()
	ttfbMs.Store(ttfb.Milliseconds())
	mibps.Store(*rate)

	go serveAdmin(*admin)
	if *awsBucket != "" {
		serveAWS(*listen, *uncounted, *bucket, *awsBucket, *awsRegion)
		return
	}
	log.Printf("proxy %s -> %s, ttfb %v, %d MiB/s per response, admin %s", *listen, *upstream, *ttfb, *rate, *admin)
	if *uncounted != "" {
		log.Printf("uncounted proxy %s -> %s", *uncounted, *upstream)
		go serveTCP(*uncounted, *upstream, false)
	}
	serveTCP(*listen, *upstream, true)
}

func serveTCP(listen, upstream string, counted bool) {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(proxyTCP(ln, upstream, counted))
}

// proxyTCP proxies every connection ln accepts to upstream, until ln fails.
func proxyTCP(ln net.Listener, upstream string, counted bool) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go handle(c, upstream, counted)
	}
}

// finalResponse reports whether a response that starts with b is a request's final one: interim responses
// (1xx, such as the 100 Continue a PUT with "Expect: 100-continue" gets before it sends its body) are skipped, so
// the request is counted and pays the time to first byte once.
func finalResponse(b []byte) bool {
	for bytes.HasPrefix(b, []byte("HTTP/1.")) && len(b) > 9 && b[9] == '1' {
		end := bytes.Index(b, []byte("\r\n\r\n"))
		if end < 0 {
			return false
		}
		b = b[end+4:]
	}
	return len(b) > 0
}

func handle(client net.Conn, upstream string, counted bool) {
	defer client.Close()
	server, err := net.Dial("tcp", upstream)
	if err != nil {
		log.Print(err)
		return
	}
	defer server.Close()

	// awaiting is set when the client sends a new request and cleared when its response starts.
	var awaiting atomic.Bool
	done := make(chan struct{}, 2)

	go func() { // client -> server
		buf := make([]byte, 64<<10)
		for {
			n, err := client.Read(buf)
			if n > 0 {
				if awaiting.CompareAndSwap(false, true) && counted {
					requests.Add(1)
				}
				if _, werr := server.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		done <- struct{}{}
	}()
	go func() { // server -> client
		buf := make([]byte, 64<<10)
		var started time.Time // when the current response's first byte went out
		var sent int64        // its bytes so far
		for {
			n, err := server.Read(buf)
			if n > 0 {
				if awaiting.Load() && finalResponse(buf[:n]) && awaiting.CompareAndSwap(true, false) {
					if d := ttfbMs.Load(); d > 0 {
						wait(time.Duration(d) * time.Millisecond)
					}
					started, sent = time.Now(), 0
				}
				if counted {
					bytesOut.Add(int64(n))
				}
				if _, werr := client.Write(buf[:n]); werr != nil {
					break
				}
				sent += int64(n)
				if r := mibps.Load(); r > 0 {
					due := started.Add(time.Duration(float64(sent) / float64(r<<20) * float64(time.Second)))
					time.Sleep(time.Until(due))
				}
			}
			if err != nil {
				break
			}
		}
		done <- struct{}{}
	}()
	<-done
}

func serveAdmin(addr string) {
	http.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"requests": requests.Load(), "bytes": bytesOut.Load(), "ttfb_ms": ttfbMs.Load(), "mibps": mibps.Load(),
			"kinds": kindCounts(),
		})
	})
	http.HandleFunc("/ttfb", func(w http.ResponseWriter, r *http.Request) {
		ms, err := strconv.ParseInt(r.URL.Query().Get("ms"), 10, 64)
		if err != nil {
			http.Error(w, "ms required", 400)
			return
		}
		ttfbMs.Store(ms)
	})
	http.HandleFunc("/mibps", func(w http.ResponseWriter, r *http.Request) {
		v, err := strconv.ParseInt(r.URL.Query().Get("v"), 10, 64)
		if err != nil || v < 0 {
			http.Error(w, "v (MiB/s, 0 for none) required", 400)
			return
		}
		mibps.Store(v)
	})
	log.Fatal(http.ListenAndServe(addr, nil))
}
