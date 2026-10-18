// s3proxy: a counting proxy for benchmarking against S3.
//
// Default mode is a TCP proxy in front of a local S3-compatible store (RustFS). It delays the first response byte of every HTTP request by a configurable time-to-first-byte
// (modelling S3 request latency), optionally caps each response's throughput (S3 streams one request at roughly
// 50–90 MiB/s), and counts requests and response bytes.
//
//	s3proxy -listen :59100 -upstream localhost:59000 -admin :59101 -ttfb 20ms -mibps 50
//	curl localhost:59101/stats            -> {"requests":N,"bytes":B,"ttfb_ms":20,"mibps":50}
//	curl -X POST 'localhost:59101/ttfb?ms=0'
//	curl -X POST 'localhost:59101/mibps?v=0'   # 0: no cap
//
// With -aws-bucket it proxies to real S3 or S3 Express One Zone instead (aws.go), and /stats also reports
// requests by kind.
package main

import (
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

func main() {
	listen := flag.String("listen", ":59100", "proxy listen address")
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
		serveAWS(*listen, *bucket, *awsBucket, *awsRegion)
		return
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("proxy %s -> %s, ttfb %v, %d MiB/s per response, admin %s", *listen, *upstream, *ttfb, *rate, *admin)
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go handle(c, *upstream)
	}
}

func handle(client net.Conn, upstream string) {
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
				if awaiting.CompareAndSwap(false, true) {
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
				if awaiting.CompareAndSwap(true, false) {
					if d := ttfbMs.Load(); d > 0 {
						time.Sleep(time.Duration(d) * time.Millisecond)
					}
					started, sent = time.Now(), 0
				}
				bytesOut.Add(int64(n))
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
