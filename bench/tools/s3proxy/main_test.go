package main

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFinalResponse(t *testing.T) {
	cases := map[string]bool{
		"HTTP/1.1 200 OK\r\n\r\n":                                    true,
		"HTTP/1.1 404 Not Found\r\n":                                 true,
		"HTTP/1.1 100 Continue\r\n\r\n":                              false,
		"HTTP/1.1 100 Continue\r\n":                                  false,
		"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\n\r\n":       true,
		"HTTP/1.1 103 Early Hints\r\nLink: </a>\r\n\r\nHTTP/1.1 204": true,
		"body bytes of a response already under way":                 true,
	}
	for response, want := range cases {
		if got := finalResponse([]byte(response)); got != want {
			t.Errorf("finalResponse(%q) = %v, want %v", response, got, want)
		}
	}
}

// upstream is an HTTP server that answers PUTs with "Expect: 100-continue" the way S3 does: 100 Continue, then the
// final response once the body is in.
func upstream(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					req, err := http.ReadRequest(r)
					if err != nil {
						return
					}
					if req.Header.Get("Expect") == "100-continue" {
						io.WriteString(c, "HTTP/1.1 100 Continue\r\n\r\n")
					}
					io.Copy(io.Discard, req.Body)
					io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// through starts the TCP proxy in front of an upstream and returns its address.
func through(t *testing.T, to string, counted bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go proxyTCP(ln, to, counted)
	return ln.Addr().String()
}

// waits counts the times to first byte paid while the test runs.
func waits(t *testing.T) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	wait = func(time.Duration) { n.Add(1) }
	ttfbMs.Store(20)
	t.Cleanup(func() {
		wait = time.Sleep
		ttfbMs.Store(0)
	})
	return &n
}

// put sends n PUTs with "Expect: 100-continue" on one connection, each waiting for 100 Continue before its body.
func put(t *testing.T, addr string, n int) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r := bufio.NewReader(c)
	for range n {
		io.WriteString(c, "PUT /bench/key HTTP/1.1\r\nHost: s3\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n")
		line, err := r.ReadString('\n')
		if err != nil || !strings.HasPrefix(line, "HTTP/1.1 100") {
			t.Fatalf("expected 100 Continue, got %q (%v)", line, err)
		}
		if _, err := r.ReadString('\n'); err != nil { // the blank line ending the interim response
			t.Fatal(err)
		}
		io.WriteString(c, "data")
		resp, err := http.ReadResponse(r, nil)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d", resp.StatusCode)
		}
	}
}

func TestAPutWithExpectContinueCountsOnceAndWaitsOnce(t *testing.T) {
	paid := waits(t)
	before := requests.Load()
	put(t, through(t, upstream(t), true), 3)
	if got := requests.Load() - before; got != 3 {
		t.Errorf("counted %d requests, want 3", got)
	}
	if got := paid.Load(); got != 3 {
		t.Errorf("paid the time to first byte %d times, want 3", got)
	}
}

func TestTheUncountedListenerCountsNothing(t *testing.T) {
	paid := waits(t)
	before, sent := requests.Load(), bytesOut.Load()
	put(t, through(t, upstream(t), false), 2)
	if requests.Load() != before || bytesOut.Load() != sent {
		t.Error("the uncounted listener changed the counters")
	}
	if got := paid.Load(); got != 2 {
		t.Errorf("paid the time to first byte %d times, want 2 (the same latency as the counted listener)", got)
	}
}
