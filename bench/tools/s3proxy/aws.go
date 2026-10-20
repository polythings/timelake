package main

// AWS mode: an HTTP proxy in front of a real S3 or S3 Express One Zone bucket.
//
// Clients keep talking path-style plain HTTP to the proxy with dummy keys, so every engine reads the same
// s3://bench/... paths as with the local RustFS. The proxy maps the client-facing bucket to the real one,
// re-signs each request with the default AWS credentials (SigV4; S3 Express via CreateSession), and counts
// requests by kind and response bytes. Requests for any other bucket go to that bucket in the same region
// (e.g. the storage behind an S3 Tables table, whose metadata names it). Each client connection gets its own
// upstream connection, so a fresh client process pays its own DNS, TCP and TLS setup as it would talking to S3
// directly.
//
//	s3proxy -aws-bucket my-bucket -aws-region us-east-2
//	s3proxy -aws-bucket my-bucket--use2-az1--x-s3 -aws-region us-east-2

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Request kinds, as S3 bills them: LIST, PUT, COPY and POST are tier 1; GET and HEAD are tier 2.
var kinds sync.Map // kind -> *atomic.Int64

func countKind(kind string) {
	v, _ := kinds.LoadOrStore(kind, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func kindCounts() map[string]int64 {
	out := map[string]int64{}
	kinds.Range(func(k, v any) bool {
		out[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}

var (
	sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)
	errorCode = regexp.MustCompile(`<Code>[^<]*</Code>`)
)

type awsProxy struct {
	clientBucket string
	bucket       string
	region       string
	host         string
	service      string // "s3" or "s3express"
	cfg          aws.Config
	signer       *v4.Signer

	mu      sync.Mutex
	session aws.Credentials // S3 Express session credentials
	conns   sync.Map        // net.Conn -> *http.Transport
}

type connKey struct{}

// countedKey marks a request from the counting listener (a bool).
type countedKey struct{}

func serveAWS(listen, uncounted, clientBucket, bucket, region string) {
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		log.Fatal(err)
	}
	p := &awsProxy{
		clientBucket: clientBucket, bucket: bucket, region: region, cfg: cfg, service: "s3",
		host:   fmt.Sprintf("%s.s3.%s.amazonaws.com", bucket, region),
		signer: v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true }),
	}
	if m := regexp.MustCompile(`--([a-z0-9-]+)--x-s3$`).FindStringSubmatch(bucket); m != nil {
		p.service = "s3express"
		p.host = fmt.Sprintf("%s.s3express-%s.%s.amazonaws.com", bucket, m[1], region)
	}
	rp := &httputil.ReverseProxy{
		Rewrite:      p.rewrite,
		Transport:    p,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) { log.Print(err); w.WriteHeader(502) },
	}
	log.Printf("proxy %s (bucket %q) -> https://%s (%s)", listen, clientBucket, p.host, p.service)
	if uncounted != "" {
		log.Printf("uncounted proxy %s", uncounted)
		go func() { log.Fatal(p.server(uncounted, rp, false).ListenAndServe()) }()
	}
	log.Fatal(p.server(listen, rp, true).ListenAndServe())
}

// server listens on addr; counted says whether its requests and bytes show in the stats.
func (p *awsProxy) server(addr string, rp *httputil.ReverseProxy, counted bool) *http.Server {
	return &http.Server{
		Addr:    addr,
		Handler: rp,
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			t := &http.Transport{
				Proxy: http.ProxyFromEnvironment, DisableCompression: true, MaxIdleConnsPerHost: 4,
				IdleConnTimeout: 90 * time.Second, ForceAttemptHTTP2: false,
			}
			p.conns.Store(c, t)
			return context.WithValue(context.WithValue(ctx, connKey{}, t), countedKey{}, counted)
		},
		ConnState: func(c net.Conn, s http.ConnState) {
			if s == http.StateClosed || s == http.StateHijacked {
				if t, ok := p.conns.LoadAndDelete(c); ok {
					t.(*http.Transport).CloseIdleConnections()
				}
			}
		},
	}
}

// rewrite maps a path-style request to a virtual-hosted one: the client bucket to the real bucket, any other
// bucket to itself.
func (p *awsProxy) rewrite(pr *httputil.ProxyRequest) {
	in := pr.In
	raw, query, _ := strings.Cut(in.RequestURI, "?")
	bucket, rest, _ := strings.Cut(strings.TrimPrefix(raw, "/"), "/")
	host := p.host
	if bucket != p.clientBucket {
		host = fmt.Sprintf("%s.s3.%s.amazonaws.com", bucket, p.region)
	}
	u, err := url.Parse("https://" + host + "/" + rest)
	if err != nil {
		u = &url.URL{Scheme: "https", Host: host, Path: "/"}
	}
	u.RawQuery = query
	pr.Out.URL = u
	pr.Out.Host = host
}

func classify(r *http.Request) string {
	switch r.Method {
	case http.MethodGet:
		if r.URL.Path == "/" || r.URL.Query().Has("list-type") {
			return "LIST"
		}
		return "GET"
	case http.MethodPut:
		if r.Header.Get("X-Amz-Copy-Source") != "" {
			return "COPY"
		}
		return "PUT"
	}
	return r.Method
}

func (p *awsProxy) RoundTrip(r *http.Request) (*http.Response, error) {
	counted, _ := r.Context().Value(countedKey{}).(bool)
	count := func(kind string) {
		if counted {
			countKind(kind)
		}
	}
	if counted {
		requests.Add(1)
	}
	count(classify(r))

	payload := r.Header.Get("X-Amz-Content-Sha256")
	if strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") {
		// Chunk signatures chain from the client's seed signature, so decode the body and send it plain.
		n, err := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("aws-chunked body without x-amz-decoded-content-length")
		}
		r.Body = io.NopCloser(io.LimitReader(newChunkDecoder(r.Body), n))
		r.ContentLength, r.TransferEncoding = n, nil
		enc := strings.TrimPrefix(strings.TrimPrefix(r.Header.Get("Content-Encoding"), "aws-chunked"), ",")
		if enc == "" {
			r.Header.Del("Content-Encoding")
		} else {
			r.Header.Set("Content-Encoding", enc)
		}
		for _, h := range []string{"X-Amz-Decoded-Content-Length", "X-Amz-Trailer"} {
			r.Header.Del(h)
		}
		payload = "UNSIGNED-PAYLOAD"
	} else if !sha256Hex.MatchString(payload) {
		payload = "UNSIGNED-PAYLOAD"
	}
	if r.ContentLength < 0 && r.Body != nil && r.Body != http.NoBody {
		// A chunked upload (e.g. pyarrow's multipart parts): S3 needs a length, so buffer it (bounded by part size).
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		r.Body, r.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
		r.TransferEncoding = nil
	}
	for _, h := range []string{"Authorization", "X-Amz-Date", "X-Amz-Security-Token", "X-Amz-S3session-Token"} {
		r.Header.Del(h)
	}
	r.Header.Set("X-Amz-Content-Sha256", payload)

	express := p.service == "s3express" && r.URL.Host == p.host
	creds, err := p.credentials(r.Context(), express)
	if err != nil {
		return nil, err
	}
	service := "s3"
	if express {
		service = "s3express"
		r.Header.Set("X-Amz-S3session-Token", creds.SessionToken)
		creds.SessionToken = ""
	}
	if err := p.signer.SignHTTP(r.Context(), creds, r, payload, service, p.region, time.Now().UTC()); err != nil {
		return nil, err
	}

	t, _ := r.Context().Value(connKey{}).(*http.Transport)
	if t == nil {
		t = http.DefaultTransport.(*http.Transport)
	}
	resp, err := t.RoundTrip(r)
	// Retry reads on a dropped connection (S3 resets some under load), as SDKs talking to S3 directly do.
	for attempt := 1; err != nil && attempt <= 3 && (r.Method == http.MethodGet || r.Method == http.MethodHead); attempt++ {
		count("RETRY")
		log.Printf("retry %d %s %s: %v", attempt, r.Method, r.URL.Path, err)
		time.Sleep(time.Duration(attempt*100) * time.Millisecond)
		resp, err = t.RoundTrip(r)
	}
	if err != nil {
		return nil, err
	}
	if d := ttfbMs.Load(); d > 0 {
		time.Sleep(time.Duration(d) * time.Millisecond)
	}
	if resp.StatusCode >= 500 {
		count(fmt.Sprintf("HTTP%d", resp.StatusCode))
	}
	if resp.StatusCode == 400 || resp.StatusCode == 403 || resp.StatusCode >= 500 {
		// Header names only: values include credentials.
		names := make([]string, 0, len(r.Header))
		for h := range r.Header {
			names = append(names, h)
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(body))
		log.Printf("%d %s %s headers=%v response=%s", resp.StatusCode, r.Method, r.URL.Path, names, errorCode.Find(body))
		if os.Getenv("S3PROXY_DEBUG") != "" {
			log.Printf("%s", body)
		}
	}
	if counted {
		resp.Body = &countingBody{resp.Body}
	}
	return resp, nil
}

func (p *awsProxy) credentials(ctx context.Context, express bool) (aws.Credentials, error) {
	if !express {
		return p.cfg.Credentials.Retrieve(ctx)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.session.AccessKeyID != "" && time.Until(p.session.Expires) > time.Minute {
		return p.session, nil
	}
	out, err := s3.NewFromConfig(p.cfg).CreateSession(ctx, &s3.CreateSessionInput{Bucket: aws.String(p.bucket)})
	if err != nil {
		return aws.Credentials{}, err
	}
	c := out.Credentials
	p.session = aws.Credentials{
		AccessKeyID: aws.ToString(c.AccessKeyId), SecretAccessKey: aws.ToString(c.SecretAccessKey),
		SessionToken: aws.ToString(c.SessionToken), CanExpire: true, Expires: aws.ToTime(c.Expiration),
	}
	return p.session, nil
}

type countingBody struct{ io.ReadCloser }

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	bytesOut.Add(int64(n))
	return n, err
}

// chunkDecoder strips aws-chunked framing ("<hex size>;chunk-signature=...\r\n<data>\r\n", ending with a
// zero-size chunk and optional trailers).
type chunkDecoder struct {
	r    *bufio.Reader
	left int64
	done bool
}

func newChunkDecoder(r io.Reader) *chunkDecoder { return &chunkDecoder{r: bufio.NewReader(r)} }

func (d *chunkDecoder) Read(p []byte) (int, error) {
	for d.left == 0 {
		if d.done {
			return 0, io.EOF
		}
		line, err := d.r.ReadString('\n')
		if err != nil {
			return 0, err
		}
		line = strings.TrimSpace(line)
		if line == "" { // CRLF after a chunk's data
			continue
		}
		size, _, _ := strings.Cut(line, ";")
		n, err := strconv.ParseInt(size, 16, 64)
		if err != nil {
			return 0, fmt.Errorf("aws-chunked: bad chunk header %q", line)
		}
		if n == 0 {
			d.done = true
			return 0, io.EOF
		}
		d.left = n
	}
	if int64(len(p)) > d.left {
		p = p[:d.left]
	}
	n, err := d.r.Read(p)
	d.left -= int64(n)
	return n, err
}
