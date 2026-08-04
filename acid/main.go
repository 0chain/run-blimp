// porcupine_s3.go — linearizability check for a Blimp node's storage endpoints.
//
// Model: each object key is an independent linearizable register (like the
// classic Jepsen register test). PUT(key,value) always transitions the
// register to value; GET(key) must return the value of the register as of
// some point between the GET's call and return. Concurrent PUT/GET operations
// are recorded with real wall-clock call/return timestamps, then checked with
// porcupine, which explores all linearizations consistent with those
// intervals — it does not need to know the true order, only that SOME valid
// sequential order exists.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Backend interface {
	Put(key, value string) error
	Get(key string) (string, error)
}

type S3Backend struct {
	client *minio.Client
	bucket string
}

func (b *S3Backend) Put(key, value string) error {
	_, err := b.client.PutObject(context.Background(), b.bucket, key,
		strings.NewReader(value), int64(len(value)), minio.PutObjectOptions{ContentType: "text/plain"})
	return err
}

func (b *S3Backend) Get(key string) (string, error) {
	obj, err := b.client.GetObject(context.Background(), b.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return "", err
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

type FileBackend struct{ dir string }

func (b *FileBackend) Put(key, value string) error {
	// Direct write+close, no tmp+rename: mountpoint-s3 does not support
	// rename, and its atomic unit IS the upload-on-close — the object
	// becomes visible only when Close() completes the PUT. For plain
	// filesystems this loses rename-atomicity, but our file targets
	// (mount-s3, NFS-backed-by-gateway) both map close→PUT.
	f, err := os.OpenFile(filepath.Join(b.dir, key), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte(value)); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (b *FileBackend) Get(key string) (string, error) {
	data, err := os.ReadFile(filepath.Join(b.dir, key))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

type kvInput struct {
	Op    string // "get" or "put"
	Value string // for put
}
type kvOutput struct {
	Value string // for get
	Ok    bool
}

type recordedOp struct {
	key string
	op  porcupine.Operation
}

func main() {
	mode := flag.String("mode", "s3", "label for the report (s3|mounts3|nfs|router)")
	endpoint := flag.String("endpoint", "", "host:port for s3-style modes")
	dir := flag.String("dir", "", "mount dir for file-style modes")
	bucket := flag.String("bucket", "porcupine-test", "")
	accessKey := flag.String("access-key", "", "")
	secretKey := flag.String("secret-key", "", "")
	numClients := flag.Int("clients", 16, "")
	numKeys := flag.Int("keys", 8, "")
	duration := flag.Duration("duration", 30*time.Second, "")
	singleWriter := flag.Bool("single-writer", false, "one designated writer per key (the property object stores actually promise); other clients only GET")
	flag.Parse()

	var backend Backend
	switch {
	case *dir != "":
		backend = &FileBackend{dir: *dir}
	case *endpoint != "":
		cli, err := minio.New(*endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(*accessKey, *secretKey, ""),
			Secure: false,
			// Pin the region so minio-go never issues GetBucketLocation —
			// the zus-router S3 proxy doesn't implement ?location and
			// rejects it with "bucket and key must be specified".
			Region: "us-east-1",
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "minio client:", err)
			os.Exit(2)
		}
		backend = &S3Backend{client: cli, bucket: *bucket}
	default:
		fmt.Fprintln(os.Stderr, "need -endpoint or -dir")
		os.Exit(2)
	}

	keys := make([]string, *numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("porcupine-key-%02d", i)
	}

	// Seed every key with a known value BEFORE the concurrent phase starts,
	// so every GET during the load has a well-defined expected predecessor.
	for _, k := range keys {
		if err := backend.Put(k, "SEED"); err != nil {
			fmt.Fprintf(os.Stderr, "seed put %s failed: %v\n", k, err)
			os.Exit(1)
		}
	}

	var mu sync.Mutex
	var recorded []recordedOp
	var errCount, opCount int64
	var valCounter int64
	var valMu sync.Mutex
	nextVal := func() string {
		valMu.Lock()
		defer valMu.Unlock()
		valCounter++
		return fmt.Sprintf("v%d", valCounter)
	}

	var wg sync.WaitGroup
	stop := time.Now().Add(*duration)
	start := time.Now()
	for c := 0; c < *numClients; c++ {
		wg.Add(1)
		go func(clientId int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientId)*7919))
			for time.Now().Before(stop) {
				key := keys[rng.Intn(len(keys))]
				isPut := rng.Intn(2) == 0
				if *singleWriter {
					// clientId 0 is the sole writer for every key; every other
					// client only GETs — this tests "does a completed write
					// become visible to all readers", the consistency property
					// object stores actually document, without the ambiguity
					// of multiple writers racing on the same key.
					isPut = clientId == 0
				}
				call := time.Since(start).Nanoseconds()
				if isPut {
					v := nextVal()
					err := backend.Put(key, v)
					ret := time.Since(start).Nanoseconds()
					mu.Lock()
					opCount++
					if err != nil {
						errCount++
					} else {
						recorded = append(recorded, recordedOp{key: key, op: porcupine.Operation{
							ClientId: clientId, Input: kvInput{Op: "put", Value: v},
							Call: call, Output: kvOutput{Ok: true}, Return: ret,
						}})
					}
					mu.Unlock()
				} else {
					v, err := backend.Get(key)
					ret := time.Since(start).Nanoseconds()
					mu.Lock()
					opCount++
					if err != nil {
						errCount++
					} else {
						recorded = append(recorded, recordedOp{key: key, op: porcupine.Operation{
							ClientId: clientId, Input: kvInput{Op: "get"},
							Call: call, Output: kvOutput{Value: v, Ok: true}, Return: ret,
						}})
					}
					mu.Unlock()
				}
			}
		}(c)
	}
	wg.Wait()

	byKey := map[string][]porcupine.Operation{}
	for _, r := range recorded {
		byKey[r.key] = append(byKey[r.key], r.op)
	}

	model := porcupine.Model{
		Init: func() interface{} { return "SEED" },
		Step: func(state, input, output interface{}) (bool, interface{}) {
			st := state.(string)
			in := input.(kvInput)
			out := output.(kvOutput)
			if in.Op == "put" {
				return true, in.Value // a write is always legal; it defines the new state
			}
			return st == out.Value, st // a read must match the current register value
		},
		DescribeOperation: func(input, output interface{}) string {
			in := input.(kvInput)
			out := output.(kvOutput)
			if in.Op == "put" {
				return fmt.Sprintf("put(%s)", in.Value)
			}
			return fmt.Sprintf("get() -> %s", out.Value)
		},
	}

	fmt.Printf("== porcupine linearizability check: mode=%s endpoint=%s dir=%s ==\n", *mode, *endpoint, *dir)
	fmt.Printf("clients=%d keys=%d duration=%s total_ops=%d errors=%d\n", *numClients, *numKeys, *duration, opCount, errCount)

	allOk := true
	for _, k := range keys {
		ops := byKey[k]
		if len(ops) == 0 {
			continue
		}
		res, info := porcupine.CheckOperationsVerbose(model, ops, 0)
		status := "OK"
		if res != porcupine.Ok {
			status = fmt.Sprintf("%v", res)
			allOk = false
		}
		fmt.Printf("  key=%-20s ops=%-4d result=%s\n", k, len(ops), status)
		if res != porcupine.Ok {
			f, _ := os.CreateTemp("", "porcupine-*.html")
			porcupine.VisualizePath(model, info, f.Name())
			fmt.Printf("    !! VIOLATION — visualization: %s\n", f.Name())
			if os.Getenv("PORCUPINE_DUMP") != "" {
				fmt.Printf("    -- raw ops for %s, sorted by call time --\n", k)
				sorted := append([]porcupine.Operation(nil), ops...)
				for i := 0; i < len(sorted); i++ {
					for j := i + 1; j < len(sorted); j++ {
						if sorted[j].Call < sorted[i].Call {
							sorted[i], sorted[j] = sorted[j], sorted[i]
						}
					}
				}
				for _, o := range sorted {
					in := o.Input.(kvInput)
					out := o.Output.(kvOutput)
					if in.Op == "put" {
						fmt.Printf("      client=%2d call=%12d ret=%12d PUT   value=%s\n", o.ClientId, o.Call, o.Return, in.Value)
					} else {
						fmt.Printf("      client=%2d call=%12d ret=%12d GET   value=%s\n", o.ClientId, o.Call, o.Return, out.Value)
					}
				}
			}
		}
	}

	if allOk {
		fmt.Printf("RESULT: LINEARIZABLE — no violations across %d keys, %d ops (%d errors)\n", *numKeys, opCount, errCount)
		os.Exit(0)
	}
	fmt.Println("RESULT: NOT LINEARIZABLE — see violations above")
	os.Exit(1)
}
