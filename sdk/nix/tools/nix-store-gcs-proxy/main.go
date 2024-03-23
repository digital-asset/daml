package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/storage"
	"github.com/urfave/cli"
	"github.com/urfave/negroni"
)

func fetchHeader(req *http.Request, key string) (string, bool) {
	if _, ok := req.Header[key]; ok {
		return req.Header.Get(key), true
	}
	return "", false
}

func run(addr, bucketName string) error {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	bucket := client.Bucket(bucketName)

	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		object := bucket.Object(req.URL.Path[1:])
		switch req.Method {
		case "HEAD":
			_, err := object.Attrs(req.Context())
			if err != nil {
				if err == storage.ErrObjectNotExist {
					w.WriteHeader(404)
					fmt.Fprintf(w, "File not found")
				} else {
					http.Error(w, err.Error(), 500)
				}
				return
			}
		case "GET":
			rc, err := object.NewReader(ctx)
			if err != nil {
				if err == storage.ErrObjectNotExist {
					w.WriteHeader(404)
					fmt.Fprintf(w, "File not found")
				} else {
					http.Error(w, err.Error(), 500)
				}
				return
			}
			defer rc.Close()

			io.Copy(w, rc)
		case "PUT":
			// Copy the supported headers over from the original request
			objectAttrs := storage.ObjectAttrsToUpdate{}
			if val, ok := fetchHeader(req, "Content-Type"); ok {
				objectAttrs.ContentType = val
			}
			if val, ok := fetchHeader(req, "Content-Language"); ok {
				objectAttrs.ContentLanguage = val
			}
			if val, ok := fetchHeader(req, "Content-Encoding"); ok {
				objectAttrs.ContentEncoding = val
			}
			if val, ok := fetchHeader(req, "Content-Disposition"); ok {
				objectAttrs.ContentDisposition = val
			}
			if val, ok := fetchHeader(req, "Cache-Control"); ok {
				objectAttrs.CacheControl = val
			}

			// Write the object to GCS
			wc := object.NewWriter(ctx)
			if _, err := io.Copy(wc, req.Body); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			if err := wc.Close(); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}

			// Apply the headers
			_, err := object.Update(ctx, objectAttrs)
			if err != nil {
				http.Error(w, err.Error(), 500)
			}

			fmt.Fprintf(w, "OK")
		default:
			w.WriteHeader(500)
		}
	})

	n := negroni.Classic() // Includes some default middlewares
	n.UseHandler(handler)

	fmt.Println("Starting proxy server on address", addr, "for bucket", bucketName)
	return http.ListenAndServe(addr, n)
}

func action(c *cli.Context) error {
	addr := c.String("addr")
	bucketName := c.String("bucket-name")
	if bucketName == "" {
		return fmt.Errorf("please specify a bucket name")
	}
	return run(addr, bucketName)
}

func main() {
	app := cli.NewApp()
	app.Name = "nix-store-gcs-proxy"
	app.Usage = "A HTTP nix store that proxies requests to Google Storage"
	app.Action = action
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "bucket-name",
			Usage: "name of the bucket to proxy the data to",
		},
		cli.StringFlag{
			Name:  "addr",
			Value: "localhost:3000",
			Usage: "listening address of the HTTP server",
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
