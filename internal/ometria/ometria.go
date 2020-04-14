package ometria

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/vpol/ometria/api"
	"github.com/vpol/ometria/internal/httpwrap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

var (
	apiKey      string
	apiURL      string
	timeout     time.Duration
	batchSize   int
	concurrency int64

	logger *log.Logger
)

func init() {
	flag.StringVar(&apiKey, "ometria_key", "", "ometria api key")
	flag.StringVar(&apiURL, "ometria_url", "http://ec2-34-242-147-110.eu-west-1.compute.amazonaws.com:8080/record", "ometria api url")
	flag.DurationVar(&timeout, "ometria_timeout", 60*time.Second, "ometria api timeout (duration)")
	flag.IntVar(&batchSize, "ometria_batchsize", 1000, "ometria batch size (number)")
	flag.Int64Var(&concurrency, "ometria_concurrency", 10, "ometria concurrency size (number)")

	logger = log.New(os.Stdout, "ometria ", log.LstdFlags)
}

type Client struct {
	sem    *semaphore.Weighted
	action func(ctx context.Context, data []api.Ometria) error
}

func NewClient() *Client {

	if apiKey == "" {
		logger.Panicf("ometria api key is empty")
	}

	if apiURL == "" {
		logger.Panicf("ometria api url is empty")
	}

	client := &Client{
		sem: semaphore.NewWeighted(concurrency),
	}

	client.action = client.put

	return client
}

func (o Client) Put(ctx context.Context, data []api.Ometria) error {
	return o.action(ctx, data)
}

func (o Client) put(ctx context.Context, data []api.Ometria) error {

	errGrp := errgroup.Group{}

	for len(data) > batchSize {

		put := data[0:batchSize]
		data = data[batchSize:]

		errGrp.Go(func() error {

			if err := o.sem.Acquire(ctx, 1); err != nil {
				logger.Printf("failed to acquire semaphore: %v", err)
				return err
			}

			defer o.sem.Release(1)

			logger.Printf("data length %d, batchSize %d", len(data), batchSize)
			return o.insert(ctx, put)
		})
	}

	if len(data) > 0 {

		errGrp.Go(func() error {

			if err := o.sem.Acquire(ctx, 1); err != nil {
				logger.Printf("failed to acquire semaphore: %v", err)
				return err
			}
			defer o.sem.Release(1)

			logger.Printf("data length %d", len(data))
			return o.insert(ctx, data)
		})
	}

	return errGrp.Wait()
}

func (o Client) insert(ctx context.Context, data []api.Ometria) error {

	hclient := httpwrap.NewHTTPClient(timeout)

	var d []byte
	buffer := bytes.NewBuffer(d)
	encoder := json.NewEncoder(buffer)
	if err := encoder.Encode(data); err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, apiURL, buffer)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", apiKey)

	req.Header.Set("Content-Type", "application/json")

	resp, cancel, err := hclient.Do(ctx, req)
	if err != nil {
		return err
	}

	defer cancel()

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	var s = struct {
		Content int    `json:"content"`
		Status  string `json:"status"`
	}{}

	if err := json.Unmarshal(body, &s); err != nil {
		return err
	}

	if s.Status != "OK" || s.Content != len(data) {
		return fmt.Errorf("unexpected response value %s", string(body))
	}

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("expected 201 got %d", resp.StatusCode)
	}

	logger.Printf("ingested %d records", len(data))

	return nil
}
