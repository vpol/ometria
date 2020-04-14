package mailchimp

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/vpol/ometria/api"
	"github.com/vpol/ometria/internal/httpwrap"
	"github.com/vpol/ometria/internal/state"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

type MailChimpResponse struct {
	Members []MailChimpEntity `json:"members"`
}

type MailChimpEntity struct {
	ID           string `json:"id"`
	EmailAddress string `json:"email_address"`
	MergeFields  struct {
		FNAME string `json:"FNAME"`
		LNAME string `json:"LNAME"`
	} `json:"merge_fields"`
	Status      string    `json:"status"`
	LastChanged time.Time `json:"last_changed"`
}

var (
	apiKey     string
	apiURL     string
	apiTimeout time.Duration
	apiLimit   int

	logger *log.Logger
)

func init() {
	flag.StringVar(&apiKey, "mailchimp_key", "", "mailchimp api key")
	flag.StringVar(&apiURL, "mailchimp_url", "https://us9.api.mailchimp.com/3.0", "mailchimp api url (without trailing slash)")
	flag.DurationVar(&apiTimeout, "mailchimp_timeout", 30*time.Second, "mailchimp api timeout")
	flag.IntVar(&apiLimit, "mailchimp_limit", 100, "mailchimp api limit records")

	logger = log.New(os.Stdout, "mailchimp ", log.LstdFlags)
}

type Client struct {
	fetch func(ctx context.Context, rUrl string, offset int, lastUpdate int64) (entities []api.Ometria, last time.Time, err error)
}

func NewClient() Client {

	if apiKey == "" {
		logger.Panicf("mailchimp api key is empty")
	}

	if apiURL == "" {
		logger.Panicf("mailchimp api url is empty")
	}

	c := Client{}

	c.fetch = c.mailChimpFetch

	return c
}

func (c Client) Get(ctx context.Context, id string, currObject state.State) (data []api.Ometria, newObject state.State, err error) {

	var last int64

	data, last, err = c.get(ctx, id, currObject)
	if err != nil {
		return nil, state.State{}, err
	}

	log.Printf("total %d records, last %d", len(data), last)

	if last > currObject.LastUpdate {
		newObject = state.State{LastUpdate: last}
	} else {
		newObject = currObject
	}

	return
}

func (c Client) mailChimpFetch(ctx context.Context, rUrl string, offset int, lastUpdate int64) (entities []api.Ometria, last time.Time, err error) {
	hclient := httpwrap.NewHTTPClient(apiTimeout)

	var req *http.Request

	req, err = http.NewRequest(http.MethodGet, rUrl, nil)
	if err != nil {
		return
	}

	q := req.URL.Query()

	if lastUpdate > 0 {
		q.Add("since_last_changed", time.Unix(lastUpdate, 0).Format("2006-01-02T15:04:05-07:00"))
	}

	if offset > 0 {
		q.Add("offset", strconv.Itoa(offset))
	}

	q.Add("count", strconv.Itoa(apiLimit))

	req.URL.RawQuery = q.Encode()

	req.SetBasicAuth("", apiKey)

	var resp *http.Response
	var cancel context.CancelFunc

	resp, cancel, err = hclient.Do(ctx, req)
	if err != nil {
		return
	}

	defer cancel()

	var body []byte

	body, err = ioutil.ReadAll(resp.Body)

	if err != nil {
		return
	}

	defer resp.Body.Close()

	var r MailChimpResponse

	err = json.Unmarshal(body, &r)

	if err != nil {
		return
	}

	for _, entity := range r.Members {

		if entity.LastChanged.After(last) {
			last = entity.LastChanged
		}

		entities = append(entities, api.Ometria{
			ID:        entity.ID,
			Firstname: entity.MergeFields.FNAME,
			Lastname:  entity.MergeFields.LNAME,
			Email:     entity.EmailAddress,
			Status:    entity.Status,
		})
	}

	return
}

func (c Client) get(ctx context.Context, id string, currState state.State) (data []api.Ometria, lastRecord int64, err error) {

	rUrl := fmt.Sprintf("%s/lists/%s/members", apiURL, id)

	f := c.fetch

	var t = true
	var offset = 0
	var last time.Time

	for t {

		var records []api.Ometria
		var lastR time.Time

		records, lastR, err = f(ctx, rUrl, offset, currState.LastUpdate)

		if err != nil {
			return
		}

		if lastR.After(last) {
			last = lastR
		}

		if len(records) < apiLimit {
			t = false
		}

		offset = offset + len(records)

		logger.Printf("received %d records, offset = %d, total = %d, more? = %t, last = %s", len(records), offset, len(data), t, last)

		data = append(data, records...)

	}

	lastRecord = last.UTC().Unix()

	return

}
