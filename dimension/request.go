package dimension

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"bytes"
	"encoding/json"

	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/rchttp"
)

// Request represents the request details
type Request struct {
	Attempt             int
	DimensionID         string
	Code                string
	Value               string
	Label               string
	CodeList            string
	InstanceID          string
	DatasetAPIURL       string
	DatasetAPIAuthToken string
	MaxAttempts         int
}

// DimensionOption to store in the dataset api
type DimensionOption struct {
	Name     string `json:"dimension"`
	Code     string `json:"code"`
	CodeList string `json:"code_list,omitempty"`
	Option   string `json:"option"`
	Label    string `json:"label"`
}

// Post executes a post request to the dataset API
func (request *Request) Post(ctx context.Context, httpClient *rchttp.Client) error {
	option, err := json.Marshal(DimensionOption{Name: request.DimensionID, Option: request.Value, Label: request.Label,
		CodeList: request.CodeList, Code: request.Code})

	path := fmt.Sprintf("%s/instances/%s/dimensions", request.DatasetAPIURL, request.InstanceID)

	var URL *url.URL
	URL, err = url.Parse(path)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", URL.String(), bytes.NewReader(option))
	if err != nil {
		return err
	}
	req.Header.Set("internal-token", request.DatasetAPIAuthToken)

	res, err := httpClient.Do(ctx, req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid status [%d] returned from [%s]", res.StatusCode, request.DatasetAPIURL)
	}

	log.Info("successfully sent request to dataset api", log.Data{"instance_id": request.InstanceID, "dimension_name": request.DimensionID, "dimension_value": request.Code})
	return nil
}
