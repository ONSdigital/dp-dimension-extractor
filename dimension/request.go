package dimension

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"bytes"
	"encoding/json"

	rchttp "github.com/ONSdigital/dp-rchttp"
	"github.com/ONSdigital/log.go/log"
)

const authorizationHeader = "Authorization"

// Request represents the request details
type Request struct {
	Attempt             int
	AuthToken           string
	Code                string
	CodeList            string
	DatasetAPIURL       string
	DatasetAPIAuthToken string
	DimensionID         string
	InstanceID          string
	Label               string
	MaxAttempts         int
	Value               string
}

// Option to store in the dataset api
type Option struct {
	Code     string `json:"code"`
	CodeList string `json:"code_list,omitempty"`
	Label    string `json:"label"`
	Name     string `json:"dimension"`
	Option   string `json:"option"`
}

// Post executes a post request to the dataset API
func (request *Request) Post(ctx context.Context, httpClient *rchttp.Client) error {
	option, err := json.Marshal(Option{Name: request.DimensionID, Option: request.Value, Label: request.Label,
		CodeList: request.CodeList, Code: request.Code})
	if err != nil {
		return err
	}

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
	// TODO Remove "intenral-token" header, now uses "Authorization" header
	req.Header.Set("internal-token", request.DatasetAPIAuthToken)
	req.Header.Set(authorizationHeader, request.AuthToken)

	res, err := httpClient.Do(ctx, req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid status [%d] returned from [%s]", res.StatusCode, request.DatasetAPIURL)
	}

	log.Event(ctx, "successfully sent request to dataset api", log.INFO, log.Data{"instance_id": request.InstanceID, "dimension_name": request.DimensionID, "dimension_value": request.Code})
	return nil
}
