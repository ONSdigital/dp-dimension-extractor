package dimension

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"bytes"
	"encoding/json"
	"github.com/ONSdigital/go-ns/log"
)

// Request represents the request details
type Request struct {
	Attempt             int
	DimensionID         string
	Code                string
	Value               string
	CodeList            string
	InstanceID          string
	DatasetAPIURL       string
	DatasetAPIAuthToken string
	MaxAttempts         int
}

// DimensionOption to store in the dataset api
type DimensionOption struct {
	Name     string `json:"dimension_id"`
	Code     string `json:"code"`
	CodeList string `json:"code_list,omitempty"`
	Option   string `json:"option"`
}

// Put executes a put request to the dataset API
func (request *Request) Post(httpClient *http.Client) error {
	// TODO Instead off backing off by bumping the sleep by 10 seconds per failed
	// request, we may want an exponential backoff
	time.Sleep(time.Duration(request.Attempt-1) * 10 * time.Second)

	option, err := json.Marshal(DimensionOption{Name: request.DimensionID, Option: request.Value, CodeList: request.CodeList, Code: request.Code})

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

	res, err := httpClient.Do(req)
	if err != nil {
		if nextError := request.retryRequest(httpClient, err); nextError != nil {
			return nextError
		}

		return nil
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		errorInvalidStatus := fmt.Errorf("invalid status [%d] returned from [%s]", res.StatusCode, request.DatasetAPIURL)

		// If request fails due to an internal server error from
		// Dataset API try again and increase the backoff
		if res.StatusCode != http.StatusInternalServerError {
			return errorInvalidStatus
		}

		if err := request.retryRequest(httpClient, errorInvalidStatus); err != nil {
			return err
		}
	}

	log.Info("successfully sent request to dataset api", log.Data{"instance_id": request.InstanceID, "dimension_name": request.DimensionID, "dimension_value": request.Code})
	return nil
}

func (request *Request) retryRequest(httpClient *http.Client, err error) error {
	if request.Attempt == request.MaxAttempts {
		return err
	}

	request.Attempt++

	log.Info("attempting request in 10 seconds", log.Data{"attempt": request.Attempt})

	if newErr := request.Post(httpClient); err != nil {
		return newErr
	}

	return nil
}
