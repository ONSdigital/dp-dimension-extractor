package dimension

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/ONSdigital/go-ns/log"
)

// Request represents the request details
type Request struct {
	Attempt        int
	Dimension      string
	DimensionValue string
	InstanceID     string
	ImportAPIURL   string
	MaxAttempts    int
}

// Put executes a put request to the import API
func (request *Request) Put(httpClient *http.Client) error {
	// TODO Instead off backing off by bumping the sleep by 10 seconds per failed
	// request, we may want an exponential backoff
	time.Sleep(time.Duration(request.Attempt-1) * 10 * time.Second)

	path := request.ImportAPIURL + "/instances/" + request.InstanceID + "/dimensions/" + request.Dimension + "/options/" + request.DimensionValue

	var URL *url.URL
	URL, err := url.Parse(path)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", URL.String(), nil)
	if err != nil {
		return err
	}

	res, err := httpClient.Do(req)
	if err != nil {
		if nextError := request.retryRequest(httpClient, err); nextError != nil {
			return nextError
		}

		return nil
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		errorInvalidStatus := fmt.Errorf("invalid status [%d] returned from [%s]", res.StatusCode, request.ImportAPIURL)

		// If request fails due to an internal server error from
		// Import API try again and increase the backoff
		if res.StatusCode != http.StatusInternalServerError {
			return errorInvalidStatus
		}

		if err := request.retryRequest(httpClient, errorInvalidStatus); err != nil {
			return err
		}
	}

	log.Info("successfully sent request to import api", log.Data{"instance_id": request.InstanceID, "dimension_name": request.Dimension, "dimension_value": request.DimensionValue})
	return nil
}

func (request *Request) retryRequest(httpClient *http.Client, err error) error {
	if request.Attempt == request.MaxAttempts {
		return err
	}

	request.Attempt++

	log.Info("attempting request in 10 seconds", log.Data{"attempt": request.Attempt})

	if newErr := request.Put(httpClient); err != nil {
		return newErr
	}

	return nil
}
