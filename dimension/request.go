package dimension

import (
	"fmt"
	"net/http"
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

// ----------------------------------------------------------------------------

// Put executes a put request to the import API
func (request *Request) Put(httpClient *http.Client) error {
	time.Sleep(time.Duration(request.Attempt) * time.Second)

	url := request.ImportAPIURL + "/instances/" + request.InstanceID + "/dimensions/" + request.Dimension + "/options/" + request.DimensionValue

	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return err
	}

	res, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		errorInvalidStatus := fmt.Errorf("invalid status [%d] returned from [%s]", res.StatusCode, request.ImportAPIURL)

		// If request fails due to an internal server error from
		// Import API try again and increase the backoff
		if res.StatusCode != http.StatusInternalServerError {
			return errorInvalidStatus
		}

		if request.Attempt == request.MaxAttempts {
			return errorInvalidStatus
		}

		request.Attempt++

		if err := request.Put(httpClient); err != nil {
			return errorInvalidStatus
		}
	}

	log.Info("successfully sent request to import api", log.Data{"instance_id": request.InstanceID, "dimension_name": request.Dimension, "dimension_value": request.DimensionValue})
	return nil
}
