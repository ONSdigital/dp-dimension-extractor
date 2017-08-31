package instance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/ONSdigital/go-ns/log"
)

// JobInstance represents the details necessary to update a job instance
type JobInstance struct {
	Attempt              int
	HeaderNames          []string `json:"headers"`
	InstanceID           string
	ImportAPIURL         string
	ImportAPIAuthToken   string
	MaxAttempts          int
	NumberOfObservations int `json:"total_observations"`
}

// NewJobInstance returns a new JobInstance object for a given instance
func NewJobInstance(importAPIURL string, importAPIAuthToken string, instanceID string, numberOfObservations int, headerNames []string, maxAttempts int) *JobInstance {
	return &JobInstance{
		Attempt:              1,
		HeaderNames:          headerNames,
		ImportAPIURL:         importAPIURL,
		ImportAPIAuthToken:   importAPIAuthToken,
		InstanceID:           instanceID,
		MaxAttempts:          maxAttempts,
		NumberOfObservations: numberOfObservations,
	}
}

// PutData executes a put request to insert the number of
// observations against a job instance via the import API
func (instance *JobInstance) PutData(httpClient *http.Client) error {
	time.Sleep(time.Duration(instance.Attempt-1) * 10 * time.Second)
	path := instance.ImportAPIURL + "/instances/" + instance.InstanceID

	var URL *url.URL
	URL, err := url.Parse(path)
	if err != nil {
		return err
	}

	requestBody, err := json.Marshal(instance)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", URL.String(), bytes.NewBuffer(requestBody))
	if err != nil {
		return err
	}
	req.Header.Set("internal-token", instance.ImportAPIAuthToken)

	res, err := httpClient.Do(req)
	if err != nil {
		if nextError := instance.retryRequest(httpClient, err); nextError != nil {
			return nextError
		}

		return nil
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		errorInvalidStatus := fmt.Errorf("invalid status [%d] returned from [%s]", res.StatusCode, instance.ImportAPIURL)

		// If request fails due to an internal server error from
		// Import API try again and increase the backoff
		if res.StatusCode != http.StatusInternalServerError {
			return errorInvalidStatus
		}

		if err := instance.retryRequest(httpClient, errorInvalidStatus); err != nil {
			return err
		}
	}

	log.Info("successfully sent request to import API", log.Data{"instance_id": instance.InstanceID, "number_of_observations": instance.NumberOfObservations})
	return nil
}

func (jobInstance *JobInstance) retryRequest(httpClient *http.Client, err error) error {
	if jobInstance.Attempt == jobInstance.MaxAttempts {
		return err
	}

	jobInstance.Attempt++

	log.Info("attempting request in 10 seconds", log.Data{"attempt": jobInstance.Attempt})

	if newErr := jobInstance.PutData(httpClient); err != nil {
		return newErr
	}

	return nil
}
