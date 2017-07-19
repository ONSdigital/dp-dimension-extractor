package instance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ONSdigital/go-ns/log"
)

// JobInstance represents the details necessary to update a job instance
type JobInstance struct {
	Attempt              int
	HeaderNames          []string `json:"headers"`
	InstanceID           string
	ImportAPIURL         string
	MaxAttempts          int
	NumberOfObservations int `json:"number_of_observations"`
}

// ----------------------------------------------------------------------------

// NewJobInstance returns a new JobInstance object for a given instance
func NewJobInstance(importAPIURL string, instanceID string, numberOfObservations int, headerNames []string, maxAttempts int) *JobInstance {
	return &JobInstance{
		Attempt:              1,
		HeaderNames:          headerNames,
		ImportAPIURL:         importAPIURL,
		InstanceID:           instanceID,
		MaxAttempts:          maxAttempts,
		NumberOfObservations: numberOfObservations,
	}
}

// ----------------------------------------------------------------------------

// PutObservationCount executes a put request to insert the number of
// observations against a job instance via the import API
func (instance *JobInstance) PutData(httpClient *http.Client) error {
	url := instance.ImportAPIURL + "/instances/" + instance.InstanceID

	requestBody, err := json.Marshal(instance.NumberOfObservations)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return err
	}

	res, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		// If request fails due to an internal server error from
		// Import API try again and increase the backoff
		if res.StatusCode != http.StatusInternalServerError {
			return fmt.Errorf("invalid status returned from [%s] api: [%d]", instance.ImportAPIURL, res.StatusCode)
		}

		if instance.Attempt == instance.MaxAttempts {
			return fmt.Errorf("invalid status returned from [%s] api: [%d]", instance.ImportAPIURL, res.StatusCode)
		}

		instance.Attempt++

		if err := instance.PutData(httpClient); err != nil {
			return fmt.Errorf("invalid status returned from [%s] api: [%d]", instance.ImportAPIURL, res.StatusCode)
		}
	}

	log.Info("successfully sent request to import API", log.Data{"instance_id": instance.InstanceID, "number_of_observations": instance.NumberOfObservations})
	return nil
}
