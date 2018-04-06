package instance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/rchttp"
)

const authorizationHeader = "Authorization"

// JobInstance represents the details necessary to update a job instance
type JobInstance struct {
	Attempt              int
	AuthToken            string
	DatasetAPIURL        string
	DatasetAPIAuthToken  string
	HeaderNames          []string `json:"headers"`
	InstanceID           string
	MaxAttempts          int
	NumberOfObservations int `json:"total_observations"`
}

// NewJobInstance returns a new JobInstance object for a given instance
func NewJobInstance(authToken, datasetAPIURL, datasetAPIAuthToken, instanceID string, numberOfObservations int, headerNames []string, maxAttempts int) *JobInstance {
	return &JobInstance{
		Attempt:              1,
		AuthToken:            authToken,
		HeaderNames:          headerNames,
		DatasetAPIURL:        datasetAPIURL,
		DatasetAPIAuthToken:  datasetAPIAuthToken,
		InstanceID:           instanceID,
		MaxAttempts:          maxAttempts,
		NumberOfObservations: numberOfObservations,
	}
}

// PutData executes a put request to update instance data via the dataset API.
func (instance *JobInstance) PutData(ctx context.Context, httpClient *rchttp.Client) error {
	time.Sleep(time.Duration(instance.Attempt-1) * 10 * time.Second)
	path := instance.DatasetAPIURL + "/instances/" + instance.InstanceID

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
	// TODO Remove "intenral-token" header, now uses "Authorization" header
	req.Header.Set("internal-token", instance.DatasetAPIAuthToken)
	req.Header.Set(authorizationHeader, instance.AuthToken)

	res, err := httpClient.Do(ctx, req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid status [%d] returned from [%s]", res.StatusCode, instance.DatasetAPIURL)
	}

	log.Info("successfully sent request to dataset API", log.Data{"instance_id": instance.InstanceID, "number_of_observations": instance.NumberOfObservations})
	return nil
}
