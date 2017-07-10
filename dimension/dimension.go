package dimension

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ONSdigital/go-ns/log"
)

// Node represents the dimension details for a node
type Node struct {
	Dimension      string `json:"nodeName"`
	DimensionValue string `json:"value"`
}

// ----------------------------------------------------------------------------

// InvalidImportAPIResponse is returned when an invalid status is returned
// from the import API
type InvalidImportAPIResponse struct {
	api    string
	status int
}

func (e *InvalidImportAPIResponse) Error() string {
	return fmt.Sprintf("invalid status returned from [%s] api: [%d]", e.api, e.status)
}

// ----------------------------------------------------------------------------

// Put executes a put request to the import API
func (dimensionNode *Node) Put(httpClient *http.Client, importAPIURL string) error {
	requestBody, err := json.Marshal(dimensionNode)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("PUT", importAPIURL, bytes.NewBuffer(requestBody))
	if err != nil {
		return err
	}
	res, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return &InvalidImportAPIResponse{importAPIURL, res.StatusCode}
	}

	log.Info("successfully sent request to import API", log.Data{"dimension-name": dimensionNode.Dimension, "dimension-value": dimensionNode.DimensionValue})
	return nil
}
