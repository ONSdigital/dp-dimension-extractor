package dimension

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/ONSdigital/go-ns/log"
)

// Request represents the request details
type Request struct {
	InstanceID     string
	ImportAPIURL   string
	Dimension      string `json:"nodeName"`
	DimensionValue string `json:"value"`
}

// Node represents the dimension details for a node
type Node struct {
	Dimension      string `json:"nodeName"`
	DimensionValue string `json:"value"`
}

// ----------------------------------------------------------------------------

// SendRequest executes a put request to the import API
func (request *Request) Put(httpClient *http.Client) error {
	url := request.ImportAPIURL + "/instances/" + request.InstanceID + "/dimensions"

	node := &Node{
		Dimension:      request.Dimension,
		DimensionValue: request.DimensionValue,
	}

	requestBody, err := json.Marshal(node)
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
		return errors.New(fmt.Sprintf("invalid status returned from [%s] api: [%d]", request.ImportAPIURL, res.StatusCode))
	}

	log.Info("successfully sent request to import API", log.Data{"dimension-name": node.Dimension, "dimension-value": node.DimensionValue})
	return nil
}
