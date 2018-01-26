package codelists

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"io/ioutil"
	"net/http"
)

// Instance which contains a list of codes
type Instance struct {
	CodeLists []CodeList `json:"dimensions"`
}

// CodeList contain a dimension's unique id and href for meta data
type CodeList struct {
	ID   string `json:"id"`
	HRef string `json:"href"`
	Name string `json:"name"`
}

//go:generate moq -out testcodelist/importclient.go -pkg testcodelist . ImportClient
// ImportClient
type ImportClient interface {
	Do(ctx context.Context, req *http.Request) (*http.Response, error)
}

// GetFromInstance returns a map of dimension names to code list IDs
func GetFromInstance(ctx context.Context, datasetAPIUrl, datasetToken, instanceID string, client ImportClient) (map[string]string, error) {
	url := fmt.Sprintf("%s/instances/%s", datasetAPIUrl, instanceID)
	codeList := make(map[string]string)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("internal-token", datasetToken)
	response, err := client.Do(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "ImportClient.Do returned error while attempting request to "+url)
	}

	if response.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected status code expected: %d, actual: %s, url: %s", http.StatusOK, response.Status, url)
	}

	bytes, err := ioutil.ReadAll(response.Body)

	var instance Instance
	err = json.Unmarshal(bytes, &instance)
	if err != nil {
		return nil, errors.Wrap(err, "error while attempting to unmarshal json response to codelists.Instance")
	}
	for _, cl := range instance.CodeLists {
		codeList[cl.Name] = cl.ID
	}
	return codeList, nil
}
