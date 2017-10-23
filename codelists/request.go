package codelists

import (
	"encoding/json"
	"errors"
	"fmt"
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
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		return nil, errors.New("Unexpected status code returned, " + response.Status)
	}

	bytes, err := ioutil.ReadAll(response.Body)

	var instance Instance
	err = json.Unmarshal(bytes, &instance)
	if err != nil {
		return nil, err
	}
	for _, cl := range instance.CodeLists {
		codeList[cl.Name] = cl.ID
	}
	return codeList, nil
}
