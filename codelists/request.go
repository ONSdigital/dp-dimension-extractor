package codelists

import (
	"encoding/json"
	"errors"
	"fmt"
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
	Get(path string) (*http.Response, error)
}

// GetFromInstance returns a map of dimension names to code list IDs
func GetFromInstance(datasetAPIUrl, instanceID string, client ImportClient) (map[string]string, error) {
	url := fmt.Sprintf("%s/instances/%s", datasetAPIUrl, instanceID)
	codeList := make(map[string]string)

	response, err := client.Get(url)
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
