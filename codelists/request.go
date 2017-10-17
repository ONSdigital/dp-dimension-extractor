package codelists

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
)

const (
	clientGetErr        = "codelists.GetFromInstance ImportClient.Get returned an error: url=%s"
	incorrectStatusErr  = "codelists.GetFromInstance ImportClient.Get returned an unexpected response status: expected=200, actual=%d, url=%s"
	readResponseBodyErr = "codelists.GetFromInstance error while attempting to read response body: url=%s"
	unmarshalBodyErr    = "codelists.GetFromInstance error while attempting to unmarshal response body to codelists.Instance: url=%s"
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
		return nil, errors.Wrap(err, fmt.Sprintf(clientGetErr, url))
	}

	if response.StatusCode != http.StatusOK {
		return nil, errors.Errorf(incorrectStatusErr, response.StatusCode, url)
	}

	defer response.Body.Close()
	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(readResponseBodyErr, url))
	}

	var instance Instance
	err = json.Unmarshal(bytes, &instance)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(unmarshalBodyErr, url))
	}
	for _, cl := range instance.CodeLists {
		codeList[cl.Name] = cl.ID
	}
	return codeList, nil
}
