package dimension

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var dimensionNode = &Node{
	Dimension:      "123_sex_female",
	DimensionValue: "female",
}

func createMockClient(status int) *http.Client {
	mockStreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(status)
	}))
	transport := &http.Transport{
		Proxy: func(req *http.Request) (*url.URL, error) {
			return url.Parse(mockStreamServer.URL)
		},
	}
	httpClient := &http.Client{Transport: transport}
	return httpClient
}

func TestUnitPutDimensionResponse(t *testing.T) {
	Convey("test successful put request", t, func() {
		instanceID := "123"
		err := dimensionNode.Put(createMockClient(200), "http://test-url.com/imports/"+instanceID+"/dimensions")
		So(err, ShouldBeNil)
	})

	Convey("test error returned when instance id does not match import jobs", t, func() {
		instanceID := "124"
		url := "http://test-url.com/imports/" + instanceID + "/dimensions"

		err := dimensionNode.Put(createMockClient(404), url)
		So(err, ShouldNotBeNil)
		expectedError := errors.New("invalid status returned from [" + url + "] api: [404]")
		So(err.Error(), ShouldEqual, expectedError.Error())
	})

	Convey("test error returned when client throws error", t, func() {
		url := "test-url.com"
		err := dimensionNode.Put(createMockClient(500), url)
		So(err, ShouldNotBeNil)
	})
}
