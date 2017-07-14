package dimension

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var request = &Request{
	Dimension:      "123_sex_female",
	DimensionValue: "female",
	ImportAPIURL:   "http://test-url.com",
	InstanceID:     "123",
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

func TestUnitSendRequest(t *testing.T) {
	Convey("test successful put request", t, func() {
		err := request.Put(createMockClient(200))
		So(err, ShouldBeNil)
	})

	Convey("test error returned when instance id does not match import jobs", t, func() {
		err := request.Put(createMockClient(404))
		So(err, ShouldNotBeNil)
		expectedError := errors.New("invalid status returned from [" + request.ImportAPIURL + "] api: [404]")
		So(err.Error(), ShouldEqual, expectedError.Error())
	})

	Convey("test error returned when client throws error", t, func() {
		err := request.Put(createMockClient(500))
		So(err, ShouldNotBeNil)
	})
}
