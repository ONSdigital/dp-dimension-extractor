package instance

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var headerNames []string

var instance = &JobInstance{
	Attempt:              1,
	HeaderNames:          headerNames,
	ImportAPIURL:         "http://test-url.com",
	InstanceID:           "123",
	MaxAttempts:          1,
	NumberOfObservations: 1255,
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

func TestUnitRequest(t *testing.T) {
	headerNames = append(headerNames,
		"v4-1",
		"Data_Marking",
		"Time_codelist",
		"Time",
		"Geography_codelist",
		"Geography",
		"Constriction_Sectors_Codelist",
		"Construction_Sectors",
	)
	instance.HeaderNames = headerNames

	Convey("test creation of instance object/stuct", t, func() {
		newInstance := NewJobInstance("http://test-url.com", "123", 1255, headerNames, 1)
		So(newInstance, ShouldResemble, instance)
	})

	Convey("test successful put request", t, func() {
		err := instance.PutData(createMockClient(200))
		So(err, ShouldBeNil)
	})

	Convey("test error returned when instance id does not match import jobs", t, func() {
		err := instance.PutData(createMockClient(404))
		So(err, ShouldNotBeNil)
		expectedError := errors.New("invalid status returned from [" + instance.ImportAPIURL + "] api: [404]")
		So(err.Error(), ShouldEqual, expectedError.Error())
	})

	Convey("test error returned when client throws error", t, func() {
		err := instance.PutData(createMockClient(500))
		So(err, ShouldNotBeNil)
	})

	Convey("test error on second attempt due to client throwing an error", t, func() {
		instance.Attempt = 1
		instance.MaxAttempts = 2
		err := instance.PutData(createMockClient(500))
		So(err, ShouldNotBeNil)
	})
}
