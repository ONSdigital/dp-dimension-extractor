package instance_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/ONSdigital/dp-dimension-extractor/instance"
	. "github.com/smartystreets/goconvey/convey"
)

var headerNames []string

var job = &instance.JobInstance{
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
		"Time codelist",
		"Time",
		"Geography codelist",
		"Geography",
		"Constriction Sectors Codelist",
		"Construction Sectors",
	)
	job.HeaderNames = headerNames

	Convey("test creation of instance object/stuct", t, func() {
		newJob := instance.NewJobInstance("http://test-url.com", "123", 1255, headerNames, 1)
		So(newJob, ShouldResemble, job)
	})

	Convey("test successful put request", t, func() {
		err := job.PutData(createMockClient(200))
		So(err, ShouldBeNil)
	})

	Convey("test error returned when instance id does not match import jobs", t, func() {
		err := job.PutData(createMockClient(404))
		So(err, ShouldNotBeNil)
		expectedError := errors.New("invalid status [404] returned from [" + job.ImportAPIURL + "]")
		So(err.Error(), ShouldEqual, expectedError.Error())
	})

	Convey("test error returned when client throws error", t, func() {
		err := job.PutData(createMockClient(500))
		So(err, ShouldNotBeNil)
	})

	Convey("test error on second attempt due to client throwing an error", t, func() {
		job.Attempt = 1
		job.MaxAttempts = 2
		err := job.PutData(createMockClient(500))
		So(err, ShouldNotBeNil)
	})
}
