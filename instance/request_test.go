package instance_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ONSdigital/dp-dimension-extractor/instance"
	"github.com/ONSdigital/go-ns/rchttp"
	. "github.com/smartystreets/goconvey/convey"
)

var headerNames []string

var (
	job = &instance.JobInstance{
		Attempt:              1,
		HeaderNames:          headerNames,
		DatasetAPIURL:        "http://test-url.com",
		DatasetAPIAuthToken:  "sfqr-4f345-f43534",
		InstanceID:           "123",
		MaxAttempts:          1,
		NumberOfObservations: 1255,
	}

	ctx         context.Context
	oneAttempt  time.Duration
	twoAttempts time.Duration
)

func init() {
	ctx = context.Background()
}

func createMockClient(maxRetries, status int) *rchttp.Client {
	mockStreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(status)
	}))
	transport := &http.Transport{
		Proxy: func(req *http.Request) (*url.URL, error) {
			return url.Parse(mockStreamServer.URL)
		},
	}

	httpClient := rchttp.DefaultClient
	httpClient.HTTPClient = &http.Client{Transport: transport}
	httpClient.MaxRetries = maxRetries

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
		newJob := instance.NewJobInstance("http://test-url.com", job.DatasetAPIAuthToken, "123", 1255, headerNames, 1)
		So(newJob, ShouldResemble, job)
	})

	Convey("test successful put request", t, func() {
		err := job.PutData(ctx, createMockClient(1, 200))
		So(err, ShouldBeNil)
	})

	Convey("test error returned when instance ID does not match an existing instance ID", t, func() {
		err := job.PutData(ctx, createMockClient(1, 404))
		So(err, ShouldNotBeNil)
		expectedError := errors.New("invalid status [404] returned from [" + job.DatasetAPIURL + "]")
		So(err.Error(), ShouldEqual, expectedError.Error())
	})

	Convey("test error returned when client throws error", t, func() {
		start := time.Now()
		err := job.PutData(ctx, createMockClient(1, 500))
		oneAttempt = time.Since(start)

		So(err, ShouldNotBeNil)
		expectedError := errors.New("invalid status [500] returned from [" + job.DatasetAPIURL + "]")
		So(err.Error(), ShouldEqual, expectedError.Error())
	})

	Convey("test error on second attempt due to client throwing an error", t, func() {
		start := time.Now()
		err := job.PutData(ctx, createMockClient(2, 500))
		twoAttempts = time.Since(start)

		So(err, ShouldNotBeNil)
		expectedError := errors.New("invalid status [500] returned from [" + job.DatasetAPIURL + "]")
		So(err.Error(), ShouldEqual, expectedError.Error())
		// This assertion is dependent on oneAttempt value from
		// `test error returned when client throws error` test
		So(twoAttempts, ShouldBeGreaterThan, oneAttempt)
	})
}
