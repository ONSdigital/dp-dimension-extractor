package dimension_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ONSdigital/dp-dimension-extractor/dimension"
	"github.com/ONSdigital/go-ns/rchttp"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	request = &dimension.RequestBatch{
		DatasetAPIURL: "http://test-url.com",
		Batch: []dimension.Request{
			dimension.Request{
				DimensionID: "123_sex_female",
				Value:       "female",
			},
		},
	}
	instanceID  = "inst123"
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

func TestUnitSendRequest(t *testing.T) {
	Convey("test successful put request", t, func() {

		err := request.Post(ctx, createMockClient(1, 200), instanceID)
		So(err, ShouldBeNil)
	})

	Convey("test error returned when instance id does not match any instances", t, func() {
		err := request.Post(ctx, createMockClient(1, 404), instanceID)
		So(err, ShouldNotBeNil)
		expectedError := errors.New("invalid status [404] returned from [" + request.DatasetAPIURL + "]")
		So(err.Error(), ShouldEqual, expectedError.Error())
	})

	Convey("test error returned when client throws error", t, func() {
		start := time.Now()
		err := request.Post(ctx, createMockClient(1, 500), instanceID)
		oneAttempt = time.Since(start)

		So(err, ShouldNotBeNil)
		expectedError := errors.New("invalid status [500] returned from [" + request.DatasetAPIURL + "]")
		So(err.Error(), ShouldEqual, expectedError.Error())
	})

	Convey("test error on second attempt", t, func() {
		start := time.Now()
		err := request.Post(ctx, createMockClient(2, 500), instanceID)
		twoAttempts = time.Since(start)

		So(err, ShouldNotBeNil)
		expectedError := errors.New("invalid status [500] returned from [" + request.DatasetAPIURL + "]")
		So(err.Error(), ShouldEqual, expectedError.Error())
		So(twoAttempts, ShouldBeGreaterThan, oneAttempt)
	})
}
