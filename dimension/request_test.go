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
	rchttp "github.com/ONSdigital/dp-rchttp"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	request = &dimension.Request{
		Attempt:       1,
		AuthToken:     "asfe-34sfd-23",
		DimensionID:   "123_sex_female",
		Value:         "female",
		DatasetAPIURL: "http://test-url.com",
		InstanceID:    "123",
		MaxAttempts:   1,
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

func TestUnitSendRequest(t *testing.T) {
	Convey("test successful put request", t, func() {

		err := request.Post(ctx, createMockClient(1, 200))
		So(err, ShouldBeNil)
	})

	Convey("test error returned when instance id does not match any instances", t, func() {
		err := request.Post(ctx, createMockClient(1, 404))
		So(err, ShouldNotBeNil)
		expectedError := errors.New("invalid status [404] returned from [" + request.DatasetAPIURL + "]")
		So(err.Error(), ShouldEqual, expectedError.Error())
	})

	Convey("test error returned when client throws error", t, func() {
		start := time.Now()
		err := request.Post(ctx, createMockClient(1, 500))
		oneAttempt = time.Since(start)

		So(err, ShouldNotBeNil)
		expectedError := errors.New("invalid status [500] returned from [" + request.DatasetAPIURL + "]")
		So(err.Error(), ShouldEqual, expectedError.Error())
	})

	Convey("test error on second attempt", t, func() {
		start := time.Now()
		err := request.Post(ctx, createMockClient(2, 500))
		twoAttempts = time.Since(start)

		So(err, ShouldNotBeNil)
		expectedError := errors.New("invalid status [500] returned from [" + request.DatasetAPIURL + "]")
		So(err.Error(), ShouldEqual, expectedError.Error())
		So(twoAttempts, ShouldBeGreaterThan, oneAttempt)
	})
}
