package codelists

import (
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/ONSdigital/dp-dimension-extractor/codelists/testcodelist"
	"github.com/ONSdigital/dp-dimension-extractor/config"
	"github.com/ian-kent/go-log/log"
	. "github.com/smartystreets/goconvey/convey"

	"golang.org/x/net/context"
)

var (
	url string
	ctx context.Context
)

func init() {
	cfg, err := config.Get()
	if err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}

	url = cfg.DatasetAPIURL
	ctx = context.Background()
}

func TestGetFromInstanceReturnsCodes(t *testing.T) {
	Convey("test successful get codelists from instance", t, func() {
		json := `{  "dimensions":[ {"name":"time", "id":"321-9873"}, {"name":"geo", "id":"84830"}  ]  }`
		mockedImportClient := &testcodelist.ImportClientMock{
			GetFunc: func(ctx context.Context, path string) (*http.Response, error) {
				body := iOReadCloser{strings.NewReader(json)}
				response := http.Response{StatusCode: http.StatusOK, Body: body}
				return &response, nil
			},
		}

		codeLists, err := GetFromInstance(ctx, url, "1234", mockedImportClient)
		So(err, ShouldBeNil)
		So(codeLists["time"], ShouldEqual, "321-9873")
	})
}

func TestGetFromInstanceReturnsErrors(t *testing.T) {
	Convey("test http client triggers an error", t, func() {
		mockedImportClient := &testcodelist.ImportClientMock{
			GetFunc: func(ctx context.Context, path string) (*http.Response, error) {
				return nil, errors.New("http client error")
			},
		}

		_, err := GetFromInstance(context.Background(), url, "1234", mockedImportClient)
		So(err, ShouldNotBeNil)
	})
	Convey("test 404 status code returns an error", t, func() {
		json := `{}`
		mockedImportClient := &testcodelist.ImportClientMock{
			GetFunc: func(ctx context.Context, path string) (*http.Response, error) {
				body := iOReadCloser{strings.NewReader(json)}
				response := http.Response{StatusCode: http.StatusNotFound, Body: body}
				return &response, nil
			},
		}

		_, err := GetFromInstance(ctx, url, "1234", mockedImportClient)
		So(err, ShouldNotBeNil)
	})
}

type iOReadCloser struct {
	io.Reader
}

func (iOReadCloser) Close() error { return nil }
