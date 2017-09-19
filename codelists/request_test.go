package codelists

import (
	"errors"
	"github.com/ONSdigital/dp-dimension-extractor/codelists/testcodelist"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestGetFromInstanceReturnsCodes(t *testing.T) {
	Convey("test successful get codelists from instance", t, func() {
		json := `{  "dimensions":[ {"name":"time", "id":"321-9873"}, {"name":"geo", "id":"84830"}  ]  }`
		mockedImportClient := &testcodelist.ImportClientMock{
			GetFunc: func(path string) (*http.Response, error) {
				body := iOReadCloser{strings.NewReader(json)}
				response := http.Response{StatusCode: http.StatusOK, Body: body}
				return &response, nil
			},
		}

		codeLists, err := GetFromInstance("http://localhost:22000", "1234", mockedImportClient)
		So(err, ShouldBeNil)
		So(codeLists["time"], ShouldEqual, "321-9873")
	})
}

func TestGetFromInstanceReturnsErrors(t *testing.T) {
	Convey("test http client triggers an error", t, func() {
		mockedImportClient := &testcodelist.ImportClientMock{
			GetFunc: func(path string) (*http.Response, error) {
				return nil, errors.New("http client error")
			},
		}

		_, err := GetFromInstance("http://localhost:22000", "1234", mockedImportClient)
		So(err, ShouldNotBeNil)
	})
	Convey("test 404 status code returns an error", t, func() {
		json := `{}`
		mockedImportClient := &testcodelist.ImportClientMock{
			GetFunc: func(path string) (*http.Response, error) {
				body := iOReadCloser{strings.NewReader(json)}
				response := http.Response{StatusCode: http.StatusNotFound, Body: body}
				return &response, nil
			},
		}

		_, err := GetFromInstance("http://localhost:22000", "1234", mockedImportClient)
		So(err, ShouldNotBeNil)
	})
}

type iOReadCloser struct {
	io.Reader
}

func (iOReadCloser) Close() error { return nil }
