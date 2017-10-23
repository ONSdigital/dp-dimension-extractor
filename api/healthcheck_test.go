package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/ONSdigital/dp-dimension-extractor/config"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

const host = ":80"

func TestHealthCheckReturnsOK(t *testing.T) {
	cfg, err := config.Get()
	if err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}

	path := cfg.DimensionExtractorURL + "/healthcheck"

	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", path, nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()

		router := mux.NewRouter()
		api := routes(host, router)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
	})
}
