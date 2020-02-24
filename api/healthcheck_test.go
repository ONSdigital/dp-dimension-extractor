package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/ONSdigital/dp-dimension-extractor/config"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

const host = ":80"

func TestHealthCheckReturnsOK(t *testing.T) {
	cfg, err := config.Get()
	if err != nil {
		log.Event(nil, "config error", log.ERROR, log.Error(err))
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
