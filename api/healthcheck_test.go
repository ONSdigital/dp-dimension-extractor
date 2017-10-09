package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

const host = "80"

func TestHealthCheckReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21400/healthcheck", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()

		router := mux.NewRouter()
		api := routes(host, router)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
	})
}
