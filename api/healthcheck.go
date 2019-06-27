package api

import (
	"net/http"

)

// HealthCheck returns the health of the application.
func (api *DimensionExtractorAPI) healthcheck(w http.ResponseWriter, req *http.Request) {

	// Check health
	h := <- api.healthChan
	api.healthChan <- h

	var code int
	if h {
		code = 200
	} else {
		code = 429
	}

	w.WriteHeader(code)
	w.Write([]byte{})
}
