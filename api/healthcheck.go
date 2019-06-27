package api

import (
	"net/http"

)

// HealthCheck returns the health of the application.
func (api *DimensionExtractorAPI) healthcheck(w http.ResponseWriter, req *http.Request) {

	// Get health
	h := <- api.healthChan
	api.healthChan <- h // Put it back for next time :)

	var code int
	if h {
		code = 200
	} else {
		code = 429
	}

	w.WriteHeader(code)
	w.Write([]byte{})
}
