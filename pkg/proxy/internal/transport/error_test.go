package transport

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteErrorHidesFailureDetails(t *testing.T) {
	recorder := httptest.NewRecorder()
	WriteError(recorder, http.StatusBadGateway)
	require.Equal(t, http.StatusBadGateway, recorder.Code)
	require.Equal(t, "internal error\n", recorder.Body.String())
	require.Equal(t, "ERROR", recorder.Header().Get("X-Cache"))
}
