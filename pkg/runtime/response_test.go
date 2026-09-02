package runtime

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequireReadMethod(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		recorder := httptest.NewRecorder()
		require.True(t, RequireReadMethod(recorder, method))
		require.Equal(t, http.StatusOK, recorder.Code)
	}
	recorder := httptest.NewRecorder()
	require.False(t, RequireReadMethod(recorder, http.MethodPut))
	require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
	require.Equal(t, "GET, HEAD", recorder.Header().Get("Allow"))
	require.Equal(t, "REJECTED", recorder.Header().Get("X-Cache"))
}
