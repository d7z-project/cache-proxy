package signedtoken

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSignVerifyRoundTrip(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
	}
	token, err := Sign([]byte("secret"), payload{Name: "package"})
	require.NoError(t, err)

	var decoded payload
	require.NoError(t, Verify([]byte("secret"), token, 1024, &decoded))
	require.Equal(t, "package", decoded.Name)
	require.ErrorIs(t, Verify([]byte("other"), token, 1024, &decoded), ErrInvalid)
	require.ErrorIs(t, Verify([]byte("secret"), token, 1, &decoded), ErrInvalid)
}

func FuzzVerify(f *testing.F) {
	valid, err := Sign([]byte("secret"), struct {
		Value string `json:"value"`
	}{Value: "seed"})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add("invalid")
	f.Fuzz(func(t *testing.T, token string) {
		if len(token) > 64<<10 {
			t.Skip()
		}
		var target struct {
			Value string `json:"value"`
		}
		_ = Verify([]byte("secret"), token, 16<<10, &target)
	})
}
