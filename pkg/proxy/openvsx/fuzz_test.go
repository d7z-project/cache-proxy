package openvsx

import (
	"bytes"
	"encoding/json"
	"net/url"
	"strings"
	"testing"
)

func FuzzRewriteDocuments(f *testing.F) {
	f.Add(byte(0), []byte(`{"url":"https://open-vsx.org/api/test"}`))
	f.Add(byte(1), []byte(`<html><script src="https://open-vsx.org/bundle.js"></script></html>`))
	f.Add(byte(0), []byte(strings.Repeat("[", maxJSONDepth+1)+"0"+strings.Repeat("]", maxJSONDepth+1)))
	origin, _ := url.Parse("https://open-vsx.org")
	publicBase, _ := url.Parse("https://openvsx.proxy.test")
	f.Fuzz(func(t *testing.T, kind byte, input []byte) {
		if len(input) > 64<<10 {
			t.Skip()
		}
		var output bytes.Buffer
		if kind%2 == 0 {
			err := rewriteJSON(bytes.NewReader(input), &output, origin, publicBase)
			if err == nil && !json.Valid(output.Bytes()) {
				t.Fatal("JSON rewrite produced invalid JSON")
			}
			return
		}
		_ = rewriteHTML(bytes.NewReader(input), &output, origin, publicBase)
	})
}
