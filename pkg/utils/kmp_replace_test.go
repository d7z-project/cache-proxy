package utils

import (
	"github.com/stretchr/testify/assert"
	"io"
	"strings"
	"testing"
)

func TestReplace(t *testing.T) {
	assert.Equal(t, "efghdeefgh", testLocal("abcdeabc", "abc", "efgh"))
}

func testLocal(src, old, new string) string {
	reader := io.NopCloser(strings.NewReader(src))
	all, err := io.ReadAll(NewKMPReplaceReader(reader, []byte(old), []byte(new)))
	if err != nil {
		return ""
	}
	return string(all)
}
