package utils

import (
	"github.com/stretchr/testify/assert"
	"io"
	"strings"
	"testing"
)

func TestReplace(t *testing.T) {
	testLocal(t, "abcdeabc", "abc", "efgh")
}

func testLocal(t *testing.T, src, old, new string) {
	reader := io.NopCloser(strings.NewReader(src))
	all, err := io.ReadAll(NewKMPReplaceReader(reader, []byte(old), []byte(new)))
	assert.NoError(t, err)
	assert.Equal(t, strings.ReplaceAll(src, old, new), string(all))
}
