package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The reserved `preference` tag must not be confirmable by the same generic
// "yes" used for ordinary tags: it needs --yes or typing the tag name.
func TestConfirmTagChange(t *testing.T) {
	run := func(input string, yes, generic bool, tags ...string) (bool, string, error) {
		var out strings.Builder
		c, err := confirmTagChange(strings.NewReader(input), &out, yes, generic, tags...)
		return c, out.String(), err
	}

	_, _, err := run("yes\n", false, true, "preference")
	assert.EqualError(t, err, "aborted", `generic "yes" must not confirm preference`)

	c, out, err := run("preference\n", false, true, "Preference")
	require.NoError(t, err)
	assert.True(t, c)
	assert.Contains(t, out, "standing preferences injected into EVERY session")

	_, _, err = run("yes\n", false, false, "a", "PREFERENCE") // rename INTO it
	assert.Error(t, err)

	c, _, err = run("", true, true, "preference") // --yes
	require.NoError(t, err)
	assert.True(t, c)

	c, _, err = run("yes\n", false, true, "k8s")
	require.NoError(t, err)
	assert.False(t, c, "ordinary tags never send the preference confirmation")
	_, _, err = run("no\n", false, true, "k8s")
	assert.Error(t, err)

	c, _, err = run("", false, false, "a", "b") // plain rename: no prompt
	require.NoError(t, err)
	assert.False(t, c)
}
