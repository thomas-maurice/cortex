package main

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
)

func sampleMemory() *cortexv1.Memory {
	return &cortexv1.Memory{
		Id: "id-1", Text: "hello", Namespace: "ns", Tags: []string{"a", "b"},
		LinkedIds: []string{"id-2", "id-3"},
		CreatedAt: timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)),
	}
}

// `get` exists to follow links, so the text form must show the linked ids —
// printMemory alone (used by list/search) omits them.
func TestPrintGotMemoryTextShowsLinks(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, printGotMemory(&buf, sampleMemory(), false))
	out := buf.String()
	assert.Contains(t, out, "[ns] hello  #a #b")
	assert.Contains(t, out, "id=id-1")
	assert.Contains(t, out, "links=id-2,id-3")
}

// --json shares the `cortex export` record shape so output is scriptable and
// re-importable.
func TestPrintGotMemoryJSONMatchesExportShape(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, printGotMemory(&buf, sampleMemory(), true))
	var rec exportRecord
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	assert.Equal(t, "id-1", rec.ID)
	assert.Equal(t, "hello", rec.Text)
	assert.Equal(t, []string{"id-2", "id-3"}, rec.LinkedIDs)
	assert.Equal(t, "2026-01-02T03:04:05Z", rec.CreatedAt)
}

func TestGetCmdRequiresExactlyOneArg(t *testing.T) {
	cmd := getCmd()
	assert.Error(t, cmd.Args(cmd, nil))
	assert.Error(t, cmd.Args(cmd, []string{"a", "b"}))
	assert.NoError(t, cmd.Args(cmd, []string{"a"}))
}
