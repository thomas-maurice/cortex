package bus

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/natstest"
)

// Why: the worker's stale-write guard trusts IndexVersion to mean "when this was
// published". A requeue or restore of an old payload (dead letter, reindex snapshot,
// trash restore) carries a stale producer value; if the bus kept it, that republish
// would be skipped as stale instead of applied. The bus must overwrite it.
func TestPublishStampsFreshIndexVersion(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	require.NoError(t, EnsureStream(ctx, js))
	st, err := js.Stream(ctx, memory.StreamName)
	require.NoError(t, err)

	const old = int64(1)
	before := time.Now().UnixMicro()

	require.NoError(t, PublishIndex(ctx, js, memory.Record{ID: "a", Text: "t", IndexVersion: old}))
	raw, err := st.GetLastMsgForSubject(ctx, memory.SubjectIndex)
	require.NoError(t, err)
	var rec memory.Record
	require.NoError(t, json.Unmarshal(raw.Data, &rec))
	assert.GreaterOrEqual(t, rec.IndexVersion, before, "producer-set record version must be overwritten with now")

	require.NoError(t, PublishSummary(ctx, js, memory.Summary{ConversationID: "c", Text: "t", IndexVersion: old}))
	raw, err = st.GetLastMsgForSubject(ctx, memory.SubjectSummary)
	require.NoError(t, err)
	var sum memory.Summary
	require.NoError(t, json.Unmarshal(raw.Data, &sum))
	assert.GreaterOrEqual(t, sum.IndexVersion, before, "producer-set summary version must be overwritten with now")
}
