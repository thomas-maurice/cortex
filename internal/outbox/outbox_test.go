package outbox

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/recallstate"
)

func dbPath(t *testing.T) string { return filepath.Join(t.TempDir(), "outbox.db") }

func ids(es []Entry) []string {
	var out []string
	for _, e := range es {
		out = append(out, e.ID)
	}
	return out
}

func TestMissingDBReadsEmpty(t *testing.T) {
	p := dbPath(t)
	all, err := ListAll(p)
	require.NoError(t, err)
	assert.Empty(t, all)
	_, ok, err := Get(p, "x")
	require.NoError(t, err)
	assert.False(t, ok)
	_, ok, err = Delete(p, "x")
	require.NoError(t, err)
	assert.False(t, ok)
	st, err := ReadStats(p)
	require.NoError(t, err)
	assert.Equal(t, 0, st.Total)
	assert.NoFileExists(t, p, "reads must not create the db")
}

func TestSurvivesReopenAndSaveRequestRoundTrip(t *testing.T) {
	p := dbPath(t)
	req := &cortexv1.SaveRequest{
		Id: proto.String("0b6d1f0e-52a4-4a53-9e55-7d2d8f6f3c11"), Text: "hello", Namespace: "ns", Tags: []string{"a", "b"}, ConversationId: "c1",
		LinkTo: []string{"l1"}, Supersedes: []string{"s1"}, NeverExpires: true,
		ValidUntil: timestamppb.New(time.Unix(2000000000, 0)),
		CreatedAt:  timestamppb.New(time.Unix(1700000000, 0)),
	}
	raw, err := proto.Marshal(req)
	require.NoError(t, err)
	require.NoError(t, Enqueue(p, Entry{ID: "id1", Request: raw, ConversationID: "c1", Preview: PreviewOf(req.Text)}))

	// Every op closes the db, so this is a genuine reopen from disk.
	got, ok, err := Get(p, "id1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.NotZero(t, got.EnqueuedAt, "enqueue time defaults to now")
	var back cortexv1.SaveRequest
	require.NoError(t, proto.Unmarshal(got.Request, &back))
	assert.True(t, proto.Equal(req, &back), "replayed verbatim")
}

func TestListOrderIsEnqueueOrderNotKeyOrder(t *testing.T) {
	p := dbPath(t)
	require.NoError(t, Enqueue(p, Entry{ID: "z", EnqueuedAt: 1}))
	require.NoError(t, Enqueue(p, Entry{ID: "a", EnqueuedAt: 3}))
	require.NoError(t, Enqueue(p, Entry{ID: "m", EnqueuedAt: 2}))
	all, err := ListAll(p)
	require.NoError(t, err)
	assert.Equal(t, []string{"z", "m", "a"}, ids(all))
}

func TestPartitionByConversationAndStats(t *testing.T) {
	p := dbPath(t)
	require.NoError(t, Enqueue(p, Entry{ID: "1", ConversationID: "A", EnqueuedAt: 20}))
	require.NoError(t, Enqueue(p, Entry{ID: "2", ConversationID: "B", EnqueuedAt: 10}))
	require.NoError(t, Enqueue(p, Entry{ID: "3", ConversationID: "A", EnqueuedAt: 30}))
	a, err := ListByConversation(p, "A")
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "3"}, ids(a))
	st, err := ReadStats(p)
	require.NoError(t, err)
	assert.Equal(t, 3, st.Total)
	assert.Equal(t, int64(10), st.OldestEnqueuedAt)
	assert.Equal(t, map[string]int{"A": 2, "B": 1}, st.ByConversation)
}

func TestReEnqueueReplaces(t *testing.T) {
	p := dbPath(t)
	require.NoError(t, Enqueue(p, Entry{ID: "x", Preview: "old", EnqueuedAt: 1}))
	require.NoError(t, Enqueue(p, Entry{ID: "x", Preview: "new", EnqueuedAt: 2}))
	all, err := ListAll(p)
	require.NoError(t, err)
	require.Len(t, all, 1)
	assert.Equal(t, "new", all[0].Preview)
}

func TestDeleteReturnsRemovedEntryOnly(t *testing.T) {
	p := dbPath(t)
	require.NoError(t, Enqueue(p, Entry{ID: "x", Preview: "px"}))
	require.NoError(t, Enqueue(p, Entry{ID: "y"}))
	got, ok, err := Delete(p, "x")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "px", got.Preview)
	_, ok, err = Delete(p, "x")
	require.NoError(t, err)
	assert.False(t, ok, "missing id is a no-op")
	all, _ := ListAll(p)
	assert.Equal(t, []string{"y"}, ids(all))
}

func TestCapRejectsInsteadOfEvicting(t *testing.T) {
	old := MaxEntries
	MaxEntries = 2
	t.Cleanup(func() { MaxEntries = old })
	p := dbPath(t)
	require.NoError(t, Enqueue(p, Entry{ID: "a"}))
	require.NoError(t, Enqueue(p, Entry{ID: "b"}))
	assert.ErrorIs(t, Enqueue(p, Entry{ID: "c"}), ErrFull)
	// Replacing an existing id at the cap must still work.
	require.NoError(t, Enqueue(p, Entry{ID: "a", Preview: "again"}))
	all, _ := ListAll(p)
	assert.ElementsMatch(t, []string{"a", "b"}, ids(all), "nothing evicted")
}

func TestConcurrentEnqueueBothLand(t *testing.T) {
	p := dbPath(t)
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				errs <- Enqueue(p, Entry{ID: fmt.Sprintf("g%d-%d", g, i)})
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	all, err := ListAll(p)
	require.NoError(t, err)
	assert.Len(t, all, 20)
}

func TestPreviewTruncatesRunes(t *testing.T) {
	long := ""
	for i := 0; i < 200; i++ {
		long += "é"
	}
	assert.Equal(t, 120, len([]rune(PreviewOf(long))))
	assert.Equal(t, "short", PreviewOf("short"))
}

// recallstate.pruneTx deletes every root bucket without a fresh lastSeen, so
// sharing its file would lose queued saves.
func TestPathIsNotRecallstateFileButSameDir(t *testing.T) {
	assert.NotEqual(t, recallstate.DefaultPath(), DefaultPath())
	assert.Equal(t, filepath.Dir(recallstate.DefaultPath()), filepath.Dir(DefaultPath()))
}
