package rpc

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/memory"
)

// protoToRecord maps a wire memory back to an internal record. It is the inverse
// of recordToProto, used by RestoreMemories to re-ingest a dump. DupCandidates
// are intentionally dropped — the worker recomputes them on (re)index. Model and
// Dims are carried for provenance but the worker re-stamps them on re-embed.
func protoToRecord(m *cortexv1.Memory) memory.Record {
	var created time.Time
	if ts := m.GetCreatedAt(); ts != nil {
		created = ts.AsTime()
	}
	return memory.Record{
		ID:             m.GetId(),
		Text:           m.GetText(),
		Namespace:      m.GetNamespace(),
		Tags:           m.GetTags(),
		Source:         m.GetSource(),
		CreatedAt:      created,
		Model:          m.GetModel(),
		Dims:           int(m.GetDims()),
		ConversationID: m.GetConversationId(),
		LinkedIDs:      m.GetLinkedIds(),
		NotDuplicateOf: m.GetNotDuplicateOf(),
	}
}

// recordToProto maps an internal memory record to its wire form.
func recordToProto(r memory.Record) *cortexv1.Memory {
	return &cortexv1.Memory{
		Id:             r.ID,
		Text:           r.Text,
		Namespace:      r.Namespace,
		Tags:           r.Tags,
		Source:         r.Source,
		CreatedAt:      timestamppb.New(r.CreatedAt),
		Model:          r.Model,
		Dims:           int32(r.Dims),
		ConversationId: r.ConversationID,
		LinkedIds:      r.LinkedIDs,
		DupCandidates:  r.DupCandidates,
		NotDuplicateOf: r.NotDuplicateOf,
	}
}

// hitToProto maps a search hit (record + distance) to its wire form.
func hitToProto(h memory.Hit) *cortexv1.Hit {
	return &cortexv1.Hit{
		Memory:   recordToProto(h.Record),
		Distance: h.Distance,
	}
}

// deadToProto maps a dead-lettered record to its wire form.
func deadToProto(dl memory.DeadLetter) *cortexv1.DeadLetter {
	return &cortexv1.DeadLetter{
		Record:     recordToProto(dl.Record),
		Error:      dl.Error,
		Deliveries: int32(dl.Deliveries),
		FailedAt:   timestamppb.New(dl.FailedAt),
	}
}

// summaryToProto maps a conversation summary (+ its recall distance) to wire form.
func summaryToProto(h memory.SummaryHit) *cortexv1.ConversationSummary {
	return &cortexv1.ConversationSummary{
		ConversationId: h.ConversationID,
		Text:           h.Text,
		Namespace:      h.Namespace,
		Source:         h.Source,
		CreatedAt:      timestamppb.New(h.CreatedAt),
		UpdatedAt:      timestamppb.New(h.UpdatedAt),
		Model:          h.Model,
		Dims:           int32(h.Dims),
		Distance:       h.Distance,
	}
}

// resolveNamespace maps a request namespace to a store filter: "" -> the
// server default, "*" -> all namespaces (no filter), anything else verbatim.
func resolveNamespace(reqNS, defaultNS string) string {
	switch reqNS {
	case "":
		return defaultNS
	case "*":
		return ""
	default:
		return reqNS
	}
}
