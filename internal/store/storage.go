package store

// Read-only storage introspection behind the System -> Storage view. Metadata calls
// (schema, aliases, node status, tenants), exact object counts via GraphQL Aggregate
// meta{count}, plus ONE sampled object per vector-bearing class to read its `dims`
// provenance property. Nothing scans objects, and nothing touches Weaviate's
// /debug/usage or profiling port.
//
// Counts use Aggregate, not node status: the node-status objectCount reflects only
// flushed segments (observed 0 for populated classes on 1.38.0). Node status is used
// for indexing status, queue length and node health only.

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"

	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// CountUnknown marks an object count that could not be obtained (node status
// failed). Callers render it as "unavailable".
const CountUnknown int64 = -1

// maxDimsProbes bounds how many tenants are tried when looking for a sample object.
const maxDimsProbes = 10

// countConcurrency bounds parallel per-tenant aggregate queries.
const countConcurrency = 8

// StorageInfo is the plain-data result of Store.StorageInfo.
type StorageInfo struct {
	Classes []ClassStorage
	Orphans []string
	Nodes   []NodeStorage
}

// ClassStorage describes one logical class and the physical class backing it.
type ClassStorage struct {
	Logical, Physical string
	Version           int
	Aliased           bool
	ObjectCount       int64
	MultiTenant       bool
	TenantCount       int
	VectorIndexType   string
	Distance          string
	Vectorizer        string
	StoredDims        int
	PropertyCount     int
	ReplicationFactor int64
	IndexingStatus    string
	VectorQueueLength int64
	Tenants           []TenantStorage
}

// TenantStorage is one tenant (= one shard) of a multi-tenant class.
type TenantStorage struct {
	Tenant         string
	Activity       string
	ObjectCount    int64
	IndexingStatus string
	QueueLength    int64
}

// NodeStorage is one Weaviate node.
type NodeStorage struct {
	Name, Version, GitHash, Status, OperationalMode string
	ShardCount                                      int64
}

// shardAgg is the aggregate of every shard of one physical class (or one tenant).
type shardAgg struct {
	count  int64
	queue  int64
	status string
}

// classAgg aggregates a physical class: totals plus per-shard-name (= tenant in
// MT) figures.
type classAgg struct {
	shardAgg
	byShard map[string]*shardAgg
}

// storageLogicalClasses is the fixed display order of the classes Cortex owns.
func storageLogicalClasses() []string {
	return append(MemoryClasses(), memory.UserClassName, memory.ApiKeyClassName, memory.MetaClassName)
}

// hasVectors reports whether a logical class stores vectors (so `dims` is meaningful).
func hasVectors(logical string) bool {
	switch logical {
	case memory.ClassName, memory.ChunkClassName, memory.SummaryClassName:
		return true
	}
	return false
}

// statusRank orders vector-indexing states by how much an operator should care.
func statusRank(s string) int {
	switch s {
	case "INDEXING":
		return 3
	case "READONLY":
		return 2
	case "READY":
		return 1
	}
	return 0
}

// worseStatus returns the more alarming of two indexing statuses
// (INDEXING > READONLY > READY > anything else/empty).
func worseStatus(a, b string) string {
	if statusRank(b) > statusRank(a) {
		return b
	}
	return a
}

// aggregateShards groups verbose node status into per-physical-class and
// per-shard figures. A shard seen on several nodes is a replica of the same data:
// its object count is taken once (the max, replicas lag each other), while queue
// length is summed and the status is the worst, so a stuck replica still shows.
func aggregateShards(nodes []*models.NodeStatus) map[string]*classAgg {
	out := map[string]*classAgg{}
	type seen struct{ class, shard string }
	replicaCount := map[seen]int64{}
	for _, n := range nodes {
		if n == nil {
			continue
		}
		for _, sh := range n.Shards {
			if sh == nil {
				continue
			}
			c := out[sh.Class]
			if c == nil {
				c = &classAgg{byShard: map[string]*shardAgg{}}
				out[sh.Class] = c
			}
			s := c.byShard[sh.Name]
			if s == nil {
				s = &shardAgg{}
				c.byShard[sh.Name] = s
			}
			s.queue += sh.VectorQueueLength
			s.status = worseStatus(s.status, sh.VectorIndexingStatus)
			c.queue += sh.VectorQueueLength
			c.status = worseStatus(c.status, sh.VectorIndexingStatus)
			k := seen{sh.Class, sh.Name}
			if sh.ObjectCount > replicaCount[k] {
				replicaCount[k] = sh.ObjectCount
			}
		}
	}
	for k, n := range replicaCount {
		out[k.class].byShard[k.shard].count = n
		out[k.class].count += n
	}
	return out
}

// findOrphans returns every schema class that is neither the target of an alias
// nor a legacy plain class of a known logical name: leftovers of an interrupted
// migration (<Name>_v<N>) and any class Cortex does not know. Sorted.
func findOrphans(classNames []string, targets map[string]string, known []string) []string {
	live := map[string]bool{}
	for _, phys := range targets {
		live[phys] = true
	}
	var out []string
	for _, n := range classNames {
		if live[n] {
			continue
		}
		if _, isAlias := targets[n]; isAlias {
			continue
		}
		if slices.Contains(known, n) {
			continue
		}
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// vectorSettings extracts index type, distance metric and vectorizer from a class
// definition. Classes with named vectors carry them per vector.
func vectorSettings(c *models.Class) (indexType, distance, vectorizer string) {
	indexType, vectorizer = c.VectorIndexType, c.Vectorizer
	cfg := c.VectorIndexConfig
	if indexType == "" && len(c.VectorConfig) > 0 {
		names := make([]string, 0, len(c.VectorConfig))
		for n := range c.VectorConfig {
			names = append(names, n)
		}
		sort.Strings(names)
		vc := c.VectorConfig[names[0]]
		indexType, cfg = vc.VectorIndexType, vc.VectorIndexConfig
		if m, ok := vc.Vectorizer.(map[string]any); ok && len(m) == 1 {
			for k := range m {
				vectorizer = k
			}
		}
	}
	return indexType, distanceOf(cfg), vectorizer
}

// distanceOf digs the distance metric out of a vector index config. Dynamic
// indexes nest it under hnsw/flat.
func distanceOf(cfg any) string {
	m, ok := cfg.(map[string]any)
	if !ok {
		return ""
	}
	if d, ok := m["distance"].(string); ok {
		return d
	}
	for _, sub := range []string{"hnsw", "flat"} {
		if d := distanceOf(m[sub]); d != "" {
			return d
		}
	}
	return ""
}

// StorageInfo gathers the physical picture of the store. onlyTenant, when set,
// restricts every per-tenant listing, count and dims sample of multi-tenant classes
// to that one tenant (a scoped caller must not pay for, or touch, other shards);
// counts of non-MT classes are then skipped (CountUnknown) as they are global. Only a failing schema
// call fails the whole call; node status, tenant listing and dims sampling degrade
// independently (counts become CountUnknown, tenants/dims stay empty).
func (s *Store) StorageInfo(ctx context.Context, onlyTenant string) (*StorageInfo, error) {
	sch, err := s.client.Schema().Getter().Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("get schema: %w", err)
	}
	targets, err := s.aliasTargets(ctx)
	if err != nil {
		return nil, err
	}
	byName := make(map[string]*models.Class, len(sch.Classes))
	names := make([]string, 0, len(sch.Classes))
	for _, c := range sch.Classes {
		byName[c.Class] = c
		names = append(names, c.Class)
	}

	var aggs map[string]*classAgg
	out := &StorageInfo{Orphans: findOrphans(names, targets, storageLogicalClasses())}
	// Verbose node status lists EVERY shard of every tenant; a scoped caller would
	// pay for the whole cluster to read one shard's status/queue, so skip it.
	if onlyTenant == "" {
		if st, nerr := s.client.Cluster().NodesStatusGetter().WithOutput("verbose").Do(ctx); nerr == nil && st != nil {
			aggs = aggregateShards(st.Nodes)
			for _, n := range st.Nodes {
				ns := NodeStorage{Name: n.Name, Version: n.Version, GitHash: n.GitHash, OperationalMode: n.OperationalMode, ShardCount: int64(len(n.Shards))}
				if n.Status != nil {
					ns.Status = *n.Status
				}
				out.Nodes = append(out.Nodes, ns)
			}
		}
	}

	for _, logical := range storageLogicalClasses() {
		phys, aliased := targets[logical]
		if !aliased {
			phys = logical
		}
		cls := byName[phys]
		if cls == nil {
			continue
		}
		cs := ClassStorage{
			Logical: logical, Physical: phys, Aliased: aliased,
			ObjectCount:   CountUnknown,
			PropertyCount: len(cls.Properties),
			MultiTenant:   cls.MultiTenancyConfig != nil && cls.MultiTenancyConfig.Enabled,
		}
		if v, ok := versionOf(logical, phys); ok {
			cs.Version = v
		}
		cs.VectorIndexType, cs.Distance, cs.Vectorizer = vectorSettings(cls)
		if cls.ReplicationConfig != nil {
			cs.ReplicationFactor = cls.ReplicationConfig.Factor
		}
		agg := aggs[phys]
		if agg != nil {
			cs.VectorQueueLength, cs.IndexingStatus = agg.queue, agg.status
		}
		if cs.MultiTenant {
			cs.Tenants = s.storageTenants(ctx, phys, agg, onlyTenant)
			cs.TenantCount = len(cs.Tenants)
			cs.ObjectCount = sumCounts(cs.Tenants)
		} else if onlyTenant == "" {
			cs.ObjectCount = s.aggregateCount(ctx, phys, "")
		}
		if hasVectors(logical) {
			cs.StoredDims = s.sampleDims(ctx, logical, cs)
		}
		out.Classes = append(out.Classes, cs)
	}
	return out, nil
}

// sumCounts totals tenant counts, or CountUnknown if any tenant's count is unknown
// (a partial sum would read as a real total).
func sumCounts(ts []TenantStorage) int64 {
	var total int64
	for _, t := range ts {
		if t.ObjectCount < 0 {
			return CountUnknown
		}
		total += t.ObjectCount
	}
	return total
}

// aggregateCount is the exact object count of a class (one tenant if non-empty)
// via Aggregate meta{count}; CountUnknown on any error.
func (s *Store) aggregateCount(ctx context.Context, class, tenant string) int64 {
	q := s.client.GraphQL().Aggregate().WithClassName(class).
		WithFields(graphql.Field{Name: "meta", Fields: []graphql.Field{{Name: "count"}}})
	if tenant != "" {
		q = q.WithTenant(tenant)
	}
	res, err := q.Do(ctx)
	if err != nil || res == nil || len(res.Errors) > 0 {
		return CountUnknown
	}
	return countFromAggregate(res.Data, class)
}

// countFromAggregate digs meta.count out of {"Aggregate": {class: [{"meta": {"count": n}}]}}.
func countFromAggregate(data map[string]models.JSONObject, class string) int64 {
	agg, _ := data["Aggregate"].(map[string]any)
	rows, _ := agg[class].([]any)
	if len(rows) == 0 {
		return CountUnknown
	}
	row, _ := rows[0].(map[string]any)
	meta, _ := row["meta"].(map[string]any)
	n, ok := meta["count"].(float64)
	if !ok {
		return CountUnknown
	}
	return int64(n)
}

// storageTenants merges the tenant listing (activity) with the shard figures
// (status, queue), so a tenant known to only one source is still shown, and fills
// exact per-tenant counts from Aggregate with bounded concurrency.
func (s *Store) storageTenants(ctx context.Context, phys string, agg *classAgg, onlyTenant string) []TenantStorage {
	if onlyTenant != "" {
		return s.scopedTenant(ctx, phys, onlyTenant)
	}
	byTenant := map[string]*TenantStorage{}
	if list, err := s.client.Schema().TenantsGetter().WithClassName(phys).Do(ctx); err == nil {
		for _, t := range list {
			byTenant[t.Name] = &TenantStorage{Tenant: t.Name, Activity: t.ActivityStatus}
		}
	}
	if agg != nil {
		for name, sh := range agg.byShard {
			ts := byTenant[name]
			if ts == nil {
				ts = &TenantStorage{Tenant: name}
				byTenant[name] = ts
			}
			ts.IndexingStatus, ts.QueueLength = sh.status, sh.queue
		}
	}
	out := make([]TenantStorage, 0, len(byTenant))
	for name, ts := range byTenant {
		if onlyTenant != "" && name != onlyTenant {
			continue
		}
		out = append(out, *ts)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Tenant < out[j].Tenant })

	sem := make(chan struct{}, countConcurrency)
	var wg sync.WaitGroup
	for i := range out {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			// Cancelled: stop launching; the rest stay CountUnknown.
			for j := i; j < len(out); j++ {
				out[j].ObjectCount = CountUnknown
			}
			wg.Wait()
			return out
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			out[i].ObjectCount = s.aggregateCount(ctx, phys, out[i].Tenant)
		}()
	}
	wg.Wait()
	return out
}

// scopedTenant is storageTenants for one caller at constant cost: an existence
// check instead of listing every tenant of the class (the client has no
// single-tenant getter, so activity is left empty), then one Aggregate.
// Exists failing → the tenant is shown with an unknown count, not hidden.
func (s *Store) scopedTenant(ctx context.Context, phys, tenant string) []TenantStorage {
	ok, err := s.client.Schema().TenantsExists().WithClassName(phys).WithTenant(tenant).Do(ctx)
	if err != nil {
		return []TenantStorage{{Tenant: tenant, ObjectCount: CountUnknown}}
	}
	if !ok {
		return nil
	}
	return []TenantStorage{{Tenant: tenant, ObjectCount: s.aggregateCount(ctx, phys, tenant)}}
}

// sampleDims reads the `dims` provenance property of ONE object (no vector is
// fetched). A vector index holds a single dimensionality, so one sample is
// representative. MT: tenants with the most objects first (counts lag, so none is ruled out on a zero), bounded number of probes; tenants
// that cannot be read (cold) are skipped. 0 = empty class / unknown.
func (s *Store) sampleDims(ctx context.Context, logical string, cs ClassStorage) int {
	if !cs.MultiTenant {
		return s.dimsOf(ctx, logical, "")
	}
	tenants := slices.Clone(cs.Tenants)
	sort.SliceStable(tenants, func(i, j int) bool { return tenants[i].ObjectCount > tenants[j].ObjectCount })
	for i, t := range tenants {
		if i >= maxDimsProbes {
			break
		}
		if d := s.dimsOf(ctx, logical, t.Tenant); d > 0 {
			return d
		}
	}
	return 0
}

func (s *Store) dimsOf(ctx context.Context, logical, tenant string) int {
	q := s.client.Experimental().Search().
		WithCollection(logical).
		WithProperties("dims").
		WithLimit(1)
	if tenant != "" {
		q = q.WithTenant(tenant)
	}
	res, err := q.Do(ctx)
	if err != nil || len(res) == 0 {
		return 0
	}
	return propInt(res[0].Properties, "dims")
}
