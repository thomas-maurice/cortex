package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// copyPageSize is the cursor page / batch-insert size of CopyClassWithVectors. A
// var so tests can shrink it and exercise multi-page copies cheaply.
var copyPageSize = 200

// supportedCopyTypes are the property data types CopyClassWithVectors can carry
// across. Cortex uses text, text[], int, date; the rest are decoded too so a
// future property cannot be silently dropped. Anything else (object, geo, blob,
// phone, cross-refs) makes the copy refuse up front rather than lose data.
var supportedCopyTypes = map[string]bool{
	"text": true, "text[]": true, "int": true, "int[]": true,
	"number": true, "number[]": true, "boolean": true, "boolean[]": true,
	"date": true, "date[]": true, "uuid": true, "uuid[]": true,
}

// CopyClassWithVectors copies every object of class `from` (a logical alias name
// or a physical class) into the PHYSICAL class `to`, preserving ids, properties and
// the stored vector bit for bit, so nothing is re-embedded. Multi-tenant classes
// are copied tenant by tenant (tenants are pre-created on `to`, so empty ones
// survive too); single-tenant classes as one unit. transform, when non-nil, may
// rewrite each object's properties (keys are the source property names).
//
// It is idempotent by object id (batch insert overwrites), hence resumable after a
// crash: just call it again. When it returns nil the id set of every tenant of `to`
// equals the source's. The returned map is tenant -> objects copied ("" for a
// single-tenant class).
//
// The caller must stop writers (maintenance mode) for the copy to be a faithful
// snapshot: an object written to `from` after its tenant was scanned is not carried.
func (s *Store) CopyClassWithVectors(ctx context.Context, from, to string, transform func(map[string]any) map[string]any) (map[string]int, error) {
	src, err := s.client.Schema().ClassGetter().WithClassName(from).Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("get source class %s: %w", from, err)
	}
	dst, err := s.client.Schema().ClassGetter().WithClassName(to).Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("get destination class %s: %w", to, err)
	}
	if dst.Class != to {
		return nil, fmt.Errorf("destination %s is not a physical class (resolves to %s)", to, dst.Class)
	}
	props := make([]string, 0, len(src.Properties))
	for _, p := range src.Properties {
		if len(p.DataType) != 1 || !supportedCopyTypes[p.DataType[0]] {
			return nil, fmt.Errorf("class %s property %q has data type %v which the copy cannot carry", from, p.Name, p.DataType)
		}
		props = append(props, p.Name)
	}

	srcMT := src.MultiTenancyConfig != nil && src.MultiTenancyConfig.Enabled
	dstMT := dst.MultiTenancyConfig != nil && dst.MultiTenancyConfig.Enabled
	if srcMT != dstMT {
		return nil, fmt.Errorf("multi-tenancy differs: %s=%v %s=%v", from, srcMT, to, dstMT)
	}

	tenants := []string{""}
	if srcMT {
		tl, err := s.client.Schema().TenantsGetter().WithClassName(from).Do(ctx)
		if err != nil {
			return nil, fmt.Errorf("list tenants of %s: %w", from, err)
		}
		tenants = tenants[:0]
		create := make([]models.Tenant, 0, len(tl))
		for _, t := range tl {
			switch t.ActivityStatus {
			case "", models.TenantActivityStatusACTIVE, models.TenantActivityStatusHOT:
			default:
				return nil, fmt.Errorf("tenant %q of %s is %s, not ACTIVE, so its data cannot be read for the migration: activate it (Weaviate tenants API, activityStatus ACTIVE) and restart the server; deactivate it again afterwards if you want", t.Name, from, t.ActivityStatus)
			}
			tenants = append(tenants, t.Name)
			create = append(create, models.Tenant{Name: t.Name})
		}
		sort.Strings(tenants)
		if len(create) > 0 {
			// Idempotent enough for resumes: an already-existing tenant is tolerated.
			if err := s.client.Schema().TenantsCreator().WithClassName(to).WithTenants(create...).Do(ctx); err != nil {
				for _, t := range create {
					if ok, cerr := s.client.Schema().TenantsExists().WithClassName(to).WithTenant(t.Name).Do(ctx); cerr != nil || !ok {
						return nil, fmt.Errorf("create tenants on %s: %w", to, err)
					}
				}
			}
		}
	}

	counts := make(map[string]int, len(tenants))
	for _, tenant := range tenants {
		n, err := s.copyTenant(ctx, from, to, tenant, props, transform)
		if err != nil {
			return nil, fmt.Errorf("copy %s -> %s (tenant %q): %w", from, to, tenant, err)
		}
		if err := s.verifyTenantCopy(ctx, from, to, tenant); err != nil {
			return nil, err
		}
		counts[tenant] = n
	}
	return counts, nil
}

func (s *Store) copyTenant(ctx context.Context, from, to, tenant string, props []string, transform func(map[string]any) map[string]any) (int, error) {
	after, total := "", 0
	for {
		q := s.client.Experimental().Search().
			WithCollection(from).
			WithMetadata(&graphql.Metadata{ID: true, Vector: true}).
			WithProperties(props...).
			WithLimit(copyPageSize)
		if tenant != "" {
			q = q.WithTenant(tenant)
		}
		if after != "" {
			q = q.WithAfter(after)
		}
		page, err := q.Do(ctx)
		if err != nil {
			return total, fmt.Errorf("read page after %q: %w", after, err)
		}
		if len(page) > 0 {
			objs := make([]*models.Object, 0, len(page))
			for _, r := range page {
				if len(r.Vector) == 0 && len(r.Vectors) > 0 {
					return total, fmt.Errorf("object %s has named vectors only; the copy supports the single unnamed vector", r.ID)
				}
				p, err := decodeProps(r.Properties)
				if err != nil {
					return total, fmt.Errorf("object %s: %w", r.ID, err)
				}
				if transform != nil {
					p = transform(p)
				}
				objs = append(objs, &models.Object{
					Class:      to,
					ID:         strfmt.UUID(r.ID),
					Tenant:     tenant,
					Vector:     r.Vector,
					Properties: p,
				})
			}
			if err := s.batchInsert(ctx, objs); err != nil {
				return total, err
			}
			total += len(page)
		}
		if len(page) < copyPageSize {
			return total, nil
		}
		after = page[len(page)-1].ID
	}
}

func (s *Store) batchInsert(ctx context.Context, objs []*models.Object) error {
	res, err := s.client.Batch().ObjectsBatcher().WithObjects(objs...).Do(ctx)
	if err != nil {
		return fmt.Errorf("batch insert: %w", err)
	}
	for _, r := range res {
		if r.Result != nil && r.Result.Errors != nil && len(r.Result.Errors.Error) > 0 {
			return fmt.Errorf("batch insert object %s: %s", r.ID, r.Result.Errors.Error[0].Message)
		}
	}
	return nil
}

// verifyTenantCopy fails unless `to` holds exactly the ids `from` holds for the tenant.
func (s *Store) verifyTenantCopy(ctx context.Context, from, to, tenant string) error {
	srcIDs, err := s.idSet(ctx, from, tenant)
	if err != nil {
		return fmt.Errorf("verify: list %s: %w", from, err)
	}
	dstIDs, err := s.idSet(ctx, to, tenant)
	if err != nil {
		return fmt.Errorf("verify: list %s: %w", to, err)
	}
	var missing []string
	for id := range srcIDs {
		if !dstIDs[id] {
			missing = append(missing, id)
		}
	}
	var extra int
	for id := range dstIDs {
		if !srcIDs[id] {
			extra++
		}
	}
	if len(missing) > 0 || extra > 0 {
		sort.Strings(missing)
		if len(missing) > 3 {
			missing = missing[:3]
		}
		return fmt.Errorf("verify copy %s -> %s tenant %q: source has %d objects, destination %d (missing e.g. %v, %d extra)",
			from, to, tenant, len(srcIDs), len(dstIDs), missing, extra)
	}
	return nil
}

func (s *Store) idSet(ctx context.Context, class, tenant string) (map[string]bool, error) {
	ids := make(map[string]bool)
	err := s.scanAll(ctx, class, tenant, nil, func(r graphql.SearchResult) { ids[r.ID] = true })
	return ids, err
}

// decodeProps converts gRPC search properties into the values a REST/batch insert
// accepts. The gRPC client hands arrays back as *pb.ListValue (packed bytes for
// int/number): leaving one as-is would make the property vanish on insert, so every
// shape is converted explicitly and an unknown one is an error, never a silent drop.
func decodeProps(in map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(in))
	for k, v := range in {
		dv, err := decodeValue(v)
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", k, err)
		}
		if dv == nil {
			continue // absent / null: stays absent
		}
		out[k] = dv
	}
	return out, nil
}

func decodeValue(v any) (any, error) {
	switch x := v.(type) {
	case nil, structpb.NullValue:
		return nil, nil // the gRPC client reports a property with no value as a null
	case string, bool, int, int32, int64, float32, float64:
		return x, nil
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano), nil
	case *pb.ListValue:
		switch {
		case x.GetTextValues() != nil:
			return nonNil(x.GetTextValues().GetValues()), nil
		case x.GetDateValues() != nil:
			return nonNil(x.GetDateValues().GetValues()), nil
		case x.GetUuidValues() != nil:
			return nonNil(x.GetUuidValues().GetValues()), nil
		case x.GetBoolValues() != nil:
			return nonNil(x.GetBoolValues().GetValues()), nil
		case x.GetIntValues() != nil:
			b := x.GetIntValues().GetValues()
			out := make([]int64, 0, len(b)/8)
			for i := 0; i+8 <= len(b); i += 8 {
				out = append(out, int64(binary.LittleEndian.Uint64(b[i:])))
			}
			return out, nil
		case x.GetNumberValues() != nil:
			b := x.GetNumberValues().GetValues()
			out := make([]float64, 0, len(b)/8)
			for i := 0; i+8 <= len(b); i += 8 {
				out = append(out, math.Float64frombits(binary.LittleEndian.Uint64(b[i:])))
			}
			return out, nil
		}
		if x.GetKind() == nil {
			return []any{}, nil // an empty array of any element type
		}
		return nil, fmt.Errorf("list value of unsupported kind %T", x.GetKind())
	}
	return nil, fmt.Errorf("unsupported value type %T", v)
}

// nonNil keeps an empty array an array ([]) rather than null.
func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
