package benchmarks

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tjstebbing/piperdb/pkg/db"
)

var (
	brands     = []string{"Apple", "Samsung", "Google", "Sony", "LG", "Microsoft", "Dell", "HP", "Lenovo", "Asus"}
	categories = []string{"phone", "laptop", "tablet", "tv", "headphones", "speaker", "watch", "camera", "monitor", "keyboard"}
	statuses   = []string{"active", "discontinued", "preorder", "clearance"}
)

func generateItems(n int) []map[string]interface{} {
	rng := rand.New(rand.NewSource(42))
	items := make([]map[string]interface{}, n)
	for i := 0; i < n; i++ {
		tags := make([]interface{}, 1+rng.Intn(3))
		for j := range tags {
			tags[j] = fmt.Sprintf("tag%d", rng.Intn(50))
		}
		items[i] = map[string]interface{}{
			"name":     fmt.Sprintf("Product-%d", i),
			"brand":    brands[rng.Intn(len(brands))],
			"category": categories[rng.Intn(len(categories))],
			"price":    float64(50 + rng.Intn(2950)),
			"rating":   float64(1+rng.Intn(50)) / 10.0,
			"status":   statuses[rng.Intn(len(statuses))],
			"stock":    rng.Intn(500),
			"tags":     tags,
		}
	}
	return items
}

type testEnv struct {
	db  db.PiperDB
	dir string
}

func newEnv(t testing.TB, n int, indexed bool) *testEnv {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "piperdb_bench_*")
	require.NoError(t, err)

	cfg := db.DefaultConfig()
	cfg.DataDir = tmpDir
	database, err := db.Open(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, database.CreateList(ctx, "products"))
	_, err = database.AddItems(ctx, "products", generateItems(n))
	require.NoError(t, err)

	if indexed {
		require.NoError(t, database.CreateIndex(ctx, "products", "brand", "equality"))
		require.NoError(t, database.CreateIndex(ctx, "products", "category", "equality"))
		require.NoError(t, database.CreateIndex(ctx, "products", "status", "equality"))
	}
	return &testEnv{db: database, dir: tmpDir}
}

func (e *testEnv) close() {
	e.db.Close()
	os.RemoveAll(e.dir)
}

// TestQueryPerformance runs timed query comparisons and prints a results table.
// This avoids the Go benchmark framework's repeated setup cost for expensive
// data loading while still giving reliable timing numbers.
func TestQueryPerformance(t *testing.T) {
	sizes := []int{1_000, 10_000, 50_000}
	queries := []struct {
		name  string
		pipe  string
		index bool // whether index helps this query
	}{
		{"equality @brand=Apple", `@brand=Apple`, true},
		{"multi @brand=Apple @category=phone", `@brand=Apple @category=phone`, true},
		{"pipeline filter|sort|take", `@brand=Samsung | sort -price | take 10`, true},
		{"range @price>1000 @price<2000", `@price>1000 @price<2000`, false},
		{"sort -price", `sort -price`, false},
		{"filter+count", `@brand=Apple | count`, true},
	}

	const iterations = 200

	type result struct {
		query   string
		size    int
		noIndex time.Duration
		indexed time.Duration
	}
	var results []result

	for _, size := range sizes {
		t.Logf("Loading %d items (no index)...", size)
		envNoIdx := newEnv(t, size, false)
		t.Logf("Loading %d items (indexed)...", size)
		envIdx := newEnv(t, size, true)

		ctx := context.Background()

		for _, q := range queries {
			// Warm up
			envNoIdx.db.ExecutePipe(ctx, "products", q.pipe, nil)
			envIdx.db.ExecutePipe(ctx, "products", q.pipe, nil)

			// Time without index
			start := time.Now()
			for i := 0; i < iterations; i++ {
				_, err := envNoIdx.db.ExecutePipe(ctx, "products", q.pipe, nil)
				assert.NoError(t, err)
			}
			noIdxDur := time.Since(start) / time.Duration(iterations)

			// Time with index
			start = time.Now()
			for i := 0; i < iterations; i++ {
				_, err := envIdx.db.ExecutePipe(ctx, "products", q.pipe, nil)
				assert.NoError(t, err)
			}
			idxDur := time.Since(start) / time.Duration(iterations)

			results = append(results, result{
				query:   q.name,
				size:    size,
				noIndex: noIdxDur,
				indexed: idxDur,
			})

			// Check which plan the indexed DB chose
			rsCheck, _ := envIdx.db.ExecutePipe(ctx, "products", q.pipe, nil)
			plan := ""
			if rsCheck != nil {
				plan = rsCheck.PlanUsed
			}

			speedup := ""
			if q.index && idxDur > 0 {
				speedup = fmt.Sprintf("%.1fx", float64(noIdxDur)/float64(idxDur))
			}
			t.Logf("  [%dk] %-40s  no-index: %12s  indexed: %12s  speedup: %s  plan: %s",
				size/1000, q.name, noIdxDur, idxDur, speedup, plan)
		}

		envNoIdx.close()
		envIdx.close()
	}

	// Test index lookup performance at high selectivity (1% match rate)
	// where the planner correctly chooses the index path.
	t.Run("HighSelectivityIndex", func(t *testing.T) {
		for _, size := range sizes {
			tmpDir, err := os.MkdirTemp("", "piperdb_bench_*")
			require.NoError(t, err)

			cfg := db.DefaultConfig()
			cfg.DataDir = tmpDir
			database, err := db.Open(cfg)
			require.NoError(t, err)

			ctx := context.Background()
			require.NoError(t, database.CreateList(ctx, "hsi"))

			// 1% of items get status=rare, rest get status=common
			items := generateItems(size)
			rng := rand.New(rand.NewSource(99))
			for i := range items {
				if rng.Intn(100) == 0 {
					items[i]["status"] = "rare"
				} else {
					items[i]["status"] = "common"
				}
			}
			_, err = database.AddItems(ctx, "hsi", items)
			require.NoError(t, err)
			require.NoError(t, database.CreateIndex(ctx, "hsi", "status", "equality"))

			// Time indexed query
			database.ExecutePipe(ctx, "hsi", `@status=rare`, nil) // warm up
			start := time.Now()
			for i := 0; i < iterations; i++ {
				database.ExecutePipe(ctx, "hsi", `@status=rare`, nil)
			}
			idxDur := time.Since(start) / time.Duration(iterations)

			rs, _ := database.ExecutePipe(ctx, "hsi", `@status=rare`, nil)
			t.Logf("  [%dk] @status=rare (1%% selectivity)  time: %12s  matches: %d  plan: %s",
				size/1000, idxDur, len(rs.Items), rs.PlanUsed)

			database.Close()
			os.RemoveAll(tmpDir)
		}
	})

	// Verify selectivity-aware plan selection
	t.Run("VerifyIndexPlan", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "piperdb_bench_*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		cfg := db.DefaultConfig()
		cfg.DataDir = tmpDir
		database, err := db.Open(cfg)
		require.NoError(t, err)
		defer database.Close()

		ctx := context.Background()
		require.NoError(t, database.CreateList(ctx, "plantest"))

		// 1000 items: 999 brand=Common, 1 brand=Rare (~0.1% selectivity)
		items := make([]map[string]interface{}, 1000)
		for i := range items {
			items[i] = map[string]interface{}{"brand": "Common", "price": float64(i)}
		}
		items[0]["brand"] = "Rare"
		_, err = database.AddItems(ctx, "plantest", items)
		require.NoError(t, err)
		require.NoError(t, database.CreateIndex(ctx, "plantest", "brand", "equality"))

		// High selectivity (0.1%) → should use index
		rs, err := database.ExecutePipe(ctx, "plantest", `@brand=Rare`, nil)
		require.NoError(t, err)
		assert.Equal(t, "index", rs.PlanUsed, "high-selectivity query should use index")
		assert.Equal(t, 1, len(rs.Items))

		// Low selectivity (99.9%) → should fall back to sequential
		rs2, err := database.ExecutePipe(ctx, "plantest", `@brand=Common`, nil)
		require.NoError(t, err)
		assert.Equal(t, "sequential", rs2.PlanUsed, "low-selectivity query should use sequential scan")
		assert.Equal(t, 999, len(rs2.Items))

		// Range filter (no index) → sequential
		rs3, err := database.ExecutePipe(ctx, "plantest", `@price>500`, nil)
		require.NoError(t, err)
		assert.Equal(t, "sequential", rs3.PlanUsed, "range filter should use sequential scan")

		// Multi-index intersection: both fields individually >5% but combined <5%
		// 10 brands × 10 categories = 100 combos → each combo ~1%
		require.NoError(t, database.CreateList(ctx, "multitest"))
		multiItems := generateItems(10_000)
		_, err = database.AddItems(ctx, "multitest", multiItems)
		require.NoError(t, err)
		require.NoError(t, database.CreateIndex(ctx, "multitest", "brand", "equality"))
		require.NoError(t, database.CreateIndex(ctx, "multitest", "category", "equality"))

		rs4, err := database.ExecutePipe(ctx, "multitest", `@brand=Apple @category=phone`, nil)
		require.NoError(t, err)
		assert.Equal(t, "index", rs4.PlanUsed, "multi-index intersection should use index")
		assert.Greater(t, len(rs4.Items), 0)
		t.Logf("Multi-index intersection: %d matches from 10k items, plan=%s", len(rs4.Items), rs4.PlanUsed)
	})
}

// Lightweight Go benchmarks for the smallest dataset (1k) only, so the
// benchmark framework's repeated function calls don't cause excessive setup.
func BenchmarkEqualityFilter_1k_NoIndex(b *testing.B) {
	env := newEnv(b, 1_000, false)
	defer env.close()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.db.ExecutePipe(ctx, "products", `@brand=Apple`, nil)
	}
}

func BenchmarkEqualityFilter_1k_Index(b *testing.B) {
	env := newEnv(b, 1_000, true)
	defer env.close()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.db.ExecutePipe(ctx, "products", `@brand=Apple`, nil)
	}
}

func BenchmarkPipeline_1k_NoIndex(b *testing.B) {
	env := newEnv(b, 1_000, false)
	defer env.close()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.db.ExecutePipe(ctx, "products", `@brand=Samsung | sort -price | take 10`, nil)
	}
}

func BenchmarkPipeline_1k_Index(b *testing.B) {
	env := newEnv(b, 1_000, true)
	defer env.close()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.db.ExecutePipe(ctx, "products", `@brand=Samsung | sort -price | take 10`, nil)
	}
}
