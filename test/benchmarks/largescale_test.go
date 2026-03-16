package benchmarks

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tjstebbing/piperdb/pkg/db"
)

// TestLargeScalePerformance benchmarks query performance at 100k and 500k items.
// Run separately: go test -v -run=TestLargeScalePerformance -timeout=60m ./test/benchmarks/
func TestLargeScalePerformance(t *testing.T) {
	sizes := []int{100_000, 500_000}
	const iterations = 100

	queries := []struct {
		name  string
		pipe  string
		index bool
	}{
		{"equality @brand=Apple", `@brand=Apple`, true},
		{"multi @brand=Apple @category=phone", `@brand=Apple @category=phone`, true},
		{"pipeline filter|sort|take", `@brand=Samsung | sort -price | take 10`, true},
		{"range @price>1000 @price<2000", `@price>1000 @price<2000`, false},
		{"sort -price", `sort -price`, false},
		{"filter+count", `@brand=Apple | count`, true},
	}

	for _, size := range sizes {
		t.Logf("=== %dk items ===", size/1000)

		t.Logf("Loading %d items (no index)...", size)
		start := time.Now()
		envNoIdx := newEnv(t, size, false)
		t.Logf("  loaded in %s", time.Since(start))

		t.Logf("Loading %d items (indexed)...", size)
		start = time.Now()
		envIdx := newEnv(t, size, true)
		t.Logf("  loaded in %s", time.Since(start))

		ctx := context.Background()

		for _, q := range queries {
			envNoIdx.db.ExecutePipe(ctx, "products", q.pipe, nil)
			envIdx.db.ExecutePipe(ctx, "products", q.pipe, nil)

			start := time.Now()
			for i := 0; i < iterations; i++ {
				envNoIdx.db.ExecutePipe(ctx, "products", q.pipe, nil)
			}
			noIdxDur := time.Since(start) / time.Duration(iterations)

			start = time.Now()
			for i := 0; i < iterations; i++ {
				envIdx.db.ExecutePipe(ctx, "products", q.pipe, nil)
			}
			idxDur := time.Since(start) / time.Duration(iterations)

			rsCheck, _ := envIdx.db.ExecutePipe(ctx, "products", q.pipe, nil)
			plan := ""
			if rsCheck != nil {
				plan = rsCheck.PlanUsed
			}

			t.Logf("  %-40s  no-index: %12s  indexed: %12s  plan: %s",
				q.name, noIdxDur, idxDur, plan)
		}

		envNoIdx.close()
		envIdx.close()
	}

	// High-selectivity test at large scale
	t.Run("HighSelectivityLargeScale", func(t *testing.T) {
		for _, size := range sizes {
			tmpDir, err := os.MkdirTemp("", "piperdb_bench_*")
			require.NoError(t, err)

			cfg := db.DefaultConfig()
			cfg.DataDir = tmpDir
			database, err := db.Open(cfg)
			require.NoError(t, err)

			ctx := context.Background()
			require.NoError(t, database.CreateList(ctx, "hsi"))

			items := generateItems(size)
			rng := rand.New(rand.NewSource(99))
			for i := range items {
				if rng.Intn(100) == 0 {
					items[i]["status"] = "rare"
				} else {
					items[i]["status"] = "common"
				}
			}

			t.Logf("  Loading %dk items for high-selectivity test...", size/1000)
			start := time.Now()
			_, err = database.AddItems(ctx, "hsi", items)
			require.NoError(t, err)
			t.Logf("  loaded in %s", time.Since(start))
			require.NoError(t, database.CreateIndex(ctx, "hsi", "status", "equality"))

			database.ExecutePipe(ctx, "hsi", `@status=rare`, nil)
			start = time.Now()
			for i := 0; i < iterations; i++ {
				database.ExecutePipe(ctx, "hsi", `@status=rare`, nil)
			}
			idxDur := time.Since(start) / time.Duration(iterations)

			rs, _ := database.ExecutePipe(ctx, "hsi", `@status=rare`, nil)
			t.Logf("  [%dk] @status=rare (1%%)  time: %12s  matches: %d  plan: %s",
				size/1000, idxDur, len(rs.Items), rs.PlanUsed)

			database.Close()
			os.RemoveAll(tmpDir)
		}
	})
}
