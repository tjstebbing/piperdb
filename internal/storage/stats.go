package storage

import (
	"sync"
	"time"

	"github.com/tjstebbing/piperdb/pkg/types"
)

// StorageStats tracks storage performance and usage statistics
type StorageStats struct {
	startTime    time.Time
	operations   map[string]*operationStats
	queryTime    map[string]time.Duration
	mutex        sync.RWMutex
}

type operationStats struct {
	count    int64
	totalTime time.Duration
	errors   int64
}

// NewStorageStats creates a new storage stats tracker
func NewStorageStats() *StorageStats {
	return &StorageStats{
		startTime:  time.Now(),
		operations: make(map[string]*operationStats),
		queryTime:  make(map[string]time.Duration),
	}
}

// RecordOperation records statistics for an operation
func (ss *StorageStats) RecordOperation(operation string, duration time.Duration, err error) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	if ss.operations[operation] == nil {
		ss.operations[operation] = &operationStats{}
	}

	stats := ss.operations[operation]
	stats.count++
	stats.totalTime += duration

	if err != nil {
		stats.errors++
	}
}

// RecordQueryTime records query execution time
func (ss *StorageStats) RecordQueryTime(query string, duration time.Duration) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	ss.queryTime[query] = duration
}

// GetOperationStats returns statistics for an operation
func (ss *StorageStats) GetOperationStats(operation string) (count int64, avgDuration time.Duration, errorRate float64) {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	stats, exists := ss.operations[operation]
	if !exists {
		return 0, 0, 0
	}

	count = stats.count
	if count > 0 {
		avgDuration = stats.totalTime / time.Duration(count)
		errorRate = float64(stats.errors) / float64(count)
	}

	return count, avgDuration, errorRate
}

// GetGlobalStats returns global database statistics
func (ss *StorageStats) GetGlobalStats(listCount int64, totalItems int64, totalSize int64) *types.DatabaseStats {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	// Calculate average query time
	totalQueries := int64(0)
	totalQueryTime := time.Duration(0)

	for _, duration := range ss.queryTime {
		totalQueries++
		totalQueryTime += duration
	}

	avgQueryTime := time.Duration(0)
	if totalQueries > 0 {
		avgQueryTime = totalQueryTime / time.Duration(totalQueries)
	}

	return &types.DatabaseStats{
		Lists:         listCount,
		TotalItems:    totalItems,
		TotalSize:     totalSize,
		QueryCount:    totalQueries,
		AvgQueryTime:  avgQueryTime,
		CacheHitRate:  0.0, // TODO: Implement cache hit rate tracking
		MemoryUsage:   0,   // TODO: Implement memory usage tracking
		ActiveConns:   0,   // TODO: Implement connection tracking
		Uptime:        time.Since(ss.startTime),
	}
}

// Reset clears all statistics
func (ss *StorageStats) Reset() {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	ss.startTime = time.Now()
	ss.operations = make(map[string]*operationStats)
	ss.queryTime = make(map[string]time.Duration)
}

// GetDetailedStats returns detailed statistics for all operations
func (ss *StorageStats) GetDetailedStats() map[string]interface{} {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	stats := make(map[string]interface{})

	for operation, opStats := range ss.operations {
		avgDuration := time.Duration(0)
		errorRate := 0.0

		if opStats.count > 0 {
			avgDuration = opStats.totalTime / time.Duration(opStats.count)
			errorRate = float64(opStats.errors) / float64(opStats.count)
		}

		stats[operation] = map[string]interface{}{
			"count":        opStats.count,
			"avg_duration": avgDuration,
			"error_rate":   errorRate,
			"total_time":   opStats.totalTime,
			"errors":       opStats.errors,
		}
	}

	stats["uptime"] = time.Since(ss.startTime)
	stats["query_count"] = len(ss.queryTime)

	return stats
}
