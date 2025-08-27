package clickhouse

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"
)

// RunAsyncInsertBufferTest tests the async insert buffer behavior with high-frequency small batches
func RunAsyncInsertBufferTest() {
	// Test configuration for high-frequency small batches with buffer optimization
	const (
		// Buffer settings for accumulating small frequent batches
		asyncTimeout = 30 * time.Second // 30 second timeout (accumulate small batches)
		bufferSize   = 1024 * 1024      // 1MB buffer (realistic for production) - this is BYTES not rows!

		// Realistic small batch simulation
		testBatchSize   = 10 // 10 rows per batch (your real scenario)
		batchFrequency  = 5  // 4-5 batches per second (realistic frequency)
		testDurationSec = 40 // Run for 40 seconds (let buffer accumulate)

		// Calculate total based on frequency and duration
		totalBatches  = batchFrequency * testDurationSec // 200 batches total
		totalTestRows = totalBatches * testBatchSize     // 2,000 rows total

		// Part size optimization
		minPartSizeBytes = 5 * 1024 * 1024 // Target 5MB minimum part size (more realistic)
	)

	ctx := context.Background()

	fmt.Println("🧪 ClickHouse High-Frequency Small Batch Buffer Test")
	fmt.Println("===================================================")
	fmt.Println("Testing realistic small batches with buffer optimization:")
	fmt.Printf("- Small batches: %d rows per batch (your real scenario)\n", testBatchSize)
	fmt.Printf("- Realistic frequency: %d-%d batches/sec\n", batchFrequency-1, batchFrequency)
	fmt.Printf("- Buffer timeout: %d seconds (accumulate small batches)\n", int(asyncTimeout.Seconds()))
	fmt.Printf("- Buffer size: %s rows\n", formatNumber(totalTestRows))
	fmt.Printf("- Target: Minimum %dMB part size for optimal performance\n", minPartSizeBytes/(1024*1024))
	fmt.Printf("- Goal: Buffer accumulates ~%d small batches before timeout flush\n", totalBatches)

	// Create ClickHouse client with specific async settings
	clickhouseConfig := ClickHouseConfig{
		Host:            "localhost",
		Port:            9000,
		Database:        "default",
		Username:        "default",
		Password:        "password",
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		BatchSize:       testBatchSize,
		AsyncSettings: AsyncConfig{
			Enabled:                   true,
			Wait:                      false,        // CRITICAL: Don't wait for flush (async)
			AsyncInsertMaxSize:        bufferSize,   // 100KB buffer (BYTES not rows!)
			AsyncInsertWait:           asyncTimeout, // 30 second timeout
			AsyncInsertThreads:        4,
			AsyncInsertMaxQueryNumber: 1000,
			AsyncInsertStaleTimeout:   30 * time.Second, // Longer than timeout
			AsyncInsertDeduplicate:    false,
			AsyncInsertUseAdaptive:    false, // CRITICAL: Disable adaptive timeout
		},
	}

	client, err := NewClient(clickhouseConfig)
	if err != nil {
		log.Fatalf("❌ Failed to create ClickHouse client: %v", err)
	}
	defer client.Close()

	// Health check
	if err := client.HealthCheck(ctx); err != nil {
		log.Fatalf("❌ ClickHouse health check failed: %v", err)
	}
	fmt.Println("✅ ClickHouse connection healthy")

	// Test table name
	tableName := "async_buffer_test"

	// Create test schema
	schema := &TableSchema{
		TableName:   tableName,
		StructName:  "AsyncBufferTest",
		PrimaryKey:  []string{"id"},
		PartitionBy: "toYYYYMM(timestamp)",
		Fields: []FieldSchema{
			{Name: "ID", ColumnName: "id", GoType: "uint64", ClickHouseType: "UInt64"},
			{Name: "BatchID", ColumnName: "batch_id", GoType: "uint64", ClickHouseType: "UInt64"},
			{Name: "Timestamp", ColumnName: "timestamp", GoType: "time.Time", ClickHouseType: "DateTime"},
			{Name: "Value", ColumnName: "value", GoType: "string", ClickHouseType: "String"},
		},
		Engine: EngineConfig{
			Type:       "MergeTree",
			Parameters: map[string]string{},
		},
		Settings: DefaultSettings(),
	}

	// Create table
	err = client.CreateTableFromSchema(ctx, schema)
	if err != nil {
		log.Fatalf("❌ Failed to create test table: %v", err)
	}
	fmt.Println("✅ Test table created successfully")

	// Try to truncate table (ignore errors)
	if err := client.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", tableName)); err != nil {
		log.Printf("Warning: Could not truncate table: %v", err)
	}

	// Test: High-frequency small batch test
	fmt.Println("\n🧪 TEST: High-Frequency Small Batches with Buffer Accumulation")
	testHighFrequencySmallBatches(ctx, client, tableName, totalBatches, testBatchSize, batchFrequency, minPartSizeBytes)

	fmt.Println("\n✅ Async insert buffer test completed!")
}

func testHighFrequencySmallBatches(ctx context.Context, client *ClickHouseClient, tableName string, totalBatches, batchSize, frequency int, minPartSize int) {
	fmt.Printf("  🎯 Goal: Accumulate %d small batches (%d rows each) in buffer\n", totalBatches, batchSize)
	fmt.Printf("  🚀 Frequency: %d batches/second (realistic frequency)\n", frequency)
	fmt.Printf("  ⏰ Buffer timeout: 30s (let small batches accumulate)\n")
	fmt.Printf("  📦 Expected: Buffer accumulates many small batches → creates fewer, larger parts\n")

	// Check initial parts state
	fmt.Printf("  📊 Parts before insertion:\n")
	checkTableParts(ctx, client, tableName)

	fmt.Printf("  🚀 Starting high-frequency small batch insertion...\n")
	fmt.Printf("  📊 Sending %d batches at %dms intervals\n", totalBatches, 1000/frequency)

	successfulBatches := 0
	startTime := time.Now()

	for batchId := 0; batchId < totalBatches; batchId++ {
		// Generate small batch data
		batchData := make([]map[string]any, batchSize)
		for i := 0; i < batchSize; i++ {
			rowId := uint64(batchId*batchSize + i + 1)
			batchData[i] = map[string]any{
				"id":        rowId,
				"batch_id":  uint64(batchId),
				"timestamp": time.Now(),
				"value":     fmt.Sprintf("event_%d", rowId),
			}
		}

		// Insert small batch with timeout
		fmt.Printf("  🔄 Inserting batch %d...\n", batchId+1)
		batchCtx, cancel := context.WithTimeout(ctx, 5*time.Second) // Added timeout
		err := client.SchemaBasedInsert(batchCtx, tableName, batchData)
		cancel() // Release context resources

		if err != nil {
			log.Printf("❌ Batch %d failed: %v", batchId+1, err)
		} else {
			successfulBatches++
			fmt.Printf("  ✅ Batch %d completed\n", batchId+1)
		}

		// Progress reporting every 50 batches
		if (batchId+1)%50 == 0 {
			elapsed := time.Since(startTime)
			actualFreq := float64(batchId+1) / elapsed.Seconds()
			fmt.Printf("  📊 Sent %d batches (%.1f batches/sec) - checking buffer state...\n", batchId+1, actualFreq)
			checkTableParts(ctx, client, tableName)
		}

		// Control frequency (sleep between batches)
		time.Sleep(time.Duration(1000/frequency) * time.Millisecond)
	}

	// Calculate total time
	totalTime := time.Since(startTime)
	actualFreq := float64(successfulBatches) / totalTime.Seconds()
	fmt.Printf("  ⏱️ Sent %d batches in %v (%.1f batches/sec)\n", successfulBatches, totalTime, actualFreq)

	// Monitor buffer state continuously instead of just waiting
	fmt.Printf("  ⏳ Monitoring buffer state every 2 seconds...\n")
	fmt.Printf("  💡 Watching for async insert buffer accumulation and flush behavior\n")
	monitorDuration := 35 * time.Second
	monitorStart := time.Now()
	for time.Since(monitorStart) < monitorDuration {
		time.Sleep(2 * time.Second)
		elapsed := time.Since(monitorStart)
		fmt.Printf("  📊 Buffer state at +%.0fs:\n", elapsed.Seconds())
		checkTableParts(ctx, client, tableName)
		checkAsyncInsertBuffer(ctx, client, tableName)
	}

	// Final state
	fmt.Printf("  📊 Final parts state:\n")
	checkTableParts(ctx, client, tableName)

	// Check async insert log to see batching behavior
	checkAsyncInsertLog(ctx, client, "HIGH-FREQUENCY")

	// Verify data count
	verifyDataCount(ctx, client, tableName, totalBatches*batchSize)

	// Summary
	fmt.Printf("  📊 High-Frequency Batching Analysis:\n")
	fmt.Printf("    📤 Sent: %d small batches (%d rows each)\n", successfulBatches, batchSize)
	fmt.Printf("    ⏱️  Actual frequency: %.1f batches/sec\n", actualFreq)

	// Parts efficiency analysis
	fmt.Printf("  📊 Parts Efficiency Analysis:\n")
	analyzePartsEfficiency(ctx, client, tableName, minPartSize)
}

func checkTableParts(ctx context.Context, client *ClickHouseClient, tableName string) {
	rows, err := client.Query(ctx, `
		SELECT
			partition,
			name,
			active,
			rows,
			bytes_on_disk,
			data_compressed_bytes,
			data_uncompressed_bytes,
			modification_time
		FROM system.parts
		WHERE table = ? AND active = 1
		ORDER BY modification_time DESC
	`, tableName)

	if err != nil {
		log.Printf("❌ Failed to query table parts: %v", err)
		return
	}
	defer rows.Close()

	fmt.Printf("    📦 Active Parts:\n")
	fmt.Printf("    %-20s %-10s %-10s %-15s %-10s %-10s\n", "Name", "Rows", "Active", "Size(MB)", "Ratio", "Modified")
	fmt.Printf("    %s\n", strings.Repeat("-", 80))

	totalRows := uint64(0)
	totalSize := uint64(0)
	partCount := 0

	for rows.Next() {
		var partition, name string
		var active uint8
		var rowCount, bytesOnDisk, dataCompressed, dataUncompressed uint64
		var modTime time.Time

		err := rows.Scan(&partition, &name, &active, &rowCount, &bytesOnDisk, &dataCompressed, &dataUncompressed, &modTime)
		if err != nil {
			log.Printf("❌ Failed to scan part row: %v", err)
			continue
		}

		sizeMB := float64(bytesOnDisk) / (1024 * 1024)
		ratio := float64(dataCompressed) / float64(dataUncompressed) * 100

		fmt.Printf("    %-20s %-10d %-10d %-15.2f %-10.1f %-10s\n",
			name, rowCount, active, sizeMB, ratio, modTime.Format("15:04:05"))

		totalRows += rowCount
		totalSize += bytesOnDisk
		partCount++
	}

	if partCount == 0 {
		fmt.Printf("    📭 No active parts found\n")
	} else {
		fmt.Printf("    %s\n", strings.Repeat("-", 80))
		avgSizeMB := float64(totalSize) / float64(partCount) / (1024 * 1024)
		fmt.Printf("    📊 Summary: %d parts, %s total rows, %.2f MB avg part size\n", partCount, formatNumber(int(totalRows)), avgSizeMB)
	}
}

func checkAsyncInsertBuffer(ctx context.Context, client *ClickHouseClient, tableName string) {
	// Check if there are pending async inserts
	rows, err := client.Query(ctx, `
		SELECT
			COUNT(*) as pending_inserts,
			SUM(total_bytes) as pending_bytes
		FROM system.asynchronous_inserts
		WHERE table = ?
	`, tableName)

	if err != nil {
		log.Printf("❌ Failed to query async insert buffer: %v", err)
		return
	}
	defer rows.Close()

	if rows.Next() {
		var pendingInserts, pendingBytes uint64
		if err := rows.Scan(&pendingInserts, &pendingBytes); err != nil {
			log.Printf("❌ Buffer scan failed: %v", err)
			return
		}

		if pendingInserts > 0 {
			fmt.Printf("    🔄 Buffer: %d pending inserts, %d bytes waiting\n", pendingInserts, pendingBytes)
		} else {
			fmt.Printf("    ✅ Buffer: Empty (all data flushed)\n")
		}
	}
}

func checkAsyncInsertLog(ctx context.Context, client *ClickHouseClient, testName string) {
	fmt.Printf("  🔍 Checking async insert log for %s test...\n", testName)

	rows, err := client.Query(ctx, `
		SELECT
			event_time,
			flush_time,
			rows,
			status,
			table
		FROM system.asynchronous_insert_log
		WHERE table = 'async_buffer_test'
		ORDER BY event_time DESC
		LIMIT 10
	`)

	if err != nil {
		log.Printf("❌ Failed to query async insert log: %v", err)
		return
	}
	defer rows.Close()

	fmt.Printf("  📋 Recent async insert log entries:\n")

	flushGroups := make(map[string][]string)

	for rows.Next() {
		var eventTime, flushTime time.Time
		var rowCount uint64
		var status, table string

		err := rows.Scan(&eventTime, &flushTime, &rowCount, &status, &table)
		if err != nil {
			log.Printf("❌ Failed to scan log row: %v", err)
			continue
		}

		delay := flushTime.Sub(eventTime)
		flushKey := flushTime.Format("15:04:05.000")

		fmt.Printf("    - Event: %s, Flush: %s, Rows: %d, Delay: %.0fs, Status: %s\n",
			eventTime.Format("15:04:05.000"),
			flushTime.Format("15:04:05.000"),
			rowCount,
			delay.Seconds(),
			status)

		// Group by flush time to detect batching
		flushGroups[flushKey] = append(flushGroups[flushKey], fmt.Sprintf("%d rows", rowCount))
	}

	// Analyze flush patterns
	fmt.Printf("  📊 Flush Analysis:\n")
	for flushTime, entries := range flushGroups {
		if len(entries) > 1 {
			fmt.Printf("    ✅ BATCHED FLUSH at %s: %d batches flushed together\n", flushTime, len(entries))
		}
	}
}

func verifyDataCount(ctx context.Context, client *ClickHouseClient, tableName string, expectedRows int) {
	fmt.Printf("  🔍 Verifying data count...\n")

	var actualRows uint64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	err := client.conn.QueryRow(ctx, query).Scan(&actualRows)
	if err != nil {
		log.Printf("❌ Failed to verify data count: %v", err)
		return
	}

	fmt.Printf("  📊 Data verification: %s rows in table (expected: %s)\n", formatNumber(int(actualRows)), formatNumber(expectedRows))
	if int(actualRows) != expectedRows {
		fmt.Printf("  ⚠️ Data count mismatch - this might be due to async buffering\n")
	}
}

func analyzePartsEfficiency(ctx context.Context, client *ClickHouseClient, tableName string, minPartSize int) {
	rows, err := client.Query(ctx, `
		SELECT
			COUNT(*) as part_count,
			SUM(rows) as total_rows,
			AVG(bytes_on_disk) as avg_part_size,
			MIN(bytes_on_disk) as min_part_size,
			MAX(bytes_on_disk) as max_part_size,
			SUM(bytes_on_disk) as total_size
		FROM system.parts
		WHERE table = ? AND active = 1
	`, tableName)

	if err != nil {
		log.Printf("❌ Failed to analyze parts: %v", err)
		return
	}
	defer rows.Close()

	if rows.Next() {
		var partCount, totalRows uint64
		var avgSize float64
		var minSize, maxSize, totalSize uint64

		if err := rows.Scan(&partCount, &totalRows, &avgSize, &minSize, &maxSize, &totalSize); err != nil {
			log.Printf("❌ Scan failed: %v", err)
			return
		}

		avgSizeMB := avgSize / (1024 * 1024)
		minSizeMB := float64(minSize) / (1024 * 1024)
		maxSizeMB := float64(maxSize) / (1024 * 1024)
		totalSizeMB := float64(totalSize) / (1024 * 1024)
		targetSizeMB := float64(minPartSize) / (1024 * 1024)

		fmt.Printf("    📊 Parts: %d total, %s rows\n", partCount, formatNumber(int(totalRows)))
		fmt.Printf("    📏 Size: %.2f MB avg, %.2f MB min, %.2f MB max, %.2f MB total\n", avgSizeMB, minSizeMB, maxSizeMB, totalSizeMB)
		fmt.Printf("    🎯 Target: %.2f MB minimum part size\n", targetSizeMB)

		if avgSizeMB < targetSizeMB {
			fmt.Printf("    ⚠️  Parts are smaller than target - consider increasing buffer size or timeout\n")
		} else {
			fmt.Printf("    ✅ Parts meet size target - good async buffer configuration!\n")
		}
	}
}

func formatNumber(n int) string {
	if n >= 1000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	} else if n >= 1000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%d", n)
}
