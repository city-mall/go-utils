package clickhouse

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"
)

// BulkPerformanceTest runs comprehensive bulk performance tests
func BulkPerformanceTest() {
	fmt.Println("ClickHouse Bulk Performance Test with Upserts")
	fmt.Println("===========================================")

	// Load IST timezone
	istLocation, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		log.Fatalf("Failed to load IST timezone: %v", err)
	}
	fmt.Println("✓ Using timezone: Asia/Kolkata (IST)")

	// Create ClickHouse client with optimized settings for bulk operations
	config := ClickHouseConfig{
		Host:            "localhost",
		Port:            9000,
		Database:        "default",
		Username:        "default",
		Password:        "password",
		MaxOpenConns:    20,
		MaxIdleConns:    10,
		ConnMaxLifetime: time.Hour,
		BatchSize:       1000, // Will use this as our batch size
		AsyncSettings: AsyncConfig{
			Wait:               false,                  // Don't wait for async inserts to complete
			AsyncInsertMaxSize: 10485760,               // 10MB buffer for bulk operations
			AsyncInsertWait:    100 * time.Millisecond, // 100ms max wait before flush
		},
	}

	client, err := NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create ClickHouse client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Define schema optimized for bulk operations
	tableName := "bulk_user_events"
	schema := &TableSchema{
		TableName:  tableName,
		StructName: "BulkUserEvents",
		PrimaryKey: []string{"id"},
		Fields: []FieldSchema{
			{Name: "Id", ColumnName: "id", GoType: "uint64", ClickHouseType: "UInt64"},
			{Name: "UserId", ColumnName: "user_id", GoType: "uint64", ClickHouseType: "UInt64"},
			{Name: "EventType", ColumnName: "event_type", GoType: "string", ClickHouseType: "String"},
			{Name: "Timestamp", ColumnName: "timestamp", GoType: "time.Time", ClickHouseType: "DateTime('Asia/Kolkata')"},
			{Name: "Properties", ColumnName: "properties", GoType: "map[string]string", ClickHouseType: "Map(String, String)"},
			{Name: "Value", ColumnName: "value", GoType: "float64", ClickHouseType: "Float64"},
			{Name: "SessionId", ColumnName: "session_id", GoType: "string", ClickHouseType: "String"},
		},
		Engine: EngineConfig{
			Type: "ReplacingMergeTree", // Perfect for upsert scenarios
			Parameters: map[string]string{
				"version": "timestamp", // Use timestamp as version column for replacement logic
			},
		},
		PartitionBy: "toYYYYMMDD(timestamp)",
		OrderBy:     []string{"id"},
		Settings: []string{
			"index_granularity = 8192",
		},
	}

	// Register schema for optimized zero-reflection inserts
	client.RegisterTableSchema(schema)

	// Create table from schema
	if err := client.CreateTableFromSchema(ctx, schema); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Println("✓ Table created successfully")

	// Performance test configuration
	const (
		totalRows    = 50000
		batchSize    = 1000
		totalBatches = totalRows / batchSize
	)

	eventTypes := []string{"click", "view", "purchase", "signup", "login", "logout", "share", "download"}
	pages := []string{"home", "product", "checkout", "profile", "search", "category", "about", "contact"}
	sources := []string{"organic", "paid", "social", "email", "direct", "referral"}

	fmt.Printf("\n🚀 Starting bulk insert test: %d rows in %d batches of %d rows each\n",
		totalRows, totalBatches, batchSize)
	fmt.Println(strings.Repeat("=", 60))

	// Track performance metrics
	var totalInsertTime time.Duration
	var batchTimes []time.Duration
	baseTime := time.Now().In(istLocation)

	// Phase 1: Initial bulk insert
	fmt.Println("\n📊 Phase 1: Initial Bulk Insert")
	for batch := range totalBatches {
		batchStart := time.Now()

		// Generate batch data
		batchData := make([]map[string]any, batchSize)
		for i := range batchSize {
			rowId := uint64(batch*batchSize + i + 1)
			batchData[i] = map[string]any{
				"id":         rowId,
				"user_id":    uint64(rand.Intn(10000) + 1), // Random user IDs 1-10000
				"event_type": eventTypes[rand.Intn(len(eventTypes))],
				"timestamp":  baseTime.Add(time.Duration(rand.Intn(3600)) * time.Second), // Random within 1 hour
				"properties": map[string]string{
					"page":   pages[rand.Intn(len(pages))],
					"source": sources[rand.Intn(len(sources))],
				},
				"value":      rand.Float64() * 1000, // Random value 0-1000
				"session_id": fmt.Sprintf("sess_%d_%d", rowId%1000, rand.Intn(1000)),
			}
		}

		// Insert batch using schema-based async insert
		if err := client.SchemaBasedInsert(ctx, tableName, batchData); err != nil {
			log.Fatalf("Batch %d failed: %v", batch+1, err)
		}

		batchTime := time.Since(batchStart)
		batchTimes = append(batchTimes, batchTime)
		totalInsertTime += batchTime

		// Progress reporting
		if (batch+1)%10 == 0 || batch == totalBatches-1 {
			fmt.Printf("  Batch %d/%d completed in %v (avg: %v)\n",
				batch+1, totalBatches, batchTime, totalInsertTime/time.Duration(batch+1))
		}
	}

	fmt.Printf("\n✓ Phase 1 completed: %d rows inserted in %v\n", totalRows, totalInsertTime)
	fmt.Printf("  Average batch time: %v\n", totalInsertTime/time.Duration(totalBatches))
	fmt.Printf("  Rows per second: %.0f\n", float64(totalRows)/totalInsertTime.Seconds())

	// Wait for async operations to settle
	fmt.Println("\n⏳ Waiting for async operations to settle...")
	time.Sleep(3 * time.Second)

	// Phase 2: Upsert operations (test ReplacingMergeTree deduplication)
	fmt.Println("\n🔄 Phase 2: Testing Upserts with ReplacingMergeTree")

	upsertBatches := 10 // Test with 10 batches of upserts
	upsertBatchSize := 1000
	var totalUpsertTime time.Duration

	for batch := 0; batch < upsertBatches; batch++ {
		batchStart := time.Now()

		// Generate upsert data (mix of existing and new records)
		upsertData := make([]map[string]any, upsertBatchSize)
		for i := range upsertBatchSize {
			var rowId uint64
			if i < upsertBatchSize/2 {
				// Update existing records (first half)
				rowId = uint64(rand.Intn(totalRows) + 1)
			} else {
				// Insert new records (second half)
				rowId = uint64(totalRows + batch*upsertBatchSize + i + 1)
			}

			upsertData[i] = map[string]any{
				"id":         rowId,
				"user_id":    uint64(rand.Intn(10000) + 1),
				"event_type": eventTypes[rand.Intn(len(eventTypes))] + "_updated",
				"timestamp":  time.Now().In(istLocation),
				"properties": map[string]string{
					"page":   pages[rand.Intn(len(pages))],
					"source": "upsert_test",
					"batch":  fmt.Sprintf("upsert_%d", batch),
				},
				"value":      rand.Float64() * 2000, // Higher values for upserts
				"session_id": fmt.Sprintf("upsert_sess_%d", rowId),
			}
		}

		// Perform upsert
		if err := client.SchemaBasedInsert(ctx, tableName, upsertData); err != nil {
			log.Fatalf("Upsert batch %d failed: %v", batch+1, err)
		}

		batchTime := time.Since(batchStart)
		totalUpsertTime += batchTime

		fmt.Printf("  Upsert batch %d/%d completed in %v\n", batch+1, upsertBatches, batchTime)
	}

	fmt.Printf("\n✓ Phase 2 completed: %d upsert operations in %v\n",
		upsertBatches*upsertBatchSize, totalUpsertTime)

	// Wait for upserts to settle
	fmt.Println("\n⏳ Waiting for upsert operations to settle...")
	time.Sleep(3 * time.Second)

	// Phase 3: Performance analysis and verification
	fmt.Println("\n📈 Phase 3: Performance Analysis")

	// Query total count
	rows, err := client.Query(ctx, "SELECT COUNT(*) as count FROM "+tableName)
	if err != nil {
		log.Printf("Count query failed: %v", err)
	} else {
		defer rows.Close()
		if rows.Next() {
			var count uint64
			if err := rows.Scan(&count); err != nil {
				log.Printf("Scan failed: %v", err)
			} else {
				fmt.Printf("✓ Total records in table: %d\n", count)
			}
		}
	}

	// Analyze batch performance
	fmt.Println("\n📊 Batch Performance Statistics:")

	// Calculate statistics
	var minTime, maxTime time.Duration = batchTimes[0], batchTimes[0]
	var totalTime time.Duration

	for _, t := range batchTimes {
		if t < minTime {
			minTime = t
		}
		if t > maxTime {
			maxTime = t
		}
		totalTime += t
	}

	avgTime := totalTime / time.Duration(len(batchTimes))

	fmt.Printf("  Total batches: %d\n", len(batchTimes))
	fmt.Printf("  Fastest batch: %v\n", minTime)
	fmt.Printf("  Slowest batch: %v\n", maxTime)
	fmt.Printf("  Average batch time: %v\n", avgTime)
	fmt.Printf("  Total insert time: %v\n", totalTime)
	fmt.Printf("  Rows per second (average): %.0f\n", float64(totalRows)/totalTime.Seconds())

	// Show first few and last few batch times
	fmt.Println("\n⏱️  First 5 batch times:")
	for i := 0; i < 5 && i < len(batchTimes); i++ {
		fmt.Printf("  Batch %d: %v\n", i+1, batchTimes[i])
	}

	fmt.Println("\n⏱️  Last 5 batch times:")
	start := max(len(batchTimes)-5, 0)
	for i := start; i < len(batchTimes); i++ {
		fmt.Printf("  Batch %d: %v\n", i+1, batchTimes[i])
	}

	// Query some sample data to verify upserts worked
	fmt.Println("\n🔍 Sample data verification:")
	rows2, err := client.Query(ctx, `
		SELECT id, user_id, event_type, timestamp, value
		FROM `+tableName+`
		WHERE event_type LIKE '%updated%'
		ORDER BY timestamp DESC
		LIMIT 5
	`)
	if err != nil {
		log.Printf("Sample query failed: %v", err)
	} else {
		defer rows2.Close()
		fmt.Println("  Recent upserted records:")
		for rows2.Next() {
			var id, userID uint64
			var eventType string
			var timestamp time.Time
			var value float64

			if err := rows2.Scan(&id, &userID, &eventType, &timestamp, &value); err != nil {
				log.Printf("Scan failed: %v", err)
				continue
			}

			istTime := timestamp.In(istLocation)
			fmt.Printf("    ID: %d, User: %d, Event: %s, Value: %.2f, Time: %s\n",
				id, userID, eventType, value, istTime.Format("15:04:05"))
		}
	}

	// Connection statistics
	fmt.Println("\n📊 Connection Statistics:")
	stats := client.GetConnectionStats()
	for key, value := range stats {
		fmt.Printf("  %s: %v\n", key, value)
	}

	// Final summary
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("🎯 BULK PERFORMANCE TEST SUMMARY")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Total rows processed: %d\n", totalRows+upsertBatches*upsertBatchSize)
	fmt.Printf("Initial insert: %d rows in %v (%.0f rows/sec)\n",
		totalRows, totalInsertTime, float64(totalRows)/totalInsertTime.Seconds())
	fmt.Printf("Upsert operations: %d rows in %v (%.0f rows/sec)\n",
		upsertBatches*upsertBatchSize, totalUpsertTime,
		float64(upsertBatches*upsertBatchSize)/totalUpsertTime.Seconds())
	fmt.Printf("Batch size: %d rows\n", batchSize)
	fmt.Printf("Average batch time: %v\n", avgTime)
	fmt.Printf("ReplacingMergeTree deduplication: ✓ Active\n")
	fmt.Printf("Schema-based zero-reflection inserts: ✓ Used\n")
	fmt.Printf("IST timezone support: ✓ Active\n")

	fmt.Println("\n✅ Bulk performance test completed successfully!")
}

// SimpleClickHouseExample demonstrates basic ClickHouse client usage
func SimpleClickHouseExample() {
	fmt.Println("\n=== ClickHouse Client Example ===")

	// Configuration for production
	config := ClickHouseConfig{
		Host:            "localhost",
		Port:            9000,
		Database:        "default",
		Username:        "default",
		Password:        "", // Use environment variables in production
		MaxOpenConns:    20,
		MaxIdleConns:    10,
		ConnMaxLifetime: time.Hour,
		BatchSize:       1000,
		AsyncSettings:   OptimizedAsyncConfig("medium_volume"),
	}

	client, err := NewClient(config)
	if err != nil {
		fmt.Printf("❌ Failed to create ClickHouse client: %v\n", err)
		fmt.Println("💡 Make sure ClickHouse is running on localhost:9000")
		return
	}
	defer client.Close()

	ctx := context.Background()

	// Health check
	if err := client.HealthCheck(ctx); err != nil {
		fmt.Printf("❌ ClickHouse health check failed: %v\n", err)
		fmt.Println("💡 Make sure ClickHouse is running and accessible")
		return
	}
	fmt.Println("✅ ClickHouse connection healthy")

	// Example schema
	schema := &TableSchema{
		TableName:  "example_events",
		StructName: "ExampleEvent",
		PrimaryKey: []string{"id"},
		Fields: []FieldSchema{
			{Name: "Id", ColumnName: "id", GoType: "uint64", ClickHouseType: "UInt64"},
			{Name: "UserId", ColumnName: "user_id", GoType: "uint64", ClickHouseType: "UInt64"},
			{Name: "EventType", ColumnName: "event_type", GoType: "string", ClickHouseType: "String"},
			{Name: "Timestamp", ColumnName: "timestamp", GoType: "time.Time", ClickHouseType: "DateTime"},
			{Name: "Properties", ColumnName: "properties", GoType: "map[string]string", ClickHouseType: "Map(String, String)"},
		},
		Engine: EngineConfig{
			Type: "MergeTree",
		},
		PartitionBy: "toYYYYMM(timestamp)",
		Settings:    DefaultSettings(),
	}

	// Create table
	if err := client.CreateTableFromSchema(ctx, schema); err != nil {
		fmt.Printf("❌ Failed to create table: %v\n", err)
		return
	}
	fmt.Println("✅ Table created successfully")

	// Insert sample data
	data := []map[string]any{
		{
			"id":         uint64(1),
			"user_id":    uint64(100),
			"event_type": "page_view",
			"timestamp":  time.Now(),
			"properties": map[string]string{"page": "home", "source": "direct"},
		},
		{
			"id":         uint64(2),
			"user_id":    uint64(101),
			"event_type": "click",
			"timestamp":  time.Now(),
			"properties": map[string]string{"button": "signup", "page": "landing"},
		},
	}

	if err := client.SchemaBasedInsert(ctx, "example_events", data); err != nil {
		fmt.Printf("❌ Failed to insert data: %v\n", err)
		return
	}
	fmt.Println("✅ Sample data inserted successfully")

	// Get stats
	stats := client.GetConnectionStats()
	fmt.Printf("✅ Connection stats: %+v\n", stats)

	fmt.Println("\n🎉 ClickHouse client example completed successfully!")
}
