package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// TableSchema represents a ClickHouse table schema
type TableSchema struct {
	TableName   string        `json:"table_name"`
	StructName  string        `json:"struct_name"`
	PrimaryKey  []string      `json:"primary_key"`
	Fields      []FieldSchema `json:"fields"`
	Engine      EngineConfig  `json:"engine"`       // Engine configuration
	PartitionBy string        `json:"partition_by"` // Partition expression
	OrderBy     []string      `json:"order_by"`     // Order by columns (optional, defaults to primary key)
	Settings    []string      `json:"settings"`     // Additional table settings
}

// EngineConfig represents ClickHouse table engine configuration
type EngineConfig struct {
	Type       string            `json:"type"`       // Engine type (e.g., "ReplacingMergeTree", "MergeTree")
	Parameters map[string]string `json:"parameters"` // Engine-specific parameters
}

// FieldSchema represents a field in the table
type FieldSchema struct {
	Name           string // Go field name
	ColumnName     string // ClickHouse column name
	GoType         string // Go type (e.g., "string", "uint64")
	ClickHouseType string // ClickHouse type (e.g., "String", "UInt64")
	Tag            string // Struct tag
}

// DefaultEngineConfig returns a default MergeTree configuration
func DefaultEngineConfig() EngineConfig {
	return EngineConfig{
		Type:       "MergeTree",
		Parameters: map[string]string{},
	}
}

// DefaultSettings returns default ClickHouse table settings
func DefaultSettings() []string {
	return []string{
		"index_granularity = 8192",
	}
}

// OptimizedAsyncConfig returns async configuration optimized for different workload patterns
func OptimizedAsyncConfig(workloadType string) AsyncConfig {
	switch workloadType {
	case "low_volume": // 10-20 inserts/sec
		return AsyncConfig{
			Enabled:                   true,
			Wait:                      false,
			AsyncInsertMaxSize:        50,               // 50 rows threshold
			AsyncInsertWait:           10 * time.Second, // 10 second timeout
			AsyncInsertThreads:        2,                // Fewer threads
			AsyncInsertMaxQueryNumber: 100,              // Smaller queue
			AsyncInsertStaleTimeout:   5 * time.Second,  // 5 sec cleanup
			AsyncInsertDeduplicate:    true,             // Enable dedup
			AsyncInsertUseAdaptive:    true,             // Adaptive timeout
		}
	case "medium_volume": // 100-500 inserts/sec
		return AsyncConfig{
			Enabled:                   true,
			Wait:                      false,
			AsyncInsertMaxSize:        500,             // 500 rows threshold
			AsyncInsertWait:           5 * time.Second, // 5 second timeout
			AsyncInsertThreads:        4,               // More threads
			AsyncInsertMaxQueryNumber: 500,             // Larger queue
			AsyncInsertStaleTimeout:   3 * time.Second, // Faster cleanup
			AsyncInsertDeduplicate:    true,
			AsyncInsertUseAdaptive:    true,
		}
	case "high_volume": // 1000+ inserts/sec
		return AsyncConfig{
			Enabled:                   true,
			Wait:                      false,
			AsyncInsertMaxSize:        2000,            // 2000 rows threshold
			AsyncInsertWait:           2 * time.Second, // 2 second timeout
			AsyncInsertThreads:        8,               // Max threads
			AsyncInsertMaxQueryNumber: 1000,            // Large queue
			AsyncInsertStaleTimeout:   1 * time.Second, // Fast cleanup
			AsyncInsertDeduplicate:    false,           // Skip dedup for speed
			AsyncInsertUseAdaptive:    true,
		}
	case "disabled": // Disable async inserts
		return AsyncConfig{
			Enabled: false,
		}
	default: // Default to low_volume
		return OptimizedAsyncConfig("low_volume")
	}
}

// DefaultAsyncConfig returns a basic async configuration
func DefaultAsyncConfig() AsyncConfig {
	return AsyncConfig{
		Enabled:                   false, // Disabled by default - user must opt-in
		Wait:                      false,
		AsyncInsertMaxSize:        1000,
		AsyncInsertWait:           60 * time.Second,
		AsyncInsertThreads:        4,
		AsyncInsertMaxQueryNumber: 100,
		AsyncInsertStaleTimeout:   10 * time.Second,
		AsyncInsertDeduplicate:    true,
		AsyncInsertUseAdaptive:    false,
	}
}

// buildAsyncSettings creates ClickHouse settings based on AsyncConfig
func buildAsyncSettings(config AsyncConfig) clickhouse.Settings {
	settings := clickhouse.Settings{}

	if config.Enabled {
		// Core async insert settings
		settings["async_insert"] = 1
		if config.Wait {
			settings["wait_for_async_insert"] = 1
		} else {
			settings["wait_for_async_insert"] = 0
		}

		// Buffer and timing settings (core settings that are widely supported)
		settings["async_insert_max_data_size"] = config.AsyncInsertMaxSize
		settings["async_insert_busy_timeout_ms"] = int(config.AsyncInsertWait.Milliseconds())

		// Performance settings (only include well-supported ones)
		settings["async_insert_threads"] = config.AsyncInsertThreads
		settings["async_insert_max_query_number"] = config.AsyncInsertMaxQueryNumber

		// Optional: stale timeout (if supported by ClickHouse version)
		if config.AsyncInsertStaleTimeout > 0 {
			settings["async_insert_stale_timeout_ms"] = int(config.AsyncInsertStaleTimeout.Milliseconds())
		}

		// Deduplication (widely supported)
		if config.AsyncInsertDeduplicate {
			settings["async_insert_deduplicate"] = 1
		}

		// Note: async_insert_use_adaptive_timeout may not be available in all ClickHouse versions
		// Skipping for compatibility
	} else {
		// Disable async inserts
		settings["async_insert"] = 0
	}

	return settings
}

// ClickHouseConfig holds configuration for ClickHouse connection
type ClickHouseConfig struct {
	Host            string
	Port            int
	Database        string
	Username        string
	Password        string
	MaxOpenConns    int           // Connection pool size
	MaxIdleConns    int           // Idle connections
	ConnMaxLifetime time.Duration // Connection lifetime
	BatchSize       int           // Optimal batch size
	AsyncSettings   AsyncConfig   // Async insert settings
}

// AsyncConfig holds async insert configuration
type AsyncConfig struct {
	Enabled                   bool          // Enable/disable async inserts
	Wait                      bool          // Wait for async insert completion
	WaitTimeout               time.Duration // Timeout for waiting
	AsyncInsertMaxSize        int           // Max size before flush (rows or bytes)
	AsyncInsertWait           time.Duration // Max wait time before flush
	AsyncInsertThreads        int           // Number of async insert threads
	AsyncInsertMaxQueryNumber int           // Max concurrent async queries
	AsyncInsertStaleTimeout   time.Duration // Stale data cleanup timeout
	AsyncInsertDeduplicate    bool          // Enable deduplication
	AsyncInsertUseAdaptive    bool          // Use adaptive timeout
}

// ClickHouseClient represents a ClickHouse client with performance optimizations
type ClickHouseClient struct {
	conn   driver.Conn
	config ClickHouseConfig

	preparedStmts sync.Map // Cache for prepared statements
	schemas       sync.Map // Cache for table schemas
	mu            sync.RWMutex
}

// NewClient creates a new optimized ClickHouse client
func NewClient(config ClickHouseConfig) (*ClickHouseClient, error) {
	// Validate required configuration
	if config.Host == "" {
		return nil, fmt.Errorf("host is required")
	}
	if config.Database == "" {
		return nil, fmt.Errorf("database is required")
	}
	if config.Username == "" {
		return nil, fmt.Errorf("username is required")
	}

	// Set default values for missing configuration
	if config.Port == 0 {
		config.Port = 9000
	}
	if config.MaxOpenConns == 0 {
		config.MaxOpenConns = 10
	}
	if config.MaxIdleConns == 0 {
		config.MaxIdleConns = 5
	}
	if config.ConnMaxLifetime == 0 {
		config.ConnMaxLifetime = time.Hour
	}
	if config.BatchSize == 0 {
		config.BatchSize = 1000
	}

	// Apply default async configuration if not provided
	if config.AsyncSettings == (AsyncConfig{}) {
		config.AsyncSettings = DefaultAsyncConfig()
	}

	// Set sensible defaults for any missing async values
	if config.AsyncSettings.AsyncInsertMaxSize == 0 {
		config.AsyncSettings.AsyncInsertMaxSize = 1000
	}
	if config.AsyncSettings.AsyncInsertWait == 0 {
		config.AsyncSettings.AsyncInsertWait = 60 * time.Second
	}
	if config.AsyncSettings.AsyncInsertThreads == 0 {
		config.AsyncSettings.AsyncInsertThreads = 4
	}
	if config.AsyncSettings.AsyncInsertMaxQueryNumber == 0 {
		config.AsyncSettings.AsyncInsertMaxQueryNumber = 100
	}
	if config.AsyncSettings.AsyncInsertStaleTimeout == 0 {
		config.AsyncSettings.AsyncInsertStaleTimeout = 10 * time.Second
	}

	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
		DialTimeout:          30 * time.Second,
		MaxOpenConns:         config.MaxOpenConns,
		MaxIdleConns:         config.MaxIdleConns,
		ConnMaxLifetime:      config.ConnMaxLifetime,
		ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
		// Configure async insert settings at connection level based on user config
		Settings: buildAsyncSettings(config.AsyncSettings),
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse at %s:%d: %w", config.Host, config.Port, err)
	}

	// Test connection with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		conn.Close() // Clean up connection on ping failure
		return nil, fmt.Errorf("failed to ping ClickHouse at %s:%d: %w", config.Host, config.Port, err)
	}

	client := &ClickHouseClient{
		conn:   conn,
		config: config,
	}

	return client, nil
}

func (c *ClickHouseClient) Close() error {
	return c.conn.Close()
}

func (c *ClickHouseClient) RegisterTableSchema(schema *TableSchema) {
	c.schemas.Store(schema.TableName, schema)
}

func (c *ClickHouseClient) SchemaBasedInsert(ctx context.Context, tableName string, data []map[string]any) error {
	// Validate inputs
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if len(data) == 0 {
		return nil // No data to insert is not an error
	}

	schemaInterface, ok := c.schemas.Load(tableName)
	if !ok {
		return fmt.Errorf("schema not registered for table %s. Use CreateTableFromSchema() or RegisterTableSchema() first", tableName)
	}
	schema := schemaInterface.(*TableSchema)

	// Process data in batches
	batchSize := c.config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000 // Fallback batch size
	}

	for i := 0; i < len(data); i += batchSize {
		end := min(i+batchSize, len(data))
		chunk := data[i:end]

		if err := c.schemaBasedInsertChunk(ctx, schema, chunk); err != nil {
			return fmt.Errorf("schema-based insert failed for table %s at rows %d-%d: %w", tableName, i+1, end, err)
		}
	}

	return nil
}

func (c *ClickHouseClient) schemaBasedInsertChunk(ctx context.Context, schema *TableSchema, data []map[string]any) error {
	stmtKey := fmt.Sprintf("schema_insert_%s", schema.TableName)
	var batch driver.Batch
	var err error

	if cachedStmt, ok := c.preparedStmts.Load(stmtKey); ok {
		batch, err = c.conn.PrepareBatch(ctx, cachedStmt.(string))
	} else {
		columns := make([]string, len(schema.Fields))
		for i, field := range schema.Fields {
			columns[i] = field.ColumnName
		}

		query := fmt.Sprintf("INSERT INTO %s (%s)", schema.TableName, strings.Join(columns, ", "))
		c.preparedStmts.Store(stmtKey, query)
		batch, err = c.conn.PrepareBatch(ctx, query)
	}

	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, item := range data {
		values := make([]any, len(schema.Fields))
		for i, field := range schema.Fields {
			if value, exists := item[field.ColumnName]; exists {
				values[i] = value
			} else {
				values[i] = nil // Default value for missing fields
			}
		}

		if err := batch.Append(values...); err != nil {
			return fmt.Errorf("failed to append to batch: %w", err)
		}
	}

	return batch.Send()
}

func (c *ClickHouseClient) CreateTableFromSchema(ctx context.Context, schema *TableSchema) error {
	// Validate schema
	if schema == nil {
		return fmt.Errorf("schema cannot be nil")
	}
	if schema.TableName == "" {
		return fmt.Errorf("table name is required")
	}
	if len(schema.Fields) == 0 {
		return fmt.Errorf("at least one field is required")
	}
	if len(schema.PrimaryKey) == 0 {
		return fmt.Errorf("primary key is required")
	}
	if schema.PartitionBy == "" {
		return fmt.Errorf("partition by expression is required")
	}

	// Validate that all primary key columns exist in fields
	for _, pkCol := range schema.PrimaryKey {
		found := false
		for _, field := range schema.Fields {
			if field.ColumnName == pkCol {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("primary key column '%s' not found in fields", pkCol)
		}
	}

	var columnDefs []string
	for _, field := range schema.Fields {
		if field.ColumnName == "" || field.ClickHouseType == "" {
			return fmt.Errorf("field column name and ClickHouse type are required")
		}
		columnDef := fmt.Sprintf("%s %s", field.ColumnName, field.ClickHouseType)
		columnDefs = append(columnDefs, columnDef)
	}

	// Set default engine if not provided
	if schema.Engine.Type == "" {
		schema.Engine.Type = "MergeTree"
	}

	engineClause := schema.Engine.Type
	if len(schema.Engine.Parameters) > 0 {
		var params []string
		// Handle special case for ReplacingMergeTree version parameter
		if schema.Engine.Type == "ReplacingMergeTree" {
			if version, ok := schema.Engine.Parameters["version"]; ok {
				params = append(params, version) // Just the column name, not key=value
			}
			// Add other parameters if needed (though ReplacingMergeTree typically only has version)
			for key, value := range schema.Engine.Parameters {
				if key != "version" {
					params = append(params, fmt.Sprintf("%s=%s", key, value))
				}
			}
		} else {
			// For other engines, use key=value format
			for key, value := range schema.Engine.Parameters {
				params = append(params, fmt.Sprintf("%s=%s", key, value))
			}
		}
		if len(params) > 0 {
			engineClause += fmt.Sprintf("(%s)", strings.Join(params, ", "))
		}
	}

	orderBy := schema.OrderBy
	if len(orderBy) == 0 {
		orderBy = schema.PrimaryKey // Default to primary key if not specified
	}

	settingsClause := ""
	if len(schema.Settings) > 0 {
		settingsClause = fmt.Sprintf("\nSETTINGS %s", strings.Join(schema.Settings, ", "))
	}

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			%s
		) ENGINE = %s
		PARTITION BY %s
		PRIMARY KEY (%s)
		ORDER BY (%s)%s
	`, schema.TableName,
		strings.Join(columnDefs, ",\n\t\t\t"),
		engineClause,
		schema.PartitionBy,
		strings.Join(schema.PrimaryKey, ", "),
		strings.Join(orderBy, ", "),
		settingsClause)

	// Execute with context
	if err := c.conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create table %s: %w", schema.TableName, err)
	}

	// Register schema after successful creation
	c.RegisterTableSchema(schema)

	return nil
}

func (c *ClickHouseClient) GetConnectionStats() map[string]any {
	return map[string]any{
		"max_open_conns":      c.config.MaxOpenConns,
		"max_idle_conns":      c.config.MaxIdleConns,
		"conn_max_lifetime":   c.config.ConnMaxLifetime,
		"batch_size":          c.config.BatchSize,
		"prepared_stmt_count": c.getPreparedStmtCount(),
	}
}

func (c *ClickHouseClient) getPreparedStmtCount() int {
	count := 0
	c.preparedStmts.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func (c *ClickHouseClient) ClearPreparedStatements() {
	c.preparedStmts = sync.Map{}
}

func (c *ClickHouseClient) Exec(ctx context.Context, query string, args ...any) error {
	return c.conn.Exec(ctx, query, args...)
}

func (c *ClickHouseClient) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

// UpdateAsyncSettings allows runtime adjustment of async insert settings
func (c *ClickHouseClient) UpdateAsyncSettings(ctx context.Context, newConfig AsyncConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update internal config
	c.config.AsyncSettings = newConfig

	// Build all async settings
	settings := buildAsyncSettings(newConfig)

	// Apply settings to current session
	for key, value := range settings {
		query := fmt.Sprintf("SET %s = %v", key, value)
		if err := c.conn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to update setting %s: %w", key, err)
		}
	}

	return nil
}

// GetAsyncSettings returns current async insert configuration
func (c *ClickHouseClient) GetAsyncSettings() AsyncConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config.AsyncSettings
}

// HealthCheck performs a health check on the ClickHouse connection
func (c *ClickHouseClient) HealthCheck(ctx context.Context) error {
	if c.conn == nil {
		return fmt.Errorf("connection is nil")
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := c.conn.Ping(pingCtx); err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	return nil
}

// IsConnected checks if the client is connected to ClickHouse
func (c *ClickHouseClient) IsConnected() bool {
	if c.conn == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	return c.conn.Ping(ctx) == nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
