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
	Wait               bool          // Wait for async insert completion
	WaitTimeout        time.Duration // Timeout for waiting
	AsyncInsertMaxSize int           // Max size before flush
	AsyncInsertWait    time.Duration // Max wait time before flush
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
	if config.AsyncSettings.AsyncInsertMaxSize == 0 {
		config.AsyncSettings.AsyncInsertMaxSize = 1048576 // 1MB default
	}
	if config.AsyncSettings.AsyncInsertWait == 0 {
		config.AsyncSettings.AsyncInsertWait = 100 * time.Millisecond // 100ms default
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
		// Configure async insert settings at connection level
		Settings: clickhouse.Settings{
			"async_insert":                 1, // Enable async inserts globally
			"async_insert_max_data_size":   config.AsyncSettings.AsyncInsertMaxSize,
			"async_insert_busy_timeout_ms": int(config.AsyncSettings.AsyncInsertWait.Milliseconds()),
		},
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
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
	if len(data) == 0 {
		return nil
	}

	schemaInterface, ok := c.schemas.Load(tableName)
	if !ok {
		return fmt.Errorf("schema not registered for table %s. Use RegisterTableSchema() first", tableName)
	}
	schema := schemaInterface.(*TableSchema)

	for i := 0; i < len(data); i += c.config.BatchSize {
		end := min(i+c.config.BatchSize, len(data))
		chunk := data[i:end]

		if err := c.schemaBasedInsertChunk(ctx, schema, chunk); err != nil {
			return fmt.Errorf("schema-based insert failed at chunk %d-%d: %w", i, end, err)
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
	var columnDefs []string
	for _, field := range schema.Fields {
		columnDef := fmt.Sprintf("%s %s", field.ColumnName, field.ClickHouseType)
		columnDefs = append(columnDefs, columnDef)
	}

	engineClause := schema.Engine.Type
	if len(schema.Engine.Parameters) > 0 {
		var params []string
		for key, value := range schema.Engine.Parameters {
			params = append(params, fmt.Sprintf("%s=%s", key, value))
		}
		engineClause += fmt.Sprintf("(%s)", strings.Join(params, ", "))
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

	c.RegisterTableSchema(schema)

	return c.conn.Exec(ctx, query)
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
