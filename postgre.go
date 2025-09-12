package graphsplit

import (
	"context"
	"fmt"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var dbLog = logging.Logger("graphsplit/db")

// DBConfig 数据库配置
type DBConfig struct {
	DSN             string        `json:"dsn" toml:"dsn"`
	MaxIdleConns    int           `json:"max_idle_conns" toml:"max_idle_conns"`
	MaxOpenConns    int           `json:"max_open_conns" toml:"max_open_conns"`
	ConnMaxLifetime time.Duration `json:"conn_max_lifetime" toml:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `json:"conn_max_idle_time" toml:"conn_max_idle_time"`
	LogLevel        string        `json:"log_level" toml:"log_level"`
}

// DefaultDBConfig 返回默认数据库配置
func DefaultDBConfig() *DBConfig {
	return &DBConfig{
		MaxIdleConns:    10,
		MaxOpenConns:    100,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: time.Minute * 30,
		LogLevel:        "warn",
	}
}

// DBManager 数据库连接池管理器
type DBManager struct {
	DB     *gorm.DB
	config *DBConfig
}

// NewDBManager 创建数据库管理器
func NewDBManager(config *DBConfig) (*DBManager, error) {
	if config == nil {
		config = DefaultDBConfig()
	}
	// 配置GORM日志级别
	var logLevel logger.LogLevel
	switch config.LogLevel {
	case "silent":
		logLevel = logger.Silent
	case "error":
		logLevel = logger.Error
	case "warn":
		logLevel = logger.Warn
	case "info":
		logLevel = logger.Info
	default:
		logLevel = logger.Warn
	}

	// 打开PostgreSQL连接
	db, err := gorm.Open(postgres.Open(config.DSN), &gorm.Config{
		Logger: logger.Default.LogMode(logLevel),
		NowFunc: func() time.Time {
			return time.Now().UTC()
		},
		PrepareStmt:                              true, // 预编译语句，提高性能
		DisableForeignKeyConstraintWhenMigrating: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// 配置连接池
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}

	sqlDB.SetMaxIdleConns(config.MaxIdleConns)
	sqlDB.SetMaxOpenConns(config.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(config.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(config.ConnMaxIdleTime)

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := sqlDB.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	dbLog.Info("Database connection established successfully")

	return &DBManager{
		DB:     db,
		config: config,
	}, nil
}

// Close 关闭数据库连接
func (m *DBManager) Close() error {
	sqlDB, err := m.DB.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB: %w", err)
	}

	dbLog.Info("Closing database connection")
	return sqlDB.Close()
}

// Ping 检查数据库连接
func (m *DBManager) Ping(ctx context.Context) error {
	sqlDB, err := m.DB.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB: %w", err)
	}
	return sqlDB.PingContext(ctx)
}

// Migrate 执行数据库迁移
func (m *DBManager) Migrate() error {
	dbLog.Info("Starting database migration")

	// AutoMigrate 基本模型
	if err := m.DB.AutoMigrate(&PieceManifest{}); err != nil {
		return fmt.Errorf("failed to migrate PieceManifest: %w", err)
	}

	// 自定义约束（如果 AutoMigrate 未完美处理）
	if err := m.DB.Exec(`
        ALTER TABLE piece_manifests 
        ADD CONSTRAINT IF NOT EXISTS chk_payload_size CHECK (payload_size > 0),
        ADD CONSTRAINT IF NOT EXISTS chk_piece_size CHECK (piece_size > 0);
    `).Error; err != nil {
		return fmt.Errorf("failed to add CHECK constraints: %w", err)
	}

	// 确保唯一索引（GORM 已处理，但显式添加）
	if err := m.DB.Exec(`
        CREATE UNIQUE INDEX IF NOT EXISTS idx_payload_cid ON piece_manifests (payload_cid);
    `).Error; err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("failed to create unique index: %w", err)
	}

	dbLog.Info("Database migration completed")
	return nil
}

// GetStats 获取连接池统计信息
func (m *DBManager) GetStats() (map[string]interface{}, error) {
	sqlDB, err := m.DB.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}

	stats := sqlDB.Stats()
	return map[string]interface{}{
		"max_open_connections": stats.MaxOpenConnections,
		"open_connections":     stats.OpenConnections,
		"in_use":               stats.InUse,
		"idle":                 stats.Idle,
		"wait_count":           stats.WaitCount,
		"wait_duration":        stats.WaitDuration.String(),
		"max_idle_closed":      stats.MaxIdleClosed,
		"max_idle_time_closed": stats.MaxIdleTimeClosed,
		"max_lifetime_closed":  stats.MaxLifetimeClosed,
	}, nil
}

// Transaction 执行事务
func (m *DBManager) Transaction(fn func(*gorm.DB) error) error {
	return m.DB.Transaction(fn)
}

// PieceManifest 改进的模型定义
type PieceManifest struct {
	ID          uint           `gorm:"primaryKey;autoIncrement" json:"id"`
	PayloadCID  string         `gorm:"type:varchar(255);not null;uniqueIndex:idx_payload_cid" json:"payload_cid" validate:"required,alphanum"`
	Filename    string         `gorm:"type:varchar(1024);not null" json:"filename" validate:"required,min=1,max=1024"`
	PieceCID    string         `gorm:"type:varchar(255);not null;index:idx_piece_cid" json:"piece_cid" validate:"required,alphanum"`
	PayloadSize int64          `gorm:"type:bigint;not null;check:payload_size > 0" json:"payload_size" validate:"min=1"`
	PieceSize   int64          `gorm:"type:bigint;not null;check:piece_size > 0" json:"piece_size" validate:"min=1"`
	Detail      string         `gorm:"type:text" json:"detail" validate:"omitempty"`
	Status      string         `gorm:"type:varchar(50);not null;default:'pending';index:idx_status" json:"status" validate:"oneof=pending processing completed failed"`
	CreatedAt   time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt   time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
	DeletedAt   gorm.DeletedAt `gorm:"index" json:"-"`
}

// TableName 指定表名
func (PieceManifest) TableName() string {
	return "piece_manifests"
}

// PieceManifestRepository 数据访问层
type PieceManifestRepository struct {
	db *gorm.DB
}

// NewPieceManifestRepository 创建仓库实例
func NewPieceManifestRepository(db *gorm.DB) *PieceManifestRepository {
	return &PieceManifestRepository{db: db}
}

// Create 创建记录（添加验证）
func (r *PieceManifestRepository) Create(ctx context.Context, manifest *PieceManifest) error {
	// 验证（手动简单验证，假设未集成完整validator库）
	if manifest.PayloadCID == "" || manifest.Filename == "" || manifest.PieceCID == "" {
		return fmt.Errorf("validation failed: required fields missing")
	}
	if manifest.PayloadSize <= 0 || manifest.PieceSize <= 0 {
		return fmt.Errorf("validation failed: sizes must be positive")
	}
	if manifest.Status != "pending" && manifest.Status != "processing" && manifest.Status != "completed" && manifest.Status != "failed" {
		return fmt.Errorf("validation failed: invalid status")
	}

	// 检查唯一性
	if existing, _ := r.GetByPayloadCID(ctx, manifest.PayloadCID); existing != nil {
		return fmt.Errorf("duplicate payload_cid: %s", manifest.PayloadCID)
	}

	return r.db.WithContext(ctx).Create(manifest).Error
}

// GetByPayloadCID 根据PayloadCID查询
func (r *PieceManifestRepository) GetByPayloadCID(ctx context.Context, payloadCID string) (*PieceManifest, error) {
	var manifest PieceManifest
	err := r.db.WithContext(ctx).Where("payload_cid = ?", payloadCID).First(&manifest).Error
	if err != nil {
		return nil, err
	}
	return &manifest, nil
}

// GetByPieceCID 根据PieceCID查询
func (r *PieceManifestRepository) GetByPieceCID(ctx context.Context, pieceCID string) ([]*PieceManifest, error) {
	var manifests []*PieceManifest
	err := r.db.WithContext(ctx).Where("piece_cid = ?", pieceCID).Find(&manifests).Error
	return manifests, err
}

// List 分页查询
func (r *PieceManifestRepository) List(ctx context.Context, offset, limit int, status string) ([]*PieceManifest, int64, error) {
	var manifests []*PieceManifest
	var total int64

	query := r.db.WithContext(ctx).Model(&PieceManifest{})
	if status != "" {
		query = query.Where("status = ?", status)
	}

	// 获取总数
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	// 获取分页数据
	if err := query.Offset(offset).Limit(limit).Order("created_at DESC").Find(&manifests).Error; err != nil {
		return nil, 0, err
	}

	return manifests, total, nil
}

// Update 更新记录
func (r *PieceManifestRepository) Update(ctx context.Context, id uint, updates map[string]interface{}) error {
	return r.db.WithContext(ctx).Model(&PieceManifest{}).Where("id = ?", id).Updates(updates).Error
}

// Delete 删除记录
func (r *PieceManifestRepository) Delete(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).Delete(&PieceManifest{}, id).Error
}

// BatchCreate 批量创建
func (r *PieceManifestRepository) BatchCreate(ctx context.Context, manifests []*PieceManifest, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 100
	}
	return r.db.WithContext(ctx).CreateInBatches(manifests, batchSize).Error
}

// GetStatsByStatus 按状态统计（添加总数）
func (r *PieceManifestRepository) GetStatsByStatus(ctx context.Context) (map[string]int64, int64, error) {
	var results []struct {
		Status string
		Count  int64
	}
	var total int64

	// 获取总数
	if err := r.db.WithContext(ctx).Model(&PieceManifest{}).Count(&total).Error; err != nil {
		return nil, 0, err
	}

	// 按状态统计
	err := r.db.WithContext(ctx).
		Model(&PieceManifest{}).
		Select("status, count(*) as count").
		Group("status").
		Find(&results).Error

	if err != nil {
		return nil, total, err
	}

	stats := make(map[string]int64)
	for _, result := range results {
		stats[result.Status] = result.Count
	}

	return stats, total, nil
}
