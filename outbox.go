package outbox

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/nilorg/eventbus"
	"gorm.io/gorm"
)

// CommitMessage 提交message
type CommitMessage struct {
	Topic         string
	Value         interface{}
	CallbackTopic string
}

// TransactionHandler ...
type TransactionHandler func(ctx context.Context, db interface{}) error

// Transactioner 事务接口
type Transactioner interface {
	Rollback(ctx context.Context) (err error)
	Commit(ctx context.Context, args ...*CommitMessage) (err error)
	Session() interface{}
}

// SubscribeHandler 订阅处理
type SubscribeHandler func(ctx context.Context, msg *Message) error

// Subscriber 订阅接口
type Subscriber interface {
	Subscribe(ctx context.Context, topic string, h SubscribeHandler) (err error)
	SubscribeAsync(ctx context.Context, topic string, h SubscribeHandler) (err error)
}

// Publisher 发布接口
type Publisher interface {
	Publish(ctx context.Context, topic string, v interface{}, callback ...string) (err error)
	PublishAsync(ctx context.Context, topic string, v interface{}, callback ...string) (err error)
}

// subscribeItem 订阅项
type subscribeItem struct {
	Topic string
	Group string
	h     SubscribeHandler
}

// Engine ...
type Engine interface {
	Publisher
	Subscriber
	Begin(ctx context.Context, opts ...*sql.TxOptions) (tx Transactioner, err error)
	Transaction(ctx context.Context, h TransactionHandler, args ...*CommitMessage) (err error)
}

var (
	// DefaultEngineOptions 默认选项
	DefaultEngineOptions = EngineOptions{
		FailedRetryInterval:        time.Minute,
		FailedRetryCount:           50,
		DataCleanInterval:          time.Hour,
		SucceedMessageExpiredAfter: 24 * time.Hour,
		SnowflakeNode:              1,
		Logger:                     &eventbus.StdLogger{},
	}
)

const (
	// CallbackTypePublished ...
	CallbackTypePublished = "Published"
	// CallbackTypeReceived ...
	CallbackTypeReceived = "Received"
)

// FailedThresholdCallbackHandler 重试阈值的失败回调处理
type FailedThresholdCallbackHandler func(ctx context.Context, typ string, v interface{})

// EngineOptions ...
type EngineOptions struct {
	FailedRetryInterval        time.Duration                  // 失败重试间隔时间
	FailedRetryCount           int                            // 最大重试次数
	FailedThresholdCallback    FailedThresholdCallbackHandler // 重试阈值的失败回调
	SucceedMessageExpiredAfter time.Duration                  // 成功消息的过期时间
	DataCleanInterval          time.Duration                  // 数据清理间隔
	SnowflakeNode              int64                          // snowflake节点数
	Logger                     Logger                         // 日志接口
}

// New 创建
func New(typ string, v interface{}, eventBus eventbus.EventBus, options ...*EngineOptions) (engine Engine, err error) {
	if typ == EngineTypeGorm {
		db := v.(*gorm.DB)
		db.AutoMigrate(
			Published{},
			Received{},
		)

		var opts EngineOptions
		if len(options) == 0 {
			opts = DefaultEngineOptions
		} else {
			opts = *options[0]
		}

		var node *snowflake.Node
		if node, err = snowflake.NewNode(opts.SnowflakeNode); err != nil {
			return
		}
		ge := &gormEngine{
			db:             db,
			eventBus:       eventBus,
			node:           node,
			options:        &opts,
			subscribeItems: make(map[string]*subscribeItem),
			logger:         opts.Logger,
		}
		// 启动哨兵，处理出错数据
		ge.sentry()
		engine = ge
	} else {
		err = errors.New("outbox engine type is error")
		return
	}
	return
}
