package outbox

import (
	"context"
	"database/sql"
	"errors"

	"github.com/bwmarrin/snowflake"
	"github.com/nilorg/eventbus"
	"gorm.io/gorm"
)

// TransactionHandler ...
type TransactionHandler func(ctx context.Context, db interface{}) error

// Transactioner 事务接口
type Transactioner interface {
	Begin(ctx context.Context, opts ...*sql.TxOptions) (tx interface{}, err error)
	Rollback(ctx context.Context) (err error)
	Commit(ctx context.Context, args ...*CommitMessage) (err error)
	Transaction(ctx context.Context, h TransactionHandler, args ...*CommitMessage) (err error)
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

// Engine ...
type Engine interface {
	Publisher
	Subscriber
	Transactioner
}

// New 创建
func New(typ string, v interface{}, eventBus eventbus.EventBus) (engine Engine, err error) {
	if typ == EngineTypeGorm {
		db := v.(*gorm.DB)
		db.AutoMigrate(
			Published{},
			Received{},
		)
		node, _ := snowflake.NewNode(1)
		engine = &gormEngine{
			db:       db,
			eventBus: eventBus,
			node:     node,
		}
	} else {
		err = errors.New("outbox engine type is error")
		return
	}
	return
}
