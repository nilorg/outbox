package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/nilorg/eventbus"
	"gorm.io/gorm"
)

// NewGormTransaction ...
func NewGormTransaction(db *gorm.DB, eventBus eventbus.EventBus) (outbox Transactioner, err error) {
	db.AutoMigrate(
		Published{},
		Received{},
	)
	var node *snowflake.Node
	if node, err = snowflake.NewNode(1); err != nil {
		return
	}
	outbox = &GormTransaction{
		db:       db,
		eventBus: eventBus,
		node:     node,
	}
	return
}

// GormTransaction ...
type GormTransaction struct {
	db       *gorm.DB
	tx       *gorm.DB
	txMutex  sync.Mutex
	eventBus eventbus.EventBus
	node     *snowflake.Node
}

// Begin ...
func (o *GormTransaction) Begin(ctx context.Context, opts ...*sql.TxOptions) (tx interface{}, err error) {
	o.txMutex.Lock()
	defer o.txMutex.Unlock()

	gormTx := o.db.WithContext(ctx).Begin(opts...)
	if err = gormTx.Error; err != nil {
		return
	}
	o.tx = gormTx
	tx = gormTx
	return
}

// Rollback ...
func (o *GormTransaction) Rollback(ctx context.Context) (err error) {
	err = o.tx.WithContext(ctx).Rollback().Error
	return
}

// Commit ...
func (o *GormTransaction) Commit(ctx context.Context, args ...*CommitMessage) (err error) {
	if len(args) > 0 {
		for _, arg := range args {
			id := o.node.Generate().Int64()
			timeNow := time.Now()
			msg := &eventbus.Message{
				Header: eventbus.MessageHeader{
					MessageHeaderMsgIDKey:       fmt.Sprint(id),
					MessageHeaderMsgTopicKey:    arg.Topic,
					MessageHeaderMsgTypeKey:     reflect.TypeOf(arg.Value).Name(),
					MessageHeaderMsgSendTimeKey: timeNow.Format("2006-01-02 15:04:05"),
					MessageHeaderMsgCallbackKey: arg.CallbackTopic,
				},
				Value: arg.Value,
			}
			var value string
			if value, err = encodeValue(msg); err != nil {
				return
			}
			p := &Published{
				ID:         id,
				Version:    "v1",
				Topic:      arg.Topic,
				Value:      value,
				Retries:    0,
				CreatedAt:  timeNow,
				StatusName: StatusNameScheduled,
			}
			if pubErr := o.eventBus.Publish(ctx, arg.Topic, msg); pubErr != nil {
				// 使用日志组件，打印日志
			} else {
				p.StatusName = StatusNameSucceeded
			}
			// 记录发件箱 成功日志
			if err = o.insert(p); err != nil {
				return
			}
		}
	}
	err = o.tx.WithContext(ctx).Commit().Error
	return
}

func (o *GormTransaction) insert(p *Published) error {
	return o.tx.Create(p).Error
}

func (o *GormTransaction) changePublishState(msgID int64, state string) error {
	return o.db.Model(&Published{}).Where("id = ?", msgID).Updates(&Published{StatusName: state}).Error
}

func (o *GormTransaction) changeReceiveState(msgID int64, state string) error {
	return o.db.Model(&Received{}).Where("id = ?", msgID).Updates(&Received{StatusName: state}).Error
}
