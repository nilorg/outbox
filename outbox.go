package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/nilorg/eventbus"
	"gorm.io/gorm"
)

// Transactioner 事务接口
type Transactioner interface {
	Begin(ctx context.Context, opts ...*sql.TxOptions) (tx interface{}, err error)
	Rollback(ctx context.Context) (err error)
	Commit(ctx context.Context, args ...*CommitMessage) (err error)
}

// Subscriber 订阅接口
type Subscriber interface {
	Subscribe(ctx context.Context, topic string, h eventbus.SubscribeHandler) (err error)
	SubscribeAsync(ctx context.Context, topic string, h eventbus.SubscribeHandler) (err error)
}

// Publisher 发布接口
type Publisher interface {
	Publish(ctx context.Context, topic string, v interface{}) (err error)
	PublishAsync(ctx context.Context, topic string, v interface{}) (err error)
}

const (
	// EngineTypeGorm ...
	EngineTypeGorm = "gorm"
)

// Engine ...
type Engine interface {
	Subscribe(ctx context.Context, topic string, h eventbus.SubscribeHandler) (err error)
	SubscribeAsync(ctx context.Context, topic string, h eventbus.SubscribeHandler) (err error)
	Publish(ctx context.Context, topic string, v interface{}, callback ...string) (err error)
	PublishAsync(ctx context.Context, topic string, v interface{}, callback ...string) (err error)
}

// gormEngine ...
type gormEngine struct {
	db          *gorm.DB
	node        *snowflake.Node
	transaction Transactioner
	eventBus    eventbus.EventBus
	logger      Logger
}

// Subscribe ...
func (e *gormEngine) Subscribe(ctx context.Context, topic string, h eventbus.SubscribeHandler) (err error) {
	return e.eventBus.Subscribe(ctx, topic, h)
}

// SubscribeAsync ...
func (e *gormEngine) SubscribeAsync(ctx context.Context, topic string, h eventbus.SubscribeHandler) (err error) {
	return e.SubscribeAsync(ctx, topic, h)
}

func (e *gormEngine) subscribe(ctx context.Context, db *gorm.DB, topic string, h eventbus.SubscribeHandler, async bool) (err error) {
	group, _ := eventbus.FromGroupIDContext(ctx)
	if async {
		return e.SubscribeAsync(ctx, topic, e.newSubscribeHandler(db, topic, group, h))
	}
	return e.Subscribe(ctx, topic, e.newSubscribeHandler(db, topic, group, h))
}

func (e *gormEngine) newSubscribeHandler(db *gorm.DB, topic, group string, h eventbus.SubscribeHandler) eventbus.SubscribeHandler {
	return func(ctx context.Context, msg *eventbus.Message) (err error) {
		id := e.node.Generate().Int64()
		var value string
		if value, err = encodeValue(msg); err != nil {
			return
		}
		r := &Received{
			ID:         id,
			Version:    Version,
			Topic:      topic,
			Group:      group,
			Value:      value,
			Retries:    0,
			CreatedAt:  time.Now(),
			StatusName: StatusNameScheduled,
		}
		hErr := h(ctx, msg)
		if hErr != nil {
			// 使用日志组件，打印日志
			e.logger.Errorf(ctx, "exec subscribe handler error: %s", hErr)
		} else {
			r.StatusName = StatusNameSucceeded
		}
		// 记录发件箱 成功日志
		if err = e.insertReceivedFromGorm(db, r); err != nil {
			return
		}
		return
	}
}

// Publish ...
func (e *gormEngine) Publish(ctx context.Context, topic string, v interface{}, callback ...string) (err error) {
	return e.publish(ctx, e.db, topic, v, false, callback...)
}

// PublishAsync ...
func (e *gormEngine) PublishAsync(ctx context.Context, topic string, v interface{}, callback ...string) (err error) {
	return e.publish(ctx, e.db, topic, v, true, callback...)
}

func (e *gormEngine) publish(ctx context.Context, db *gorm.DB, topic string, v interface{}, async bool, callback ...string) (err error) {
	id := e.node.Generate().Int64()
	timeNow := time.Now()
	callbackName := ""
	if len(callback) > 0 {
		callbackName = callback[0]
	}
	msg := &eventbus.Message{
		Header: eventbus.MessageHeader{
			MessageHeaderMsgIDKey:       fmt.Sprint(id),
			MessageHeaderMsgTopicKey:    topic,
			MessageHeaderMsgTypeKey:     reflect.TypeOf(v).Name(),
			MessageHeaderMsgSendTimeKey: timeNow.Format("2006-01-02 15:04:05"),
			MessageHeaderMsgCallbackKey: callbackName,
		},
		Value: v,
	}
	var value string
	if value, err = encodeValue(msg); err != nil {
		return
	}
	p := &Published{
		ID:         id,
		Version:    Version,
		Topic:      topic,
		Value:      value,
		Retries:    0,
		CreatedAt:  timeNow,
		StatusName: StatusNameScheduled,
	}
	var pubErr error
	if async {
		pubErr = e.eventBus.PublishAsync(ctx, topic, msg)
	} else {
		pubErr = e.eventBus.Publish(ctx, topic, msg)
	}
	if pubErr != nil {
		// 使用日志组件，打印日志
	} else {
		p.StatusName = StatusNameSucceeded
	}
	// 记录发件箱 成功日志
	if err = e.insertPublishedFromGorm(db, p); err != nil {
		return
	}
	return
}

func (e *gormEngine) insertPublishedFromGorm(db *gorm.DB, p *Published) error {
	return db.Create(p).Error
}

func (e *gormEngine) insertReceivedFromGorm(db *gorm.DB, r *Received) error {
	return db.Create(r).Error
}

func (e *gormEngine) changePublishState(db *gorm.DB, msgID int64, state string) error {
	return db.Model(&Published{}).Where("id = ?", msgID).Updates(&Published{StatusName: state}).Error
}

func (e *gormEngine) changeReceiveState(db *gorm.DB, msgID int64, state string) error {
	return db.Model(&Received{}).Where("id = ?", msgID).Updates(&Received{StatusName: state}).Error
}

// New 创建
func New(typ string, v interface{}, eventBus eventbus.EventBus) (engine Engine, err error) {
	var (
		transaction Transactioner
	)
	if typ == EngineTypeGorm {
		// rmqOpt := eventbus.DefaultRabbitMQOptions
		// rmqOpt.ExchangeName = "nilorg.outbox.v1"
		// rmqOpt.DefaultGroupID = "nilorg.outbox.default.group.v1"
		// if eventBus, err = eventbus.NewRabbitMQ(conn, &rmqOpt); err != nil {
		// 	return
		// }
		db := v.(*gorm.DB)
		if transaction, err = NewGormTransaction(db, eventBus); err != nil {
			return
		}
		engine = &gormEngine{
			db:          db,
			transaction: transaction,
			eventBus:    eventBus,
		}
	} else {
		err = errors.New("type is error")
		return
	}
	return
}
