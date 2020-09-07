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

const (
	// EngineTypeGorm ...
	EngineTypeGorm = "gorm"
)

// gormEngine ...
type gormEngine struct {
	db       *gorm.DB
	tx       *gorm.DB
	txFlag   bool
	txMutex  sync.Mutex
	node     *snowflake.Node
	eventBus eventbus.EventBus
	logger   Logger
}

// Subscribe ...
func (e *gormEngine) Subscribe(ctx context.Context, topic string, h SubscribeHandler) (err error) {
	return e.subscribe(ctx, topic, h, false)
}

// SubscribeAsync ...
func (e *gormEngine) SubscribeAsync(ctx context.Context, topic string, h SubscribeHandler) (err error) {
	return e.subscribe(ctx, topic, h, true)
}

func (e *gormEngine) subscribe(ctx context.Context, topic string, h SubscribeHandler, async bool) (err error) {
	group, _ := eventbus.FromGroupIDContext(ctx)
	if async {
		return e.eventBus.SubscribeAsync(ctx, topic, e.newSubscribeHandler(topic, group, h))
	}
	return e.eventBus.Subscribe(ctx, topic, e.newSubscribeHandler(topic, group, h))
}

func (e *gormEngine) newSubscribeHandler(topic, group string, h SubscribeHandler) eventbus.SubscribeHandler {
	return func(ctx context.Context, baseMsg *eventbus.Message) (err error) {
		id := e.node.Generate().Int64()
		var value string
		if value, err = encodeValue(baseMsg); err != nil {
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
		msg := Message(*baseMsg)
		hErr := h(ctx, &msg)
		if hErr != nil {
			// 下次自动执行时间
			exp := time.Now().Add(5 * time.Minute)
			r.ExpiresAt = &exp
			// 使用日志组件，打印日志
			if group != "" {
				e.logger.Errorf(ctx, "exec subscribe %s handler error: %s", topic, hErr)
			} else {
				e.logger.Errorf(ctx, "exec subscribe %s(%s) handler error: %s", topic, group, hErr)
			}
		} else {
			r.StatusName = StatusNameSucceeded
		}
		// 记录发件箱 成功日志
		if err = e.insertReceivedFromGorm(e.db, r); err != nil {
			return
		}
		return
	}
}

// Publish ...
func (e *gormEngine) Publish(ctx context.Context, topic string, v interface{}, callback ...string) (err error) {
	return e.publish(ctx, topic, v, false, callback...)
}

// PublishAsync ...
func (e *gormEngine) PublishAsync(ctx context.Context, topic string, v interface{}, callback ...string) (err error) {
	return e.publish(ctx, topic, v, true, callback...)
}

func (e *gormEngine) publish(ctx context.Context, topic string, v interface{}, async bool, callback ...string) (err error) {
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
		// 下次自动执行时间
		exp := time.Now().Add(5 * time.Minute)
		p.ExpiresAt = &exp
		// 使用日志组件，打印日志
		e.logger.Errorf(ctx, "exec async:%v publish %s error: %s", async, topic, pubErr)
	} else {
		p.StatusName = StatusNameSucceeded
	}
	var db *gorm.DB
	if e.txFlag {
		db = e.tx
	} else {
		db = e.db
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

// Begin ...
func (e *gormEngine) Begin(ctx context.Context, opts ...*sql.TxOptions) (tx interface{}, err error) {
	e.txMutex.Lock()
	defer e.txMutex.Unlock()
	gormTx := e.db.WithContext(ctx).Begin(opts...)
	if err = gormTx.Error; err != nil {
		e.tx = nil
		return
	}
	e.tx = gormTx
	e.txFlag = true

	tx = gormTx
	return
}

// Rollback ...
func (e *gormEngine) Rollback(ctx context.Context) (err error) {
	e.txMutex.Lock()
	defer e.txMutex.Unlock()
	err = e.tx.WithContext(ctx).Rollback().Error
	if err != nil {
		return
	}
	e.txFlag = false
	return
}

// Commit ...
func (e *gormEngine) Commit(ctx context.Context, args ...*CommitMessage) (err error) {
	e.txMutex.Lock()
	defer e.txMutex.Unlock()

	if len(args) > 0 {
		for _, arg := range args {
			if pubErr := e.publish(ctx, arg.Topic, arg.Value, false, arg.CallbackTopic); pubErr != nil {
				e.logger.Errorf(ctx, "commit publish error: %s", pubErr)
				err = e.Rollback(ctx)
				return
			}
		}
	}
	err = e.tx.WithContext(ctx).Commit().Error
	if err != nil {
		return
	}
	e.txFlag = false
	return
}

func (e *gormEngine) Transaction(ctx context.Context, h TransactionHandler, args ...*CommitMessage) (err error) {
	var tx interface{}
	tx, err = e.Begin(ctx)
	if err != nil {
		return
	}
	if err = h(ctx, tx); err != nil {
		e.Rollback(ctx)
	}
	err = e.Commit(ctx, args...)
	return
}
