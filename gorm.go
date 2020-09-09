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
	options             *EngineOptions
	db                  *gorm.DB
	tx                  *gorm.DB
	txFlag              bool
	txMutex             sync.Mutex
	node                *snowflake.Node
	eventBus            eventbus.EventBus
	subscribeItems      map[string]*subscribeItem
	subscribeItemsMutex sync.Mutex
	logger              Logger
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
	e.subscribeItemsMutex.Lock()
	defer e.subscribeItemsMutex.Unlock()

	group, _ := eventbus.FromGroupIDContext(ctx)

	hex := fmt.Sprintf("%s-%s", topic, group)
	e.subscribeItems[hex] = &subscribeItem{
		Topic: topic,
		Group: group,
		h:     h,
	}

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
			Version:    MessageVersion,
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
			// 使用日志组件，打印日志
			if group != "" {
				e.logger.Errorf(ctx, "exec subscribe %s handler error: %s", topic, hErr)
			} else {
				e.logger.Errorf(ctx, "exec subscribe %s(%s) handler error: %s", topic, group, hErr)
			}
		} else {
			// 成功消息记录，到期时间
			exp := time.Now().AddDate(0, 0, 15)
			r.ExpiresAt = &exp
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
		Version:    MessageVersion,
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
		e.logger.Errorf(ctx, "exec async:%v publish %s error: %s", async, topic, pubErr)
	} else {
		// 成功消息记录，到期时间
		exp := time.Now().Add(24 * time.Hour)
		p.ExpiresAt = &exp
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
	if p.Version == "" {
		p.Version = MessageVersion
	}
	return db.Create(p).Error
}

func (e *gormEngine) insertReceivedFromGorm(db *gorm.DB, r *Received) error {
	if r.Version == "" {
		r.Version = MessageVersion
	}
	return db.Create(r).Error
}

func (e *gormEngine) changePublishState(db *gorm.DB, msgID int64, state string) error {
	return db.Model(&Published{}).Where(&Published{ID: msgID}).Updates(&Published{StatusName: state}).Error
}

func (e *gormEngine) changeReceiveState(db *gorm.DB, msgID int64, state string) error {
	return db.Model(&Received{}).Where(&Received{ID: msgID}).Updates(&Received{StatusName: state}).Error
}

func (e *gormEngine) findPublishByState(db *gorm.DB, state string) (list []*Published, err error) {
	err = db.Model(&Published{}).Where(&Published{Version: MessageVersion, StatusName: state}).Find(&list).Error
	return
}

func (e *gormEngine) findReceiveByState(db *gorm.DB, state string) (list []*Received, err error) {
	err = db.Model(&Received{}).Where(&Received{Version: MessageVersion, StatusName: state}).Find(&list).Error
	return
}

func (e *gormEngine) deletePublished(db *gorm.DB, state string, expired time.Time) (err error) {
	err = db.Delete(&Published{}, "version = ? and status_name = ? and expires_at <= ?", MessageVersion, state, expired).Error
	return
}

func (e *gormEngine) deleteReceived(db *gorm.DB, state string, expired time.Time) (err error) {
	err = db.Delete(&Received{}, "version = ? and status_name = ? and expires_at <= ?", MessageVersion, state, expired).Error
	return
}

func (e *gormEngine) retriePublished(db *gorm.DB, msgID int64) (err error) {
	err = db.Model(&Published{}).Where("id = ?", msgID).Update("retries", gorm.Expr("retries + ?", 1)).Error
	return
}

func (e *gormEngine) retrieReceived(db *gorm.DB, msgID int64) (err error) {
	err = db.Model(&Received{}).Where("id = ?", msgID).Update("retries", gorm.Expr("retries + ?", 1)).Error
	return
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

// dataClean 数据清理
func (e *gormEngine) dataClean() (err error) {
	expd := time.Now().Add(e.options.SucceedMessageExpiredAfter)
	if err = e.deletePublished(e.db, StatusNameSucceeded, expd); err != nil {
		return
	}
	if err = e.deleteReceived(e.db, StatusNameSucceeded, expd); err != nil {
		return
	}
	return
}

// failedRetryInterval 失败重试间隔
func (e *gormEngine) failedRetryInterval() (err error) {
	if err = e.failedPublishedRetryInterval(); err != nil {
		return
	}
	if err = e.failedReceivedRetryInterval(); err != nil {
		return
	}
	return
}

// failedPublishedRetryInterval 失败重试间隔
func (e *gormEngine) failedPublishedRetryInterval() (err error) {
	var (
		publisheds []*Published
	)
	if publisheds, err = e.findPublishByState(e.db, StatusNameScheduled); err != nil {
		return
	}
	ctx := context.Background()
	for _, published := range publisheds {
		// 重试次数超过50，执行通知回调
		if published.Retries >= 50 {
			if err = e.changePublishState(e.db, published.ID, StatusNameFailed); err != nil {
				return
			}
			if e.options.FailedThresholdCallback != nil {
				go e.options.FailedThresholdCallback(ctx, CallbackTypePublished, published)
			}
		}
		// 版本不相同，跳过执行
		if published.Version != MessageVersion {
			continue
		}
		if msg, msgErr := decodeValue([]byte(published.Value)); msgErr != nil {
			e.logger.Errorf(ctx, "failedRetryInterval-decodeValue error: %v", msgErr)
			if err = e.retriePublished(e.db, published.ID); err != nil {
				return
			}
		} else {
			if pubErr := e.eventBus.Publish(ctx, published.Topic, msg); pubErr != nil {
				if err = e.retriePublished(e.db, published.ID); err != nil {
					return
				}
			} else {
				if err = e.changePublishState(e.db, published.ID, StatusNameSucceeded); err != nil {
					return
				}
			}
		}
	}
	return
}

// failedReceivedRetryInterval 失败重试间隔
func (e *gormEngine) failedReceivedRetryInterval() (err error) {
	var (
		receiveds []*Received
	)
	ctx := context.Background()
	if receiveds, err = e.findReceiveByState(e.db, StatusNameScheduled); err != nil {
		return
	}
	e.subscribeItemsMutex.Lock()
	defer e.subscribeItemsMutex.Unlock()

	for _, received := range receiveds {
		// 重试次数超过50，执行通知回调
		if received.Retries >= 50 {
			if err = e.changeReceiveState(e.db, received.ID, StatusNameFailed); err != nil {
				return
			}
			if e.options.FailedThresholdCallback != nil {
				go e.options.FailedThresholdCallback(ctx, CallbackTypeReceived, received)
			}
		}
		// 版本不相同，跳过执行
		if received.Version != MessageVersion {
			continue
		}
		if msg, msgErr := decodeValue([]byte(received.Value)); msgErr != nil {
			e.logger.Errorf(ctx, "failedRetryInterval-decodeValue error: %v", msgErr)
			if err = e.retrieReceived(e.db, received.ID); err != nil {
				return
			}
		} else {
			hex := fmt.Sprintf("%s-%s", received.Topic, received.Group)
			if subscribeItem, subscribeItemOk := e.subscribeItems[hex]; !subscribeItemOk || subscribeItem.h == nil {
				if err = e.retrieReceived(e.db, received.ID); err != nil {
					return
				}
			} else {
				subMsg := Message(*msg)
				if subErr := subscribeItem.h(ctx, &subMsg); subErr != nil {
					if err = e.retrieReceived(e.db, received.ID); err != nil {
						return
					}
				} else {
					if err = e.changePublishState(e.db, received.ID, StatusNameSucceeded); err != nil {
						return
					}
				}
			}
		}
	}
	return
}

func (e *gormEngine) sentry() {
	go func() {
		dataCleanTicker := time.NewTicker(e.options.DataCleanInterval)
		defer dataCleanTicker.Stop()
		failedRetryIntervalTicker := time.NewTicker(e.options.FailedRetryInterval)
		defer dataCleanTicker.Stop()
		for {
			select {
			case <-dataCleanTicker.C:
				recoverHandle(e.logger, e.dataClean)
			case <-failedRetryIntervalTicker.C:
				recoverHandle(e.logger, e.failedRetryInterval)
			}
		}
	}()
}
