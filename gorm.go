package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
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
		if value, err = EncodeValue(baseMsg.Value); err != nil {
			return
		}
		baseMsg.Value = value
		var bytes []byte
		if bytes, err = json.Marshal(baseMsg); err != nil {
			return
		}
		r := &Received{
			ID:         id,
			Version:    MessageVersion,
			Topic:      topic,
			Group:      group,
			Value:      string(bytes),
			Retries:    0,
			CreatedAt:  time.Now(),
			StatusName: StatusNameScheduled,
		}
		msg := Message(*baseMsg)
		hErr := h(ctx, &msg)
		if hErr != nil {
			// 使用日志组件，打印日志
			if group != "" {
				e.logger.Errorf(ctx, "execute subscribe %s handler error: %s", topic, hErr)
			} else {
				e.logger.Errorf(ctx, "execute subscribe %s(%s) handler error: %s", topic, group, hErr)
			}
		} else {
			// 成功消息记录，到期时间
			exp := time.Now().Add(e.options.SucceedMessageExpiredAfter)
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
	var value string
	if value, err = EncodeValue(v); err != nil {
		return
	}
	msg := &eventbus.Message{
		Header: eventbus.MessageHeader{
			MessageHeaderMsgIDKey:       fmt.Sprint(id),
			MessageHeaderMsgTopicKey:    topic,
			MessageHeaderMsgTypeKey:     reflect.TypeOf(v).Name(),
			MessageHeaderMsgSendTimeKey: fmt.Sprint(timeNow.Unix()),
			MessageHeaderMsgCallbackKey: callbackName,
		},
		Value: value,
	}
	var bytes []byte
	if bytes, err = json.Marshal(msg); err != nil {
		return
	}
	p := &Published{
		ID:         id,
		Version:    MessageVersion,
		Topic:      topic,
		Value:      string(bytes),
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
		e.logger.Errorf(ctx, "execute async:%v publish %s error: %s", async, topic, pubErr)
	} else {
		// 成功消息记录，到期时间
		exp := time.Now().Add(e.options.SucceedMessageExpiredAfter)
		p.ExpiresAt = &exp
		p.StatusName = StatusNameSucceeded
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

func (e *gormEngine) changePublishStateSucceeded(db *gorm.DB, msgID int64) error {
	exp := time.Now().Add(e.options.SucceedMessageExpiredAfter)
	return db.Model(&Published{}).Where(&Published{ID: msgID}).Updates(&Published{StatusName: StatusNameSucceeded, ExpiresAt: &exp}).Error
}

func (e *gormEngine) changeReceiveState(db *gorm.DB, msgID int64, state string) error {
	return db.Model(&Received{}).Where(&Received{ID: msgID}).Updates(&Received{StatusName: state}).Error
}

func (e *gormEngine) changeReceiveStateSucceeded(db *gorm.DB, msgID int64) error {
	exp := time.Now().Add(e.options.SucceedMessageExpiredAfter)
	return db.Model(&Received{}).Where(&Received{ID: msgID}).Updates(&Received{StatusName: StatusNameSucceeded, ExpiresAt: &exp}).Error
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
func (e *gormEngine) Begin(ctx context.Context, opts ...*sql.TxOptions) (tx Transactioner, err error) {
	gormTx := e.db.WithContext(ctx).Begin(opts...)
	if err = gormTx.Error; err != nil {
		return
	}
	tx = &gormTransaction{
		session: gormTx,
		logger:  e.logger,
		publish: e.publish,
	}
	return
}

func (e *gormEngine) Transaction(ctx context.Context, h TransactionHandler, args ...*CommitMessage) (err error) {
	var tx Transactioner
	tx, err = e.Begin(ctx)
	if err != nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback(ctx)
		}
	}()

	if err = h(ctx, tx.Session()); err != nil {
		tx.Rollback(ctx)
		return
	}
	err = tx.Commit(ctx, args...)
	return
}

// dataClean 数据清理
func (e *gormEngine) dataClean() (err error) {
	ctx := context.Background()
	e.logger.Debugln(ctx, "execute dataClean")
	expd := time.Now()
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
	ctx := context.Background()
	e.logger.Debugln(ctx, "execute failedRetryInterval")
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
		// 添加重试次数
		if err = e.retriePublished(e.db, published.ID); err != nil {
			return
		}
		var msg Message
		if msgErr := json.Unmarshal([]byte(published.Value), &msg); msgErr != nil {
			e.logger.Errorf(ctx, "failedRetryInterval-decodeValue error: %v", msgErr)
		} else {
			if pubErr := e.eventBus.Publish(ctx, published.Topic, msg); pubErr != nil {
				e.logger.Errorf(ctx, "failedRetryInterval-Publish error: %v", pubErr)
			} else {
				if err = e.changePublishStateSucceeded(e.db, published.ID); err != nil {
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
		// 添加重试次数
		if err = e.retrieReceived(e.db, received.ID); err != nil {
			return
		}
		var msg Message
		if msgErr := json.Unmarshal([]byte(received.Value), &msg); msgErr != nil {
			e.logger.Errorf(ctx, "failedRetryInterval-decodeValue error: %v", msgErr)
		} else {
			hex := fmt.Sprintf("%s-%s", received.Topic, received.Group)
			if subscribeItem, subscribeItemOk := e.subscribeItems[hex]; !subscribeItemOk || subscribeItem.h == nil {
				e.logger.Warnf(ctx, "failedRetryInterval-subscribeItem %s error: %v", hex, msgErr)
			} else {
				if subErr := subscribeItem.h(ctx, &msg); subErr != nil {
					e.logger.Errorf(ctx, "failedRetryInterval-subscribeItem %s execute error: %v", hex, msgErr)
				} else {
					if err = e.changeReceiveStateSucceeded(e.db, received.ID); err != nil {
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

type gormTransaction struct {
	session *gorm.DB
	logger  Logger
	publish func(ctx context.Context, db *gorm.DB, topic string, v interface{}, async bool, callback ...string) (err error)
}

func (tran *gormTransaction) Session() interface{} {
	return tran.session
}

func (tran *gormTransaction) Rollback(ctx context.Context) (err error) {
	err = tran.session.WithContext(ctx).Rollback().Error
	return
}

func (tran *gormTransaction) Commit(ctx context.Context, args ...*CommitMessage) (err error) {
	if len(args) > 0 {
		for _, arg := range args {
			if pubErr := tran.publish(ctx, tran.session, arg.Topic, arg.Value, false, arg.CallbackTopic); pubErr != nil {
				tran.logger.Errorf(ctx, "commit publish error: %s", pubErr)
				err = tran.Rollback(ctx)
				return
			}
		}
	}
	err = tran.session.WithContext(ctx).Commit().Error
	return
}
