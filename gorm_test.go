package outbox

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nilorg/eventbus"
	"github.com/streadway/amqp"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func newTestEventBus(t *testing.T) (bus eventbus.EventBus) {
	var err error
	var conn *amqp.Connection
	conn, err = amqp.Dial("amqp://root:test123@localhost:5672/")
	if err != nil {
		t.Fatal(err)
		return
	}
	bus, err = eventbus.NewRabbitMQ(conn)
	if err != nil {
		t.Fatal(err)
		return
	}
	return
}

func newTestGormEngine(t *testing.T) (engine Engine) {
	db, err := gorm.Open(
		mysql.Open("root:test123@tcp(127.0.0.1:3306)/outbox?charset=utf8&parseTime=True&loc=Local"),
		&gorm.Config{
			NamingStrategy: schema.NamingStrategy{
				SingularTable: true,
			},
		},
	)
	if err != nil {
		t.Fatalf("gorm open error: %v", err)
	}
	bus := newTestEventBus(t)
	opts := &EngineOptions{
		FailedRetryInterval:        time.Minute,
		FailedRetryCount:           50,
		DataCleanInterval:          time.Hour,
		SucceedMessageExpiredAfter: 24 * time.Hour,
		SnowflakeNode:              1,
		Logger:                     &eventbus.StdLogger{},
	}
	engine, err = New(EngineTypeGorm, db, bus, opts)
	if err != nil {
		t.Fatalf("new gorm engine error: %s", err)
	}
	return
}

func TestGorm(t *testing.T) {
	engine := newTestGormEngine(t)
	ctx := context.Background()
	var err error
	topic := "order.create.success.sync"
	ctxGroup1 := eventbus.NewGroupIDContext(ctx, "nilorg.events.sync.group1")
	go func() {
		err = engine.Subscribe(ctxGroup1, topic, func(ctx context.Context, msg *Message) error {
			fmt.Printf("group1 %s: %+v is timeout: %v\n", topic, msg, msg.IsTimeout(3*time.Minute))
			return nil
		})
		if err != nil {
			t.Error(err)
			return
		}
	}()
	go func() {
		err = engine.Subscribe(ctxGroup1, topic, func(ctx context.Context, msg *Message) error {
			fmt.Printf("group1(copy) %s: %+v is timeout: %v\n", topic, msg, msg.IsTimeout(3*time.Minute))
			return nil
		})
		if err != nil {
			t.Error(err)
			return
		}
	}()
	ctxGroup2 := eventbus.NewGroupIDContext(ctx, "nilorg.events.sync.group2")
	go func() {
		err = engine.Subscribe(ctxGroup2, topic, func(ctx context.Context, msg *Message) error {
			fmt.Printf("group2 %s: %+v is timeout: %v\n", topic, msg, msg.IsTimeout(3*time.Minute))
			return nil
		})
		if err != nil {
			t.Error(err)
			return
		}
	}()
	fmt.Println("暂停30秒，停止RabbitMQ")
	time.Sleep(30 * time.Second)
	for i := 0; i < 100; i++ {
		err = engine.Publish(ctx, topic, "sync message")
		if err != nil {
			t.Error(err)
			return
		}
		fmt.Println("Publish sync success")
	}
	fmt.Println("暂停3分钟，测试重试机制")
	time.Sleep(3 * time.Minute)
}
