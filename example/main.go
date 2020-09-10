package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nilorg/eventbus"
	"github.com/nilorg/outbox"
	"github.com/streadway/amqp"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

var (
	db     *gorm.DB
	bus    eventbus.EventBus
	engine outbox.Engine
)

func init() {
	var (
		err  error
		conn *amqp.Connection
	)
	conn, err = amqp.Dial("amqp://root:test123@localhost:5672/")
	if err != nil {
		panic(err)
	}
	bus, err = eventbus.NewRabbitMQ(conn)
	if err != nil {
		panic(err)
	}
	db, err = gorm.Open(
		mysql.Open("root:test123@tcp(127.0.0.1:3306)/outbox?charset=utf8&parseTime=True&loc=Local"),
		&gorm.Config{
			NamingStrategy: schema.NamingStrategy{
				SingularTable: true,
			},
		},
	)
	if err != nil {
		panic(err)
	}
	engine, err = outbox.New(outbox.EngineTypeGorm, db, bus)
	if err != nil {
		panic(err)
	}
	engine.SubscribeAsync(context.Background(), "user.commit", func(ctx context.Context, msg *outbox.Message) error {
		fmt.Printf("user.commit: %+v\n", msg)
		return nil
	})
}

func main() {
	db.AutoMigrate(User{})
	testUserTran()
	testUserTran2()
	time.Sleep(5 * time.Second)
}

func testUserTran() {
	ctx := context.Background()
	var (
		tx  outbox.Transactioner
		err error
	)
	tx, err = engine.Begin(ctx)
	if err != nil {
		log.Printf("tx error: %v", err)
		return
	}
	txDb := tx.Session().(*gorm.DB)
	err = txDb.Create(&User{
		Name: "test_name",
		Age:  11,
	}).Error
	if err != nil {
		tx.Rollback(ctx)
		return
	}
	tx.Commit(ctx, &outbox.CommitMessage{
		Topic: "user.commit",
		Value: "提交内容时间:" + time.Now().String(),
	})
}

func testUserTran2() {
	ctx := context.Background()

	err := engine.Transaction(ctx, func(ctx context.Context, db interface{}) error {
		txDb := db.(*gorm.DB)
		err := txDb.Create(&User{
			Name: "testUserTran2",
			Age:  11,
		}).Error
		return err
	}, &outbox.CommitMessage{
		Topic: "user.commit",
		Value: "提交内容时间2222:" + time.Now().String(),
	})

	if err != nil {
		fmt.Printf("tran error: %s\n", err)
	}
}

// User ...
type User struct {
	gorm.Model
	Name string
	Age  int
}
