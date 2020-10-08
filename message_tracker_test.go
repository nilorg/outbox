package outbox

import (
	"testing"

	"github.com/bwmarrin/snowflake"
	"github.com/go-redis/redis/v8"
)

func TestRedisMessageTracker(t *testing.T) {
	rd := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})
	hashKey := "outbox:msg_tracker"
	messageTracker := NewRedisMessageTracker(rd, hashKey)
	sn, _ := snowflake.NewNode(0)
	for i := 0; i < 100; i++ {
		msgID := sn.Generate()
		err := messageTracker.MarkAsProcessed(msgID.String())
		if err != nil {
			t.Error(err)
			return
		}
		var exist bool
		exist, err = messageTracker.HasProcessed(msgID.String())
		if err != nil {
			t.Error(err)
			return
		}
		t.Log(exist)
	}
}
