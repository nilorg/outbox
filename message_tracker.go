package outbox

import (
	"context"

	"github.com/go-redis/redis/v8"
)

// MessageTracker 消息追踪
type MessageTracker interface {
	HasProcessed(msgID string) (exist bool, err error)
	MarkAsProcessed(msgID string) (err error)
}

// NewRedisMessageTracker 创建基于redis做的消息追踪，用于幂等操作
func NewRedisMessageTracker(rd *redis.Client, hashKey string) MessageTracker {
	return &redisMessageTracker{
		redis: rd,
	}
}

type redisMessageTracker struct {
	redis   *redis.Client
	hashKey string
}

func (t *redisMessageTracker) HasProcessed(msgID string) (exist bool, err error) {
	exist, err = t.redis.HExists(context.Background(), t.hashKey, msgID).Result()
	return
}

func (t *redisMessageTracker) MarkAsProcessed(msgID string) (err error) {
	err = t.redis.HSet(context.Background(), t.hashKey, msgID, "1").Err()
	return
}
