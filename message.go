package outbox

import (
	"encoding/json"

	"github.com/nilorg/eventbus"
)

const (
	// Version 版本
	Version = "v1"
)

// Message 消息
type Message eventbus.Message

// IsCallback 存在回调
func (m *Message) IsCallback() bool {
	_, ok := m.Header[MessageHeaderMsgCallbackKey]
	return ok
}

// Callback 回调地址
func (m *Message) Callback() string {
	callback, _ := m.Header[MessageHeaderMsgCallbackKey]
	return callback
}

// IsUser 是否存在用户
func (m *Message) IsUser() bool {
	_, ok := m.Header[MessageHeaderMsgUserKey]
	return ok
}

// User 用户
func (m *Message) User() string {
	user, _ := m.Header[MessageHeaderMsgUserKey]
	return user
}

func encodeValue(v *eventbus.Message) (s string, err error) {
	var data []byte
	data, err = json.Marshal(v)
	if err != nil {
		return
	}
	s = string(data)
	return
}
