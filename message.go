package outbox

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/nilorg/eventbus"
)

const (
	// MessageVersion 版本
	MessageVersion = "v1"
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

// IsTimeout 是否超时
func (m *Message) IsTimeout(timeout time.Duration) bool {
	v, ok := m.Header[MessageHeaderMsgSendTimeKey]
	if !ok {
		return true
	}
	utc, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return true
	}
	t := time.Unix(utc, 0).Add(timeout)
	u := time.Now()
	return t.Before(u)
}

// IsID 是否存在ID
func (m *Message) IsID() bool {
	_, ok := m.Header[MessageHeaderMsgIDKey]
	return ok
}

// ID msg id
func (m *Message) ID() string {
	id, _ := m.Header[MessageHeaderMsgIDKey]
	return id
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

func decodeValue(data []byte) (msg *eventbus.Message, err error) {
	var tempMsg eventbus.Message
	if err = json.Unmarshal(data, &tempMsg); err != nil {
		return
	}
	msg = &tempMsg
	return
}
