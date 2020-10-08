package outbox

import (
	"encoding/base64"
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

// EncodeValue 对值进行编码
func EncodeValue(v interface{}) (s string, err error) {
	var bytes []byte
	if bytes, err = json.Marshal(v); err != nil {
		return
	}
	s = base64.StdEncoding.EncodeToString(bytes)
	return
}

// DecodeValue 对值进行解码
func DecodeValue(s string, v interface{}) (err error) {
	var bytes []byte
	if bytes, err = base64.StdEncoding.DecodeString(s); err != nil {
		return
	}
	err = json.Unmarshal(bytes, v)
	return
}
