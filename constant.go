package outbox

const (
	// MessageHeaderMsgIDKey 消息ID
	MessageHeaderMsgIDKey = "nilorg.outbox.msg.id"
	// MessageHeaderMsgTopicKey 消息主题
	MessageHeaderMsgTopicKey = "nilorg.outbox.msg.topic"
	// MessageHeaderMsgTypeKey 消息内容类型
	MessageHeaderMsgTypeKey = "nilorg.outbox.msg.type"
	// MessageHeaderMsgSendTimeKey 消息发送时间
	MessageHeaderMsgSendTimeKey = "nilorg.outbox.msg.sendtime"
	// MessageHeaderMsgCallbackKey 消息回调
	MessageHeaderMsgCallbackKey = "nilorg.outbox.msg.callback"
	// MessageHeaderMsgUserKey 消息用户
	MessageHeaderMsgUserKey = "nilorg.outbox.msg.user"
)

const (
	// StatusNameFailed 失败
	StatusNameFailed = "failed"
	// StatusNameScheduled 列入计划
	StatusNameScheduled = "scheduled"
	// StatusNameSucceeded 成功
	StatusNameSucceeded = "succeeded"
)
