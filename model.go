package outbox

import "time"

const (
	// PublishedTableName ...
	PublishedTableName = "outbox_published"
	// ReceivedTableName ...
	ReceivedTableName = "outbox_received"
)

// Published ...
type Published struct {
	ID         int64      `json:"id" gorm:"column:id;primaryKey;type:BIGINT(20)"`
	Version    string     `json:"version" gorm:"column:version;type:VARCHAR(20)"`
	Topic      string     `json:"topic" gorm:"column:topic;type:VARCHAR(200);not null"`
	Value      string     `json:"value" gorm:"column:value;type:LONGTEXT"`
	Retries    int        `json:"retries" gorm:"column:retries;type:INT(11)"`
	CreatedAt  time.Time  `json:"created_at" gorm:"column:created_at;type:DATETIME;not null"`
	ExpiresAt  *time.Time `json:"expires_at" gorm:"column:expires_at;type:DATETIME"`
	StatusName string     `json:"status_name" gorm:"index;column:status_name;type:VARCHAR(40);not null"`
}

// TableName ...
func (Published) TableName() string {
	return PublishedTableName
}

// Received ...
type Received struct {
	ID         int64      `json:"id" gorm:"column:id;primaryKey;type:BIGINT(20)"`
	Version    string     `json:"version" gorm:"column:version;type:VARCHAR(20)"`
	Topic      string     `json:"name" gorm:"column:name;type:VARCHAR(200);not null"`
	Group      string     `json:"group" gorm:"column:group;type:VARCHAR(200);not null"`
	Value      string     `json:"value" gorm:"column:value;type:LONGTEXT"`
	Retries    int        `json:"retries" gorm:"column:retries;type:INT(11)"`
	CreatedAt  time.Time  `json:"created_at" gorm:"column:created_at;type:DATETIME;not null"`
	ExpiresAt  *time.Time `json:"expires_at" gorm:"column:expires_at;type:DATETIME"`
	StatusName string     `json:"status_name" gorm:"index;column:status_name;type:VARCHAR(40);not null"`
}

// TableName ...
func (Received) TableName() string {
	return ReceivedTableName
}
