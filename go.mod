module github.com/nilorg/outbox

go 1.14

require (
	github.com/bwmarrin/snowflake v0.3.0
	github.com/go-redis/redis/v8 v8.4.4
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/nilorg/eventbus v0.0.0-20211122105808-14e691ba0a22
	github.com/streadway/amqp v1.0.0
	gopkg.in/check.v1 v1.0.0-20200902074654-038fdea0a05b // indirect
	gorm.io/driver/mysql v1.0.3
	gorm.io/gorm v1.20.6
)

// replace github.com/nilorg/eventbus v0.0.0-20200909093902-120d016f5cfa => ../eventbus
