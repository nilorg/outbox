module github.com/nilorg/outbox

go 1.14

require (
	github.com/bwmarrin/snowflake v0.3.0
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/nilorg/eventbus v0.0.0-20200916034151-55536d7a13b3
	github.com/streadway/amqp v1.0.0
	github.com/stretchr/testify v1.6.1 // indirect
	golang.org/x/sys v0.0.0-20191010194322-b09406accb47 // indirect
	gopkg.in/check.v1 v1.0.0-20200902074654-038fdea0a05b // indirect
	gorm.io/driver/mysql v1.0.1
	gorm.io/gorm v1.20.0
)

// replace github.com/nilorg/eventbus v0.0.0-20200909093902-120d016f5cfa => ../eventbus
