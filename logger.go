package outbox

import (
	"context"
	"log"
)

// Logger logger
type Logger interface {
	// Debugf 测试
	Debugf(ctx context.Context, format string, args ...interface{})
	// Debugln 测试
	Debugln(ctx context.Context, args ...interface{})
	// Infof 信息
	Infof(ctx context.Context, format string, args ...interface{})
	// Infoln 消息
	Infoln(ctx context.Context, args ...interface{})
	// Warnf 警告
	Warnf(ctx context.Context, format string, args ...interface{})
	// Warnln 警告
	Warnln(ctx context.Context, args ...interface{})
	// Warningf 警告
	Warningf(ctx context.Context, format string, args ...interface{})
	// Warningln 警告
	Warningln(ctx context.Context, args ...interface{})
	// Errorf 错误
	Errorf(ctx context.Context, format string, args ...interface{})
	// Errorln 错误
	Errorln(ctx context.Context, args ...interface{})
}

// StdLogger ...
type StdLogger struct {
}

// Debugf 测试
func (StdLogger) Debugf(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[Debug] "+format, args...)
}

// Debugln 测试
func (StdLogger) Debugln(ctx context.Context, args ...interface{}) {
	nArgs := []interface{}{
		"[Debug]",
	}
	nArgs = append(nArgs, args...)
	log.Println(nArgs...)
}

// Infof 信息
func (StdLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
}

// Infoln 消息
func (StdLogger) Infoln(ctx context.Context, args ...interface{}) {
	nArgs := []interface{}{
		"[INFO]",
	}
	nArgs = append(nArgs, args...)
	log.Println(nArgs...)
}

// Warnf 警告
func (StdLogger) Warnf(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[Warn] "+format, args...)
}

// Warnln 警告
func (StdLogger) Warnln(ctx context.Context, args ...interface{}) {
	nArgs := []interface{}{
		"[Warn]",
	}
	nArgs = append(nArgs, args...)
	log.Println(nArgs...)
}

// Warningf 警告
func (StdLogger) Warningf(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[Warning] "+format, args...)
}

// Warningln 警告
func (StdLogger) Warningln(ctx context.Context, args ...interface{}) {
	nArgs := []interface{}{
		"[Warning]",
	}
	nArgs = append(nArgs, args...)
	log.Println(nArgs...)
}

// Errorf 错误
func (StdLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[Error] "+format, args...)
}

// Errorln 错误
func (StdLogger) Errorln(ctx context.Context, args ...interface{}) {
	nArgs := []interface{}{
		"[Error]",
	}
	nArgs = append(nArgs, args...)
	log.Println(nArgs...)
}
