package outbox

import "context"

func recoverHandle(log Logger, f func() error) {
	defer func() {
		if reErr := recover(); reErr != nil {
			log.Errorf(context.Background(), "recoverHandle recover error: %v", reErr)
		}
	}()
	if err := f(); err != nil {
		log.Errorf(context.Background(), "recoverHandle error: %v", err)
	}
}
