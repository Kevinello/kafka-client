package kafka

import "github.com/go-logr/logr"

type logger struct {
	logr.Logger
}

func (logger *logger) Printf(template string, args ...any) {
	keyAndValues := make([]any, 0)
	for idx, value := range args {
		keyAndValues = append(keyAndValues, idx+1, value)
	}
	logger.Info(template, keyAndValues...)
}
