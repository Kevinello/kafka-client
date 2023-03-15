package kafka

import "github.com/go-logr/logr"

type logger struct {
	logr.Logger
}

// Printf implement kafkago.Logger
//
//	@receiver logger *logger
//	@param template string
//	@param args ...any
//	@author kevineluo
//	@update 2023-03-15 10:51:04
func (logger *logger) Printf(template string, args ...any) {
	keyAndValues := make([]any, 0)
	for idx, value := range args {
		keyAndValues = append(keyAndValues, idx+1, value)
	}
	logger.Info(template, keyAndValues...)
}
