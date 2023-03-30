package kafka

import (
	"fmt"

	"github.com/go-logr/logr"
)

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
	logger.Info(fmt.Sprintf(template, args...))
}
