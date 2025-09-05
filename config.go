package kafka

import (
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/hashicorp/go-multierror"
	"github.com/segmentio/kafka-go/sasl"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type (
	// ConsumerConfig configuration object used to create new instances of Consumer
	//
	//	@author kevineluo
	//	@update 2023-03-15 03:01:48
	ConsumerConfig struct {
		// the Kafka broker address, the default value is "localhost:9092".
		// If you have multiple brokers, you can use a comma to separate them, such as "localhost:9092,localhost:9093,localhost:9094".
		// default: "localhost:9092"
		Bootstrap string `json:"bootstrap"`

		// Group ID of consumer
		GroupID string `json:"group_id"`

		// Whether to disable consuming messages in a loop, default: false
		DisableLoop bool `json:"disable_loop"`

		// If no message received after [MaxMsgInterval] seconds then restart Consumer, default: 300 seconds
		MaxMsgInterval time.Duration `json:"max_msg_interval"`

		// Interval for consumer to sync topics, default: 15 seconds
		SyncTopicInterval time.Duration `json:"sync_topic_interval"`

		// Maximum number of goroutine for subscribing to topics, default: runtime.NumCPU()
		MaxConsumeGoroutines int `json:"max_consume_goroutines"`

		// max error count from consuming messages, set it to -1 to ignore error, default: 5
		MaxConsumeErrorCount int `json:"max_consume_error_count"`

		// function which handles received messages from the Kafka broker
		MessageHandler MessageHandler `json:"-"`

		// Function used to sync topics, default: GetAllTopic
		GetTopics GetTopicsFunc `json:"-"`

		// logger implement logr.LogSinker, default: zapr.Logger
		Logger *logr.Logger `json:"-"`

		// Mechanism sasl authentication, default: nil
		Mechanism sasl.Mechanism `json:"-"`

		// TLS TLS configuration, default: nil
		TLS *tls.Config `json:"-"`

		// used when Config.logger is nil, follow the zap style level(https://pkg.go.dev/go.uber.org/zap@v1.24.0/zapcore#Level),
		// setting the log level for zapr.Logger(config.logLevel should be in range[-1, 5])
		// default: 0 -- InfoLevel
		LogLevel int `json:"log_level"`

		// enable verbose kafka-go log, default: false
		Verbose bool `json:"verbose"`
	}

	// ProducerConfig configuration object used to create new instances of Producer
	//
	//	@author kevineluo
	//	@update 2023-03-15 03:01:48
	ProducerConfig struct {
		// kafka bootstrap, default: "localhost:9092"
		Bootstrap string `json:"bootstrap"`

		// determine synchronously / asynchronously write messages, default: false
		Async bool `json:"async"`

		// By default kafka has the auto.create.topics.enable='true', you can ignore this config.
		// when this config, producer will attempt to create topic prior to publishing the message,
		// else it will return an error when meeting missing topic,
		// default: false
		AllowAutoTopicCreation bool `json:"allow_auto_topic_creation"`

		// logger implement logr.LogSinker, default: zapr.Logger
		Logger *logr.Logger `json:"-"`

		// Mechanism sasl authentication, default: nil
		Mechanism sasl.Mechanism `json:"-"`

		// TLS TLS configuration, default: nil
		TLS *tls.Config `json:"-"`

		// used when Config.logger is nil, follow the zap style level(https://pkg.go.dev/go.uber.org/zap@v1.24.0/zapcore#Level),
		// setting the log level for zapr.Logger(config.logLevel should be in range[-1, 5])
		// default: 0 -- InfoLevel
		LogLevel int `json:"log_level"`

		// enable verbose kafka-go log, default: false
		Verbose bool `json:"verbose"`
	}
)

// Validate check config and set default value
//
//	@receiver config *ConsumerConfig
//	@return err error
//	@author kevineluo
//	@update 2023-03-31 04:44:47
func (config *ConsumerConfig) Validate() (err error) {
	// use default bootstrap if not set
	if config.Bootstrap == "" {
		config.Bootstrap = "localhost:9092"
	}
	// check consumer group id
	if config.GroupID == "" {
		err = multierror.Append(err, errors.New("missing GroupID"))
	}
	// use default max message interval if not set
	if config.MaxMsgInterval == 0 {
		config.MaxMsgInterval = 5 * 60 * time.Second
	} else if config.MaxMsgInterval < 30*time.Second {
		err = multierror.Append(err, fmt.Errorf("MaxMsgInterval should be greater than 30 seconds, got %v", config.MaxMsgInterval))
	}
	// use default sync topic interval if not set
	if config.SyncTopicInterval == 0 {
		config.SyncTopicInterval = 15 * time.Second
	}
	// use default max consume goroutines if not set
	if config.MaxConsumeGoroutines == 0 {
		config.MaxConsumeGoroutines = runtime.NumCPU()
	}
	// use default max consume error count if not set
	if config.MaxConsumeErrorCount == 0 {
		config.MaxConsumeErrorCount = 5
	}
	// check message handler
	if config.MessageHandler == nil {
		err = multierror.Append(err, errors.New("missing MessageHandler"))
	}
	// use default get topics function if not set
	if config.GetTopics == nil {
		config.GetTopics = GetAllTopic()
	}
	// use default logger if not set
	if config.Logger == nil {
		var tmpErr error
		config.Logger, tmpErr = initDefaultLogger(config.LogLevel)
		err = multierror.Append(err, tmpErr).ErrorOrNil()
	}

	return
}

// Validate check config and set default value
//
//	@receiver config *ProducerConfig
//	@return err error
//	@author kevineluo
//	@update 2023-03-15 03:19:23
func (config *ProducerConfig) Validate() (err error) {
	if config.Bootstrap == "" {
		config.Bootstrap = "localhost:9092"
	}
	if config.Logger == nil {
		config.Logger, err = initDefaultLogger(config.LogLevel)
		if err != nil {
			return
		}
	}

	return
}

// initDefaultLogger init default zapr.Logger
//
//	@param level int
//	@return logger logr.Logger
//	@return err error
//	@author kevineluo
//	@update 2023-04-06 10:58:46
func initDefaultLogger(level int) (logger *logr.Logger, err error) {
	var cfg zap.Config
	logger = new(logr.Logger)
	zapLevel := zapcore.Level(level)
	if zapLevel >= zap.DebugLevel && zapLevel <= zap.FatalLevel {
		if zapLevel == zap.DebugLevel {
			cfg = zap.NewDevelopmentConfig()
		} else {
			cfg = zap.NewProductionConfig()
		}
	} else {
		err = fmt.Errorf("[InitDefaultLogger] found invalid level: %d, level should be in range[-1, 5]", level)
	}
	// set time format
	cfg.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	// add color for console
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	zapLogger := zap.New(zapcore.NewCore(zapcore.NewConsoleEncoder(cfg.EncoderConfig), zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout)), zapLevel))
	*logger = zapr.NewLogger(zapLogger)
	return
}
