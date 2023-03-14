package kafka

import (
	"fmt"
)

const (
	// KvTopicPrefix 从Entrance发出的topic，kv__开头的
	//	@update 2023-02-22 08:35:09
	KvTopicPrefix = "kv__"

	// ToAnalyzerPrefix 从Entrance发出的topic，to_analyzer__开头的
	//	@update 2023-02-22 08:35:06
	ToAnalyzerPrefix = "to_analyzer__"

	// AfterTransTopicPrefix TranslatedTopicPrefix 经过翻译后的数据，topic是translated__开头的
	//	@update 2023-02-22 08:35:02
	AfterTransTopicPrefix = "after_trans__"

	// ToTranslateTopicPrefix 需要翻译的数据，topic是"to_translate__"开头的
	//	@update 2023-02-22 08:34:56
	ToTranslateTopicPrefix = "to_translate__"

	// DataLakeTopicPrefix 经过datalake 入hive "dl__"
	//	@update 2023-02-22 08:34:51
	DataLakeTopicPrefix = "dl__"

	// SingletonTopicPrefix 入clickhouse "singleton"
	//	@update 2023-02-22 08:34:48
	SingletonTopicPrefix = "singleton"

	// SingletonV2TopicPrefix 入clickhouse "singleton" v2
	//	@update 2023-02-22 08:32:13
	SingletonV2TopicPrefix = "singleton_v2"

	// TimelineTopicPrefix 入clickhouse "timeline"
	//	@update 2023-02-22 08:33:13
	TimelineTopicPrefix = "timeline"
)

// GetTopic GetTopicNameWithTag 生成后台数据流topic
//
//	@param tag string
//	@param pid int
//	@param plugin int
//	@param step int
//	@param isVip bool
//	@return topic string
//	@author kevineluo
//	@update 2023-02-22 08:47:37
func GetTopic(tag string, pid int, plugin int, step int, isVip bool) (topic string) {
	topic = fmt.Sprintf("%s%d.%d", tag, step, plugin)

	if isVip {
		topic += ".vip"
	}

	return topic
}
