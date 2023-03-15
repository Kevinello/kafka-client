package kafka

import "os"

// GetEnv 获取环境变量，获取不到则使用默认值
//
//	@param key string
//	@param defaultVal string
//	@return val string
//	@author: Kevineluo 2022-04-19 03:34:22
func GetEnv(key, defaultVal string) (val string) {
	val = os.Getenv(key)
	if val == "" {
		val = defaultVal
	}
	return
}
