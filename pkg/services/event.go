package services

import "regexp"

type Event struct {
	urls  []string                      // 下载配置
	meta  *FileMeta                     // 相关文件的缓存信息
	rules map[*regexp.Regexp]*EventRule // 相关的命中策略
}

type EventRule struct {
}
