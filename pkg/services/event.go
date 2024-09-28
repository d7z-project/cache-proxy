package services

import "regexp"

type Event struct {
	urls  []string                      // 下载配置
	rules map[*regexp.Regexp]*EventRule // 相关的命中策略
}

type EventRule struct {
}
