package models

import "time"

type Config struct {
	Bind      string                 `yaml:"bind"`    // 绑定地址
	Backend   string                 `yaml:"backend"` // 存储位置
	Caches    map[string]ConfigCache `yaml:"caches"`  // 缓存配置
	Gc        ConfigGc               `yaml:"gc"`      // 缓存重建时间
	ErrorHtml string                 `yaml:"page"`    // 错误页面
}

type ConfigGc struct {
	Meta time.Duration `yaml:"meta"`
	Blob time.Duration `yaml:"blob"`
}

type ConfigCache struct {
	URLs  []string          `yaml:"urls"`  // 缓存后端地址
	Rules []ConfigCacheRule `yaml:"rules"` // 缓存策略 , 如果没有命中则跳过缓存
}

type ConfigCacheRule struct {
	Regex   string        `yaml:"regex"`   // 命中正则表达式
	Ttl     time.Duration `yaml:"ttl"`     // 缓存超时，以下载时间为准，如果为 0 则永不过期
	Refresh time.Duration `yaml:"refresh"` // 缓存刷新时间，文件下载后将在此时间过期后在调用时刷新
}
