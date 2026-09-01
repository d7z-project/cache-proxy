# AGENTS.md

本文件描述当前工程约束。修改行为或配置时必须同步更新本文件和 `README.md`。

## Proxy mode

1. mode Driver 位于 `pkg/proxy/<mode>/`，实现 `runtime.ModeDriver`。
2. mode 常量在 `pkg/config/config.go`，Driver 在 `pkg/app/drivers.go` 注册。
3. 普通 HTTP mode 复用 `httpcache.Handler`，协议包只负责路径分类、目标解析、响应转换和摘要校验。
4. 请求分类固定为 `passthrough`、`metadata`、`content`，缓存策略由分类和全局 TTL 决定。
5. 配置使用公共 instance 结构；mode 专属字段只能放在严格解码的 `options` 中。

## Go 代码

- 遵循标准库和现有包边界；短小单次逻辑保持内联，helper 必须有复用价值或明确职责。
- 内部跨函数状态使用完整领域名称；局部循环变量使用 Go 惯用短名称。
- error 文本小写开头，并用 `%w` 保留错误链。
- 使用结构化解析器处理 JSON、XML、YAML 和仓库 metadata，不使用脆弱的字符串替换协议解析。
- 重构保持请求路径、缓存键与协议输出稳定；行为变化必须补测试并更新文档。

## 配置

- YAML 字段使用 `snake_case`，严格拒绝未知字段。
- 全局缓存只暴露 `cache.metadata_ttl` 和 `cache.retention`。
- instance 必须包含 `name`、`enabled`、`mode`、`upstreams`，并且只能设置 `path` 或 `bind` 之一。
- 支持的 options：file rules/pass headers；Git/OCI auth；Git sync/operation timeout；Go SumDB/GOPRIVATE；Cargo/PyPI 目标 host allowlist；Cargo auth-required。

## 缓存语义

- metadata 在 `metadata_ttl` 内直接命中；过期后只做一次 conditional GET；`304` 推进 fetched-at；瞬时失败时服务 stale。
- content 使用稳定 key 和 `retention`；并发 miss 合并为一次与客户端生命周期解耦的流式填充。
- metadata contention 有 stale 时直接服务 stale、否则 join；content contention 始终 join；passthrough 独立回源。
- ETag/Last-Modified 绑定来源 upstream，不跨 origin 发送 validator。
- 本地目录、临时文件或对象发布失败不得把有效上游响应转换成下游 5xx；使用 `X-Cache: BYPASS`。
- 已知摘要的对象校验通过后才能发布到缓存。大对象使用临时文件和流式 reader，禁止整体 `io.ReadAll`。
- 路径先 `path.Clean` 再经 `httpcache.SafePath`；未知目标 host 由 `httpcache` 中央校验。

## Linux 仓库

- APKINDEX、Pacman database/signature 是 mutable metadata；包与 sidecar 是 stable content。
- Debian `InRelease`/`Release` 是请求锚点。依赖 metadata 必须出现在该请求捕获的 SHA256 清单中，并按大小和摘要校验后发布。
- Debian canonical 与 by-hash 只可映射到同一份已验证内容；不同压缩路径是独立对象。
- RPM `repomd.xml` 是请求锚点。依赖 metadata 必须由该请求捕获的 repomd 声明，并按声明大小和摘要校验。
- 未声明的 Debian/RPM metadata 返回 404；artifact 获取不依赖索引成员关系。
- 每个锚点请求固定一个 upstream；不得在同一请求中混合镜像版本。

## 特殊 mode

- NPM、Cargo、PyPI 和 Flatpak 的下游重写通过 Driver 注入的类型化 response transform 完成；共享缓存不按 mode 字符串分派。
- OCI 保留 token/auth、blob digest 和 manifest ref 状态；digest 对象必须校验。
- Flatpak summary/config 是 metadata；OSTree object 发布前校验 digest；static delta 作为有限 retention 的 opaque content，依赖客户端验证。
- Git 使用一个 bare mirror 和读写锁；冷启动及同步持锁期间代理 smart HTTP；串行 `git_sync` 任务 clone/fetch。

## 调度、准入与资源

- scheduler 是非持久化的单 goroutine 串行循环，只运行 expiration cleanup、blob GC 和 Git sync。
- 所有上游流量共享按 host 归一化的 admission：限制 active body 与请求起始间隔。
- 只有真实上游 429 建立 cooldown，并遵守 `Retry-After`；本地排队、超时和取消不记作上游限流。
- 启动保留 `utils.CleanStaleTempFiles(24h)`。

## 测试

- 使用 `testify/require`、`httptest.NewServer` 和 `blobfs.Open(t.TempDir(), blobfs.DefaultConfig())`。
- 行为变化补聚焦测试；并发逻辑执行 `-race`。
- 非可信 metadata、路径和重写解析器保留有界 fuzz target。
- 测试只描述当前协议与运行时行为；实现调整时同步清理失效和重复测试。

## 文档

- `README.md` 面向使用者，按项目简介、安装、快速开始、配置、运行维护和开发顺序组织。
- 文档描述当前公开契约和最终运行行为；变更过程与评审记录由提交、PR 或临时设计文件承载。
- 配置示例必须能通过当前严格配置校验，命令、端点、镜像标签和 fuzz target 必须与仓库保持一致。
