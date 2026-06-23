# AGENTS.md

## 添加新的 proxy mode

1. 在 `pkg/proxy/<mode>/policy.go` 中定义 `Block` 结构体（`yaml:",inline"` 嵌入 Policy）和 `Driver`
2. 实现 `proxyruntime.ModeDriver` 接口：`Mode()` 返回 `config.Mode<Xxx>`，`Plan()` 解码 Block → 创建 handler → `BindPath` 或 `BindAddr`
3. 在 `pkg/config/config.go` 的 `Instance` 中添加 `*ModeBlock` 字段，常量 `Mode<Xxx>`，并在 `SelectMode()` 候选列表追加
4. 在 `pkg/app/drivers.go` 的 `builtinDrivers()` 注册 `NewDriver()`
5. 如需路由规则支持，实现 `httpcache.Resolver`；如需元数据发现刷新，基于 `filerepo.IndexedHandler` 构建

## YAML 配置结构体

- `Block` 字段用 `yaml:"<snake_case>"` 标签，必须字段不加 `omitempty`
- Policy 内嵌用 `yaml:",inline"`
- 枚举值 (policy / busy policy) 在代码中定义为 `config.Policy*` / `config.BusyPolicy*` 常量
- 时长类型使用 `config.Duration` / `config.Expiration` / `config.Freshness`
- 每新增或修改字段，同步更新 README 对应 mode 的字段速查表（Field / Type / Default / Description 四列）

## 测试

- 使用 `github.com/stretchr/testify/require`
- 上游模拟：`httptest.NewServer`
- 存储模拟：`blobfs.Open(t.TempDir(), blobfs.DefaultConfig())`
- 提取复用辅助函数：`newTestHandler(t, ...)`, `newTestStore(t)`

## 安全约束

- 路径解析必须先 `path.Clean`，再用 `httpcache.SafePath` 检查
- 5xx 错误响应使用 `httpcache.ErrorResponse`（对外显示 `"internal error"`）
- 大文件下载（OCI blob / Cargo crate）必须通过 `utils.TempFileFromReader` 流式落盘，禁止 `io.ReadAll` 全量读入内存
- 直接 `TargetURL` 仅允许 host 匹配已配置上游，其他一律走 `UpstreamPath`
- 启动时调用 `utils.CleanStaleTempFiles(24h)` 清理残留临时文件

## 日志

- 生产环境默认 JSON 格式 (`slog.NewJSONHandler`)，`LOG_LEVEL=debug` 切换为 text 格式
- 上游请求 URL 使用 `httpcache.redactedURL` 脱敏后记录
