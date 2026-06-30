# AGENTS.md

本项目的行为规范与约束。所有新增/修改代码必须遵守下述规则，不匹配的代码需一并进行适配。

## AGENTS.md 自身

- 上述所有规范变更时，必须同步更新本文件对应章节
- 新增约束规则时追加到对应分类下；如无合适分类则新建章节

## 添加新的 proxy mode

1. 在 `pkg/proxy/<mode>/policy.go` 中定义 `Block` 结构体（`yaml:",inline"` 嵌入 Policy）和 `Driver`
2. 实现 `proxyruntime.ModeDriver` 接口：`Mode()` 返回 `config.Mode<Xxx>`，`Plan()` 解码 Block → 创建 handler → `BindPath` 或 `BindAddr`
3. 在 `pkg/config/config.go` 的 `Instance` 中添加 `*ModeBlock` 字段，常量 `Mode<Xxx>`，并在 `SelectMode()` 候选列表追加
4. 在 `pkg/app/drivers.go` 的 `builtinDrivers()` 注册 `NewDriver()`
5. 如需路由规则支持，实现 `httpcache.Resolver`；如需元数据管理，基于 `filerepo.IndexedHandler` 构建（用户请求只负责路径发现，元数据刷新为纯定时器驱动）
6. 如需健康检查支持，在 `Plan()` 中通过 `httpcache.NewHandler` 的 `svcHealth` 参数注入 `health.ServiceHealth`（参考 `filerepo.health_factory.go`）；handler 内部通过 `openRemote()` 自动调用 `RecordResult`/`RecordFailure`，无需额外代码

## YAML 配置结构体

- `Block` 字段用 `yaml:"<snake_case>"` 标签，必须字段不加 `omitempty`
- Policy 内嵌用 `yaml:",inline"`
- 枚举值 (policy / busy policy) 在代码中定义为 `config.Policy*` / `config.BusyPolicy*` 常量
- 时长类型使用 `config.Duration` / `config.Expiration` / `config.Freshness`
- 每新增或修改字段，同步更新 README 对应 mode 的字段速查表（Field / Type / Default / Description 四列）
- Linux 仓库模式 (`apk` / `deb` / `rpm` / `pacman`) 的 metadata 不再暴露 `metadata_policy` / `metadata_fresh_for` / `metadata_busy_policy`；metadata 始终按 root generation 刷新和服务

## Linux 仓库 metadata generation

- 同一个 root generation 内的所有 metadata、签名、校验文件必须从同一个 upstream 获取；多 upstream 只能在 root generation 级别 failover，禁止单文件混用 upstream
- metadata refresh 必须先写 staging generation，所有 required metadata 和校验通过后才能发布 current generation
- 客户端 metadata 请求只允许读取 current generation；禁止走通用 httpcache 单文件 revalidate
- required metadata 缺失或校验失败时，本次 generation 必须失败，继续服务旧 generation；签名、文件列表等 companion 默认为 optional，FetchDerived 处理 404/403 为非致命
- artifact / auxiliary 下载不能因包索引缺失、refresh 失败或无 current generation 而被阻断；索引命中时绑定 current generation 的 upstream 和 identity，能得到 SHA256 时必须校验后才写入缓存
- 伴生文件推导：每个主元数据文件 X 自动推导并尝试缓存 X.sig、X.asc、X.gpg（FetchDerived 处理 404/403 为非致命）；模式可追加额外伴生（如 RPM 加 .key）
- 元数据刷新为后台驱动，用户请求仅负责路径发现；无本地缓存时直接 bypass 到上游并发布 `EventMetadataDiscovered` 事件到总线，由调度器注册刷新任务
- metadata 下载与解压解析必须使用临时文件/reader 流式处理，禁止对 Packages/primary 等大型 metadata 使用 `io.ReadAll` 全量读入内存
- 伴生文件 `FetchDerived` 同时处理 404 和 403（均为非致命），403 不会导致 generation 失败

## 测试

- 使用 `github.com/stretchr/testify/require`
- 上游模拟：`httptest.NewServer`
- 存储模拟：`blobfs.Open(t.TempDir(), blobfs.DefaultConfig())`
- 提取复用辅助函数：`newTestHandler(t, ...)`, `newTestStore(t)`

## 安全约束

- 路径解析必须先 `path.Clean`，再用 `httpcache.SafePath` 检查
- 5xx 错误响应使用 `httpcache.ErrorResponse`（对外显示 `"internal error"`）
- 大文件下载（OCI blob / Cargo crate）必须通过 `utils.TempFileFromReader` 流式落盘，禁止 `io.ReadAll` 全量读入内存
- 直接 `TargetURL` 必须由 `httpcache` 统一校验，host 只能匹配已配置上游或 route-scoped allowlist；不允许各 resolver 自行放行未知 host
- metadata refresh 阶段 upstream failover：只对网络错误、`429`、`5xx` 重试；core metadata `403`/`404` 直接返回
- 运行时 artifact/auxiliary 请求（TargetURL）：索引命中时优先使用 generation 绑定 upstream，TargetURL 网络错误时退化为通用 upstream 轮询；HTTP 4xx 直接返回客户端；索引未命中按普通反代缓存处理
- 亲和性：给定 root 的 artifact/auxiliary/unknown 请求优先使用该 root 当前 generation 绑定的 upstream
- 包索引只用于下载增强（upstream 亲和、identity、SHA256 校验）和清理旧 indexed 缓存；不得作为 artifact/auxiliary 下载准入条件
- 已知 SHA256/digest 的大文件保持首包流式返回，但校验失败不得写入缓存
- OCI manifest 的 `Docker-Content-Digest` 和 OCI blob 请求 digest 必须校验后才写入 state/cache
- 启动时调用 `utils.CleanStaleTempFiles(24h)` 清理残留临时文件

## 日志

- 生产环境默认 JSON 格式 (`slog.NewJSONHandler`)，`LOG_LEVEL=debug` 切换为 text 格式
- 上游请求 URL 使用 `httpcache.redactedURL` 脱敏后记录

## 健康检查 (pkg/health)

`ServiceHealth` 提供统一的服务健康管理，包括：
- **上游传输层健康**：per URL 的状态机 (Closed/Degraded/Open/HalfOpen)，滑动窗口错误率评估，EWMA 延迟追踪，金丝雀逐级恢复
- **资源级健康**：per repo 的状态机 (Pending/Active/Suspect/Blocked/Removed)，错误计数，熔断恢复
- **透明故障转移**：`WeightedUpstreams()` 按权重排序上游列表，权重 0 的跳过，全部 0 时 bypass 兜底
- **可观测性**：5 个 Prometheus 指标 (`cache_proxy_upstream_health`, `_weight`, `_error_rate`, `_latency_seconds`, `cache_proxy_circuit_breaker_events_total`)

核心 API：
- `health.New(name, mode, config, upstreams, stats, ua)` — 创建，config 从 `health.DefaultConfig()` 开始覆盖
- `health.Start(ctx)` / `health.Stop(ctx)` — 启动/停止探测定时器；Start 必须传入实例 lifecycle context
- `health.WeightedUpstreams(upstreams)` — 取权重排序的上游列表
- `health.RecordResult(url, status, latency)` / `health.RecordFailure(url, err)` — 被动记录请求结果（驱动错误率窗口 + 状态机）
- `health.TryStartRefresh(path)` / `health.FinishRefresh(path, gen, err, targets)` — CAS 防重入的刷新生命周期 (filerepo)
- `health.AddResource(path, targets, upstreams)` — 注册新资源
- `health.DashboardStatus()` — 返回仪表盘 (color, label, extra)
- `health.AggregateState()` — 返回整体状态 (Healthy/Degraded/Unhealthy)

### 状态机

- **Closed**: 正常合闸，`errorRate > degradeRate` 或 `latency > degradeLatency` 进入 Degraded；`errorRate >= tripRate` 直接熔断 Open
- **Degraded**: 降级限流，`errorRate <= degradeRate` 且 `latency <= degradeLatency` 恢复 Closed；`errorRate >= tripRate` 熔断 Open
- **Open**: 熔断零流量；cooldown 后主动探测成功进入 HalfOpen
- **HalfOpen**: 金丝雀逐步放量 (0.1 → +0.1 → 上限 0.5 → Closed)；任何失败立即回 Open

### 错误率模型

- 固定容量时间桶环形缓冲区（每桶 1s，最大 2KB/上游），`evaluation_window` 内滑动计算
- 最小样本 10 个才启用错误率判断；主动探测 Closed/Degraded 每 `probe_interval`，Open/HalfOpen 每 `canary_cooldown`

编写健康检查相关逻辑时：
- 主动和被动事件统一走 `recordSuccess` / `recordFailure`，由 `evaluateRate` 统一驱动状态转换
- 探测结果 404/403 不标记上游不健康，而是更新对应的 ResourceHealth
- `context.Canceled` 不计入故障计数
- 全部测试需用 `-race` 验证，ServiceHealth 使用单一 `sync.RWMutex` 无死锁
- 状态切换统一输出 debug 日志，格式：`"upstream state change" from=xxx to=yyy weight=N error_rate=N reason=reason`
- 故障转移透明：`openRemote()` 循环依次尝试，返回首个成功响应；failover 通过 debug 日志记录
- 5 个 Prometheus 指标通过 `StatsRecorder.SetUpstreamHealth` 和 `RecordCircuitEvent` 写入

## 事件总线 (pkg/bus)

纯数据 pub/sub，用于解耦跨组件通信。总线只传输数据，不携带函数引用。

事件类型：
- `EventMetadataDiscovered` — 新的元数据子路径被 proxy handler 发现，payload 包含 Instance 和 SubPath
- `EventMetadataRemoved` — 元数据根被移除

接口：
- `bus.New() *Bus` — 创建总线
- `Subscribe(types ...EventType) <-chan Event` — 订阅多种类型，缓冲 128
- `Publish(Event)` — 非阻塞发布，满则丢弃并记录 debug 日志

## 调度器 (pkg/scheduler)

程序级统一任务调度器，单 goroutine 串行执行所有定时任务，最大化避免并发内存峰值。

任务类型：
- `TypeBlobGC` — blob 存储 GC（系统级，App 注册）
- `TypeExpireCleanup` — 缓存过期清理（所有 proxy 注册）
- `TypeMetadataRefresh` — 元数据刷新（包 proxy 通过 Factory 动态注册）
- `TypeMetadataGC` — 元数据世代 GC（包 proxy 通过 Factory 动态注册）

核心 API：
- `New(bus, store) *Scheduler` — 创建，绑定总线和持久化存储
- `Register(TaskDef)` — 注册静态任务（Plan 阶段调用）
- `RegisterFactory(TaskFactory)` — 注册动态任务工厂（同步，必须在 Start 前）
- `Unregister(key)` / `Info(key)` / `Snapshot()` — 查询和管理
- `Start(ctx)` / `Stop(ctx)` — 生命周期

工厂模式：
- `TaskFactory` 在 Plan 时注册到调度器
- 发现事件到达时，调度器查找 factory，调用 `NewRefresh(subPath)` / `NewGC(subPath)` 创建 handler
- 重启时，`restoreFromStore()` 从持久化状态恢复任务，用已注册的 factory 重绑 handler
- `RegisterFactory` 是同步方法（不通过 cmdCh），确保在 `Start()` 前注册以供恢复使用

任务执行：
- 单 goroutine 命令循环：cmdCh + busSub + ticker(500ms) select
- 到期任务从最小堆取出，同步执行，完成后再入堆
- 每次执行有 context 超时保护：`min(interval/2, 30min, min=1min)`
- handler panic 被 `safeCall()` 捕获，记录日志，标记任务失败
- 失败退避：`failures * interval / 8`，上限 `interval/2`，下限 1min

持久化：
- 仅 `metadata_refresh` + `metadata_gc` 任务持久化到 `_scheduler/tasks.yaml`
- 静态任务（blob_gc, expire_cleanup）每次 Plan 重新注册，不持久化

### Plan 阶段任务注册

所有 proxy 的 `Plan()` 必须注册调度任务：

普通 proxy（file/npm/git/gomod/maven/pypi/oci/cargo）：
```go
plan.Scheduler().Register(scheduler.TaskDef{
    Key:      scheduler.NewTaskKey(plan.Name(), scheduler.TypeExpireCleanup, ""),
    Interval: 6 * time.Hour,
    Handler:  func(ctx context.Context) error { return handler.Cleanup(ctx, config.DefaultCleanupConfig()) },
})
```

包 proxy（deb/rpm/apk/pacman）：
```go
// 1. 注册过期清理
plan.Scheduler().Register(scheduler.TaskDef{...})
// 2. 注册动态工厂
plan.Scheduler().RegisterFactory(scheduler.TaskFactory{
    NewRefresh: func(subPath) TaskHandler { return handler.RefreshSubPath(ctx, subPath) },
    NewGC:      func(subPath) TaskHandler { return handler.CleanupSubPath(ctx, subPath) },
})
```
