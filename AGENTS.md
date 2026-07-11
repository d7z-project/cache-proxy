# AGENTS.md

本文件描述本项目当前仍然有效的工程约束。修改代码时，行为必须与这里保持一致；规则变化时同步更新本文件。

## 新增或修改 proxy mode

1. 在 `pkg/proxy/<mode>/` 实现 `Driver`，满足 `proxyruntime.ModeDriver`
2. 在 `pkg/config/config.go` 增加对应 mode 常量、`Instance` 字段和 `SelectMode()` 分支
3. 在 `pkg/app/drivers.go` 注册 `NewDriver()`
4. 需要缓存代理时优先复用 `httpcache`
5. Linux 仓库模式（`apk` / `deb` / `rpm` / `pacman`）必须基于 `filerepo.IndexedHandler`
6. Flatpak/OSTree 模式使用专用 handler，metadata 使用 generation，objects / deltas 不绑定 generation
7. 修改配置结构后，同步更新 `README.md`

## YAML 与配置

- YAML 字段使用 `snake_case`
- `Block` 必填字段不加 `omitempty`
- 复用 `config.Duration`、`config.Expiration`、`config.Freshness`
- policy / busy policy 使用 `config.Policy*`、`config.BusyPolicy*` 常量
- 已删除的配置不做兼容；配置解码保持严格模式
- Linux 仓库模式不暴露 `metadata_policy` / `metadata_fresh_for` / `metadata_busy_policy`

## Linux 仓库元数据

- 同一个 generation 内的 metadata、签名、校验文件必须来自同一个 upstream
- refresh 先写 staging，全部必需文件校验通过后才能发布 current generation
- 客户端 metadata 请求只读取 current generation；没有 current 时才允许直连上游并触发后台刷新
- 自动发现只允许由主元数据请求触发；伴生文件请求不能创建或识别新仓库
- artifact / auxiliary 下载不能依赖包索引命中，也不能因为 refresh 失败被阻断
- 包索引只用于清理旧缓存：refresh 阶段生成完整相对路径集合，并随 generation 持久化为本地 cleanup index，供后续清理工具直接读取
- cleanup index 不进入运行时长期内存，不作为下载校验或准入条件；metadata GC 删除旧 generation 时同步删除对应 cleanup index
- metadata 下载、解压、解析必须走流式 reader 或临时文件，禁止对大 metadata 整体 `io.ReadAll`
- 伴生文件获取里 `404` / `403` 视为非致命

## 调度与清理

- 调度器保持单 goroutine 串行执行，避免放大内存峰值
- 每个 proxy 在 `Plan()` 阶段注册过期清理任务
- Linux 仓库模式额外注册 metadata refresh / metadata GC factory
- 运行时清理参数统一来自 `plan.CleanupConfig()`
- 静态清理与 blob GC 不持久化；metadata refresh / GC 持久化到调度状态

## 安全与资源使用

- 路径处理先 `path.Clean`，再通过 `httpcache.SafePath`
- 5xx 对外响应统一用 `httpcache.ErrorResponse`
- 大文件下载必须流式写入临时文件，禁止全量读入内存
- `TargetURL` 校验统一由 `httpcache` 负责，不允许各 resolver 自行放行未知 host
- 已知 SHA256 / digest 的对象必须校验通过后才能写入缓存
- Flatpak/OSTree objects 必须在写入 immutable 缓存前完成校验
- Flatpak static deltas 可作为 opaque blob 按路径缓存，不做服务端语义校验；必须使用有限过期时间、路径安全校验，并在文档中说明依赖客户端校验
- 启动时保留 `utils.CleanStaleTempFiles(24h)`

## 测试

- 使用 `github.com/stretchr/testify/require`
- 上游交互优先用 `httptest.NewServer`
- 存储测试优先用 `blobfs.Open(t.TempDir(), blobfs.DefaultConfig())`
- 新增行为改动必须补对应测试；并发相关逻辑需覆盖 `-race`
- 删除或重写旧实现时，同时清理失效测试和重复测试
