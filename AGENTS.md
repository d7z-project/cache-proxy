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

## Go 代码结构

- 无附加行为的 HTTP mode 直接复用 `httpcache.Handler`，专用 handler 只承载协议或生命周期差异
- helper 以实际复用或清晰的职责边界为准；短小的单次转发逻辑保持内联
- 配置结构与运行时结构字段完全一致时复用同一领域类型，避免仅用于搬运字段的镜像类型
- 调用链始终传递有效的 `context.Context`，生命周期入口使用调用方 context 派生取消与超时
- 内部字段和跨函数状态使用完整领域名称，局部循环变量可使用 Go 惯用短名称
- error 文本使用小写开头，并通过 `%w` 保留可判定的错误链
- 重构保持协议、缓存键、持久化格式和调度时序稳定；行为变化必须独立说明并补测试

## YAML 与配置

- YAML 字段使用 `snake_case`
- `Block` 必填字段不加 `omitempty`
- 复用 `config.Duration`、`config.Expiration`、`config.Freshness`
- policy / busy policy 使用 `config.Policy*`、`config.BusyPolicy*` 常量
- 配置解码保持严格模式，未知字段必须报错
- Linux 仓库 metadata freshness 和 busy policy 由 generation refresh 机制统一管理

## Linux 仓库元数据

- 同一个 generation 内的 metadata、签名、校验文件必须来自同一个 upstream
- refresh 先写 staging，全部必需文件校验通过后才能发布 current generation
- 主元数据选定闭包内的每个 metadata 对象都必须获取并校验成功；只有外部伴生文件和主元数据明确未列出的请求目标允许提交 `403` / `404` 状态
- Debian Release SHA256 条目是签名校验描述，不代表每个路径都能直接下载；generation 只获取持久化 root targets 对应的逻辑闭包，禁止全量抓取无关组件、架构和辅助索引
- Debian 未压缩索引缺失时，只能从同一 Release 声明且已独立校验的完整压缩表示流式重建，并再次校验未压缩大小和 SHA256；不同压缩格式禁止互相别名，canonical 与 by-hash 仅能映射到同一份已验证字节
- Debian distribution 只有 Release anchor 时禁止发布 current generation；bootstrap metadata 保持直通，直到至少一个非 Release target 的完整闭包通过校验
- generation 提交 current 前必须确认 root closure revision 未变化；并发发现新 target 时禁止发布旧 target 子集，必须基于扩展后的 closure 重试
- 客户端 metadata 请求只读取 current generation；没有 current 时才允许直连上游并触发后台刷新
- 已有 current generation 时，清单中缺失的 metadata 请求必须返回 `503` 并触发刷新，禁止回源补齐
- 已有 root 的同 root metadata 发现必须扩展持久化 target 闭包，不得因 discovery create/update 角色差异形成永久 `503`
- 没有 current 时，不能识别 root 的 metadata 伴生文件必须直通且不得创建 root 或触发 refresh
- `current.yaml` 是唯一持久化提交标记；启动只能恢复其精确引用且完整校验通过的 snapshot，禁止回退选择最新 snapshot
- 自动发现只允许由主元数据请求触发；伴生文件请求不能创建或识别新仓库
- artifact / package sidecar 下载不能依赖包索引命中，也不能因为 refresh 失败被阻断
- 协议资源语义必须由各 mode inspector / resolver 分类；`httpcache` 和 `filerepo` 通用 resolver 禁止识别 Pacman、RPM 等协议文件名
- metadata 及其伴生文件绑定 generation；artifact 和 package sidecar 使用不含 generation 的稳定 content cache key
- 包索引只用于清理旧缓存：refresh 阶段生成完整相对路径集合，并随 generation 持久化为本地 cleanup index，供后续清理工具直接读取
- cleanup index 不进入运行时长期内存，不作为下载校验或准入条件；metadata GC 删除旧 generation 时同步删除对应 cleanup index
- metadata GC 必须保护内存 current、持久化 current 和活跃 reader generation，并保留宽限期内 generation 及至少一个最新 previous generation
- metadata 下载、解压、解析必须走流式 reader 或临时文件，禁止对大 metadata 整体 `io.ReadAll`
- 伴生文件获取里 `404` / `403` 视为非致命
- 大型 Debian refresh 必须使用绑定签名 Release digest 的持久化 staging 分片续传；锚点变化或 staging 校验失败时必须整体废弃候选 generation
- Flatpak 没有 current generation 时 summary 和伴生 metadata 直通；只有 summary 触发后台 generation refresh，前台请求不得等待 refresh 锁
- Git 使用不可变 generation 和唯一 `current.yaml` 提交标记；同步期间及同步失败后持续服务旧 current，尚无 current 时仅直通 Git smart HTTP
- OCI manifest 按 digest 不可变存储，ref `state.yaml` 是唯一原子提交标记；命中只能读取 state 精确引用的 digest 对象

## 调度与清理

- 调度器保持单 goroutine 串行执行，避免放大内存峰值
- 每个 proxy 在 `Plan()` 阶段注册过期清理任务
- Linux 仓库模式额外注册 metadata refresh / metadata GC factory
- 运行时清理参数统一来自 `plan.CleanupConfig()`
- 静态清理与 blob GC 不持久化；metadata refresh / GC 持久化到调度状态
- 清理达到 batch 上限必须短延迟续跑；inactive repository root 通过持久化 last-seen 和 generation GC 完整退役，禁止遗留 cleanup index 阻止退役
- 客户端下载、metadata refresh 和 OCI token 请求必须共用按 upstream host 归一化的 admission / cooldown；只有真实上游 `429` 建立 cooldown，并且必须遵守 `Retry-After`
- admission 同时限制 active body 和每 host 请求起始间隔；前台排队必须有界，refresh 在调度任务 context 内等待
- 本地 admission 等待、超时和 context 取消不得记为上游限流、健康失败或触发镜像回退；refresh 收到真实 `429` 后不得继续向其他镜像扩散请求

## 安全与资源使用

- 路径处理先 `path.Clean`，再通过 `httpcache.SafePath`
- 5xx 对外响应统一用 `httpcache.ErrorResponse`
- 大文件下载必须流式写入临时文件，禁止全量读入内存
- revalidate 使用单次 conditional GET；`304` 必须推进 `fetched-at`，瞬时失败有 stale 时不得追加第二次上游请求
- ETag / Last-Modified 必须记录来源 upstream，禁止跨 origin 发送条件校验器
- 上游 body 读取完成后立即释放传输 admission；校验和 blob 落盘不得继续占用上游并发槽
- 同一缓存对象的并发 miss / refresh 必须合并为一次上游传输，客户端断开不能取消已开始的后台缓存填充
- busy policy 语义固定为：`join` 合并传输，`stale` 优先旧对象且无旧对象时 join，`bypass` 独立回源
- `TargetURL` 校验统一由 `httpcache` 负责，不允许各 resolver 自行放行未知 host
- Cargo 下载模板只能携带路由信息，目标 host 必须来自显式 `allowed_crate_hosts` 或实例 upstream，元数据不得为自身目标授权
- 已知 SHA256 / digest 的对象必须校验通过后才能写入缓存
- Flatpak/OSTree objects 必须在写入 immutable 缓存前完成校验
- Flatpak static deltas 可作为 opaque blob 按路径缓存，不做服务端语义校验；必须使用有限过期时间、路径安全校验，并在文档中说明依赖客户端校验
- 完整读取并验证后的前台上游响应不能因 best-effort 缓存写入或引用提交失败转成下游错误；可安全返回临时表示时使用 `BYPASS`
- 启动时保留 `utils.CleanStaleTempFiles(24h)`

## 测试

- 使用 `github.com/stretchr/testify/require`
- 上游交互优先用 `httptest.NewServer`
- 存储测试优先用 `blobfs.Open(t.TempDir(), blobfs.DefaultConfig())`
- 新增行为改动必须补对应测试；并发相关逻辑需覆盖 `-race`
- 删除或重写旧实现时，同时清理失效测试和重复测试
- 解析器、路径分类和非可信 metadata 变更应补有界 fuzz target；常规 seed corpus 随 `make test` 执行，主动 fuzz 使用标准 `go test <package> -fuzz=<target>`
