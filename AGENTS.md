# AGENTS.md

本文件描述项目当前有效的工程约束。代码、测试和文档必须同时遵守；架构规则变化时同步更新本文件。

## Proxy mode

1. 在 `pkg/proxy/<mode>/` 实现 `Plan(context.Context, *proxyruntime.InstancePlan) error`
2. 在 `pkg/config/config.go` 注册 mode，并在 `pkg/app/drivers.go` 注册对应的 `Plan`
3. 协议包负责请求分类、缓存身份、freshness、校验、上游选择和协议状态
4. 通用响应对象使用 `storeio`；Linux 仓库 metadata 使用 `filerepo.GenerationManager`
5. Flatpak/OSTree single-file 与 indexed summary 分别使用 generation，immutable objects 和 finite-retention deltas 使用稳定响应键
6. 配置或外部行为变化时同步更新 `README.md`
7. 所有 mode 对上游只读；新增协议能力不得引入发布、上传、删除或其他上游变更操作
8. 每个 instance 配置一个必填主 `upstream`；上游高可用由该地址前方的 DNS 或负载均衡提供

## Go 代码结构

- helper 必须对应实际复用或清晰的职责边界；短小单次转发逻辑保持内联
- `transport` 统一 upstream HTTP client、body idle timeout、URL path segment 转义、通用 revalidation/cacheability 判断和响应转发；协议包保留身份、凭据作用域和生命周期决策
- HTTP freshness 的纯计算与 header 合并位于 runtime，供 transport 和 filerepo 共用；storeio 记录响应接收时间与年龄，304 不重置内容创建时间
- `artifactcache` 统一稳定包对象的 fill、flight、conditional revalidation、stale 和 streaming 生命周期；协议包保留路径分类、缓存键、freshness 和内容校验
- 缓存响应直接使用 `storeio.ResponseObject`；协议专用结构只承载额外状态
- npm / PyPI 的签名下载地址复用 `signedtoken` 的有界 HMAC envelope；payload 字段、期限、digest 和目标 URL 仍由协议包校验
- 内部字段和跨函数状态使用完整领域名称，局部循环变量使用 Go 惯用短名称
- error 文本小写开头，通过 `%w` 保留可判定错误链
- 协议、缓存键、持久化格式或调度时序变化必须明确记录并补测试
- 大块逻辑按状态转换和资源所有权拆分，避免薄包装与重复生命周期代码
- filerepo 按 generation 管理、刷新调度、对象校验、状态持久化和 GC 分文件，保持同包内直接协作；不为文件拆分引入额外接口层

## YAML 与运行时配置

- YAML 字段使用 `snake_case`，必填字段不使用 `omitempty`
- duration、expiration 和 byte size 分别复用 `config.Duration`、`config.Expiration`、`config.ByteSize`
- 配置保持严格解码，未知字段必须报错
- instance 的可选 refresh.interval 至少为 1s，约束可变 metadata 的本地期限与周期检查；上游更短期限优先，Git 使用 options.sync_interval；immutable identity 与 retention 不受该字段影响
- `upstream` 是每个 instance 的必填 HTTP(S) 标量；高可用由该地址前方的 DNS 或负载均衡提供
- 每个 instance 使用独立的 `<backend>/instances/<name>/<mode>/{blobs,state,work}`
- 自建持久化 state 只描述当前结构，使用严格解码以及身份、路径或摘要校验
- response metadata 的读取、更新、删除和 GC 共用严格校验；ValidatedAt 描述验证接收时间，CreatedAt 描述当前内容创建时间；backend 格式与运行构建绑定
- 所有下载临时文件共享进程级 spool budget；`StartStream` 和 `CaptureResponse` 必须使用 plan 派生的 `Spooler`
- handler 在构造阶段固定持有 plan 派生的 `Spooler`；请求热路径不得重复从 work 目录解析或创建 spooler

## Linux 仓库 metadata

- 一个 generation 内的 anchor、metadata、签名和校验文件来自同一配置 upstream
- anchor SHA256 是 generation identity；每次 staging 使用独立随机 `candidate_id`
- candidate 位于 `generations/<root-hash>/<generation>/<candidate-id>/`
- refresh 下载并校验协议要求的 closure 后发布；协议明确允许不可用的对象只有在上游返回 `403` / `404` 时才能从 candidate 省略，其他失败必须中止发布
- 启动只恢复 `current.yaml` 精确引用、且 upstream 与当前配置完全一致并通过完整校验的 current/previous snapshot；禁止扫描 generation 目录推测可用版本
- 首次合格 anchor 请求立即透传上游，同时由生命周期 context 捕获；并发请求读取完成的 pending/current anchor
- 已有 current 时 metadata 优先读取 current；仅当 current 不含请求路径时，协议明确标记且路径可绑定版本的对象才能从无歧义的 previous snapshot 读取；其余 miss 或本地 blob 丢失触发 refresh 并交回协议 handler，以原请求对同一 upstream 透明 `BYPASS`，禁止生成 `503` 或负缓存状态
- current miss 或本地 blob 丢失触发的 refresh 必须在 anchor 返回 `304` 或内容 digest 未变化时重新构建 candidate，使恢复可用的对象无需等待 anchor 变化即可进入 snapshot；普通周期 poll 不重建未变化的 candidate
- current anchor 收到显式 `no-cache` / `max-age=0` 或上游要求验证时，等待验证及必要的原子发布；请求等待最多 30s，取消或超时不取消 scheduler 工作
- generation 验证时间与 header 存于绑定精确 candidate 的 current marker，不改写 snapshot 或 PublishedAt；no-store 退役该 root 的 current/pending 路由，active reader 继续受 GC 保护
- artifact 和 package sidecar 使用 generation-independent response key，且不依赖 metadata refresh 成功
- Debian 支持标准、嵌套和 flat root；InRelease 与 Release 同时存在时必须归一化一致
- Debian instance 根路径和未分类的同源资源透明直通；目录请求必须保留尾斜线
- Debian Release 的每个 strong-checksum entry 是独立可用性单元；上游 `200` 必须通过声明大小和所有 strong checksum 才能进入 snapshot，上游 `403` / `404` 则省略该 entry，`429`、其他非 `200`、传输或持久化错误必须中止 candidate
- Debian candidate 的续传对象键同时绑定精确 Release path 和 digest；相同内容的其他 entry 禁止被当作该路径已经验证可用。压缩、未压缩以及不同目录中的 entry 均保持独立
- Acquire-By-Hash 首选 by-hash，仅在其返回 `403` / `404` 时回退同 upstream canonical；canonical 通过原 entry 大小和摘要校验后才能与该 entry 的 by-hash alias 指向同一 blob；通过校验的 SHA256/SHA512 by-hash 路径可从精确 previous snapshot 读取，canonical 与固定名称签名保持 current-only
- RPM generation 发布 `repomd.xml` 与上游实际可用且通过 wire/open size 和 checksum 校验的引用对象；引用对象只有 `403` / `404` 可以省略，其他失败中止 candidate。RPM metadata 全部保持 current-only，禁止按 location 文件名推测不可变性并从 previous snapshot 回退
- Flatpak `summary.idx` generation 只绑定已验证索引及其签名；优先 digest-specific signature，仅在 `403` / `404` 时回退 `summary.idx.sig`。`summaries/<sha256>.gz` 按请求下载，解压后 SHA256 校验通过才进入不含 generation 的内容缓存，禁止预取其他架构或 subset
- OSTree delta index、detached commit metadata 与 indexed-summary delta 必须严格识别编码路径并使用有限的成功响应缓存，不绑定 summary generation且不缓存缺失响应
- metadata 解析、解压和状态读取必须有 byte、entry、token 或 expansion 上限

## 存储、调度与清理

- `storeio` 是 response path、签名密钥、临时下载、stream、flight 和响应清理的唯一通用实现
- logical response key 映射到 SHA256 分片路径，metadata 中保存并验证原 logical key
- 调度器单 goroutine 串行执行；达到 batch 上限时短延迟 continuation
- metadata refresh 按 repository root 串行处理 pending、显式 poll 和周期 poll；失败 candidate 使用有上限退避并在重试前重新验证 anchor
- 周期 poll 只选到期 root，以 instance/root 的稳定偏移在周期的 80%-100% 检查；失败 pending 不阻塞其他 root，零 freshness 不形成后台忙循环，任务执行预算与调度周期分离
- `TriggerNow` 只唤醒内存调度状态；scheduler 的单一执行循环在 metadata refresh/GC 完成后持久化其调度时间，metadata 请求热路径不执行状态文件 fsync
- response cleaner 及其游标由一个串行调度任务独占；任务事件通过 Result 和 Err 表达结果与完整错误上下文
- response、OCI 和 generation GC 按 inspected objects 计 batch，并通过内存游标继续
- generation GC 保护 current、pending、active reader、grace-period candidate 和 `current.yaml` 精确提交且校验通过的 previous candidate，并回收无引用或损坏 candidate 与空状态目录
- inactive current 或 pending root 通过 last-seen 与 generation GC 完整退役
- Git scheduler 串行执行 mirror sync；repository lock 保护 mirror 状态转换
- Git 本地响应必须 defer 释放读锁；upload-pack body 在获取 repository lock 前按 16 MiB 与 operation_timeout 有界读取，sync 使用 TryLock，忙时 2 秒后继续调度
- generation GC 跨批次只保留候选前缀与游标，不积累完整历史 snapshot 或对象键列表；删除前在 commitMu 下再次检查运行时引用

## 网络、安全与资源

- URL path 安全判断基于一次 percent decode 后的逻辑 segment；合法等价编码共享缓存身份，编码分隔符、反斜线、NUL 和父目录段必须在 clean/join 前拒绝
- `CleanURLPath` 接受根并保留一个目录尾斜线；metadata 内部引用继续使用不接受根与目录的 `CleanRelative`
- path mount 的 `GET` / `HEAD` 根请求统一 `308` 到尾斜线形式并保留 query；透明 mode 在 metadata manager 和对象缓存前直通根与目录
- 客户端 `RawPath` 不参与缓存身份或直接回源；回源 URL 必须由已验证的逻辑 segment 重新转义，unknown 资源由 mode 明确分类为协议对象或安全只读直通
- 主上游只来自当前 instance 的唯一配置；持久化 upstream 不能扩展允许访问的 origin
- Go SumDB、OCI token realm 和 HTTP redirect 是受协议约束的辅助端点，不构成备用主上游
- 普通上游请求只允许无 body 的 `GET` / `HEAD`；Git/Cargo 仅允许精确的 `git-upload-pack`，npm 仅允许两个精确 audit endpoint 使用只读 `POST`
- 禁止的方法必须在认证、admission、上游连接和缓存状态变更前拒绝；通用 transport 不接受任意 method 或请求 body
- 上游 read 请求必须移除 method override 与实体 header；非 read 重定向不得改变 origin、method 或 path
- 所有外部 5xx 使用 `runtime.WriteError` 或 transport 等价入口
- 下载与 metadata 捕获流式写入临时文件；已知 digest 的对象校验后发布
- conditional validator 绑定原 upstream；`304` 与相同摘要 `200` 推进 freshness
- 上游 body（包括 OCI token 响应）统一使用 transport 的单次阻塞读取超时，并由请求总期限限制完整传输时长
- admission 按每次实际 HTTP transport 请求的 host 执行，包括重定向各 hop；真实 `429` 只影响实际响应 host
- upstream body EOF/close 后立即释放 admission，本地校验和 publication 不占用传输槽
- 同一对象 miss/revalidate 使用 flight 合并；客户端断开不取消已开始的生命周期缓存填充
- 普通对象 spool 在传输中失败时停止缓存写入并继续向当前客户端传递剩余 upstream body
- 只有真实 upstream `429` 建立 host cooldown，并遵守 `Retry-After`
- admission 在释放、取消和定时唤醒时回收空闲动态 host，保留 pacing/cooldown 到期语义；额外动态 host 上限 4096，Snapshot 只读
- upstream 统计按 origin 聚合，每个 instance 上限 64 个 origin 与一个 other 桶；非标准 HTTP method 聚合为 OTHER
- stream 最后一个 reader 与 producer 退出后必须关闭遗留 fallback body，关闭操作在 growing-file mutex 外执行
- Flatpak/OSTree immutable objects 发布前校验；static delta 使用有限 retention 并依赖客户端验证内容
- 启动只清理 instance work 目录内超过 24 小时且以 `.cache-proxy-tmp-` 开头的自有文件

## 测试

- 使用 `github.com/stretchr/testify/require`
- 上游交互优先使用 `httptest.NewServer`
- 存储测试优先使用 `blobfs.Open(t.TempDir(), blobfs.DefaultConfig())`
- 行为变化必须补测试；并发和生命周期逻辑覆盖 `go test -race`
- 路径、分类器、状态和非可信 metadata parser 使用有界 fuzz target；语料通过 `make test-fuzz` 执行
- 并发 fuzz target 必须限制输入大小、goroutine 数和等待时间，并覆盖取消、提前关闭、共享资源争用和发布顺序
- 每个 mode 必须有原生客户端端到端覆盖；Debian standard 和 flat repository 分开验证
- E2E 同时覆盖透明 mode 的根、目录、unknown/query 资源以及 Go/OCI 的严格端点边界
- E2E case 和 fixture builder 按 mode 独立，fixture builder 与专用客户端镜像按 mode 分目录；只有断言、生命周期、运行时适配和基础镜像层允许共享
- E2E 由 `test/e2e/run.sh` 统一编排，Makefile 只暴露 `test-e2e` target，不承载容器生命周期或客户端命令
- E2E 的 proxy、fixture、探测和包客户端全部使用 Docker/Podman host network 容器；宿主不得直接运行包管理器
- E2E 每阶段使用全新客户端，覆盖 cold、warm、上游更新和持久 backend 离线重启；清理仅作用于本次 run label 的资源
- 最终验证包含 `gofmt`、`git diff --check`、全量测试、race、fuzz smoke、vet、静态构建和生产配置严格校验

## 文档

- `README.md` 面向使用者，按简介、能力、安装、快速开始、配置、客户端、运维和开发组织
- 架构约束记录在 `AGENTS.md`，公开功能、配置和运行行为记录在 `README.md`
- `README.md` 只描述当前公开行为；内部状态机细节仅在直接影响配置、数据或运维时记录
- 配置示例必须通过当前严格配置校验，命令、端点、镜像标签和测试入口必须与仓库保持一致
