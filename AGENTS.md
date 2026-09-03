# AGENTS.md

本文件描述项目当前有效的工程约束。代码、测试和文档必须同时遵守；架构规则变化时同步更新本文件。

## Proxy mode

1. 在 `pkg/proxy/<mode>/` 实现满足 `proxyruntime.ModeDriver` 的 `Driver`
2. 在 `pkg/config/config.go` 注册 mode，并在 `pkg/app/drivers.go` 注册 `NewDriver()`
3. 协议包负责请求分类、缓存身份、freshness、校验、上游选择和协议状态
4. 通用响应对象使用 `storeio`；Linux 仓库 metadata 使用 `filerepo.GenerationManager`
5. Flatpak/OSTree single-file 与 indexed summary 分别使用 generation，immutable objects 和 finite-retention deltas 使用稳定响应键
6. 配置或外部行为变化时同步更新 `README.md`
7. 所有 mode 对上游只读；新增协议能力不得引入发布、上传、删除或其他上游变更操作
8. 每个 instance 配置一个必填主 `upstream`；上游高可用由该地址前方的 DNS 或负载均衡提供

## Go 代码结构

- helper 必须对应实际复用或清晰的职责边界；短小单次转发逻辑保持内联
- `transport` 统一 upstream HTTP client、body idle timeout、URL path segment 转义、通用 revalidation/cacheability 判断和响应转发；协议包保留身份、凭据作用域和生命周期决策
- `artifactcache` 统一稳定包对象的 fill、flight、conditional revalidation、stale 和 streaming 生命周期；协议包保留路径分类、缓存键、freshness 和内容校验
- npm / PyPI 的签名下载地址复用 `signedtoken` 的有界 HMAC envelope；payload 字段、期限、digest 和目标 URL 仍由协议包校验
- 内部字段和跨函数状态使用完整领域名称，局部循环变量使用 Go 惯用短名称
- error 文本小写开头，通过 `%w` 保留可判定错误链
- 协议、缓存键、持久化格式或调度时序变化必须明确记录并补测试
- 大块逻辑按状态转换和资源所有权拆分，避免薄包装与重复生命周期代码

## YAML 与运行时配置

- YAML 字段使用 `snake_case`，必填字段不使用 `omitempty`
- duration、expiration 和 byte size 分别复用 `config.Duration`、`config.Expiration`、`config.ByteSize`
- 配置保持严格解码，未知字段必须报错
- `upstream` 是每个 instance 的必填 HTTP(S) 标量；高可用由该地址前方的 DNS 或负载均衡提供
- 每个 instance 使用独立的 `<backend>/instances/<name>/<mode>/{blobs,state,work}`
- 自建持久化 state 只描述当前结构，使用严格解码以及身份、路径或摘要校验
- 所有下载临时文件共享进程级 spool budget；`StartStream` 和 `CaptureResponse` 必须使用 plan 派生的 `Spooler`

## Linux 仓库 metadata

- 一个 generation 内的 anchor、metadata、签名和校验文件来自同一配置 upstream
- anchor SHA256 是 generation identity；每次 staging 使用独立随机 `candidate_id`
- candidate 位于 `generations/<root-hash>/<generation>/<candidate-id>/`
- refresh 完整下载并校验 closure 后发布；`current.yaml` 是唯一提交标记
- 启动只恢复 current 精确引用、且 upstream 与当前配置完全一致的 snapshot；不一致的 current/pending commit marker 立即失效
- 首次合格 anchor 请求立即透传上游，同时由生命周期 context 捕获；并发请求读取完成的 pending/current anchor
- 已有 current 时 metadata 只读 current；分类为 metadata 但不在 snapshot 中的路径返回 `503` 并触发 refresh
- current anchor 收到显式 `no-cache` / `max-age=0` 时触发后台 refresh，本次响应仍读取已提交 generation
- artifact 和 package sidecar 使用 generation-independent response key，且不依赖 metadata refresh 成功
- Debian 支持标准、嵌套和 flat root；InRelease 与 Release 同时存在时必须归一化一致
- Debian instance 根路径和未分类的同源资源透明直通；目录请求必须保留尾斜线
- Debian Release 中实际存在的每个压缩或未压缩表示按自身 strong checksum 独立校验，不同压缩格式禁止别名
- 带已识别压缩 sibling 的未压缩 checksum entry 允许上游缺失；没有压缩 sibling 的 entry 仍是必需对象
- Acquire-By-Hash 首选 by-hash，仅在其返回 `403` / `404` 时回退同 upstream canonical；canonical 通过原 entry 大小和摘要校验后才能与 by-hash 指向同一 blob
- RPM generation 只闭合 `repomd.xml` 及其精确引用对象并校验 wire/open size 与 checksum；未引用的同源 `repodata` 资源透明直通
- Flatpak indexed summary 的当前分片及可选索引签名必须与 `summary.idx` 同 generation；分片按解压后 SHA256 校验
- OSTree delta index 与 indexed-summary delta 必须严格识别编码路径并使用有限缓存，不绑定 summary generation；indexed-summary delta 不缓存缺失响应
- metadata 解析、解压和状态读取必须有 byte、entry、token 或 expansion 上限

## 存储、调度与清理

- `storeio` 是 response path、签名密钥、临时下载、stream、flight 和响应清理的唯一通用实现
- logical response key 映射到 SHA256 分片路径，metadata 中保存并验证原 logical key
- 调度器单 goroutine 串行执行；达到 batch 上限时短延迟 continuation
- metadata refresh 按 repository root 串行处理 pending、显式 poll 和周期 poll；失败 candidate 使用有上限退避并在重试前重新验证 anchor
- `TriggerNow` 只唤醒内存调度状态；scheduler 在任务完成后持久化，metadata 请求热路径不执行状态文件 fsync
- response、OCI 和 generation GC 按 inspected objects 计 batch，并通过内存游标继续
- generation GC 保护 current、pending、active reader、grace-period candidate 和至少一个 previous candidate，
  并回收无引用损坏 candidate 与空状态目录
- inactive current 或 pending root 通过 last-seen 与 generation GC 完整退役
- Git scheduler 串行执行 mirror sync；repository lock 保护 mirror 状态转换

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
- admission 按每次实际 HTTP transport 请求的 host 执行，包括重定向各 hop；真实 `429` 只影响实际响应 host
- upstream body EOF/close 后立即释放 admission，本地校验和 publication 不占用传输槽
- 同一对象 miss/revalidate 使用 flight 合并；客户端断开不取消已开始的生命周期缓存填充
- 普通对象 spool 在传输中失败时停止缓存写入并继续向当前客户端传递剩余 upstream body
- 只有真实 upstream `429` 建立 host cooldown，并遵守 `Retry-After`
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
- 配置示例必须通过当前严格配置校验，命令、端点、镜像标签和测试入口必须与仓库保持一致
