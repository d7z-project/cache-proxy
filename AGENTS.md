# Agent 工程约束

本文件供 AI/agent 修改项目时使用，只记录必须遵守的边界与验收要求。
具体行为以代码、测试和配置定义为依据；不要将实现清单、修复过程或开发教程追加到这里。

## 修改范围与 Go 风格

- 先阅读相关实现与测试，再做最小必要修改；保留已有未提交变更，不夹带无关重构。
- 遵循 Go 惯用写法。内部状态使用清晰的领域名称，局部变量保持简洁；错误小写开头，通过 `%w` 保留错误链。
- helper 应有实际复用或明确职责边界；内联短小单次转发，合并重复生命周期逻辑，不为拆文件新增接口层。
- 大块逻辑按状态转换与资源所有权组织。精简不能削弱协议校验、并发安全或资源限制。
- 不为内部格式添加兼容层；协议规定的版本、端点及格式不能当作历史兼容代码删除。

## 职责边界

- 协议实现位于 `pkg/proxy/<mode>`，通过 `Plan` 接入配置与 app 注册；协议包负责分类、缓存身份、freshness、内容校验和凭据作用域。
- `transport` 负责通用 HTTP 传输、超时、安全 URL 构造和响应转发；共享 freshness 计算与 header 合并复用 `runtime`。
- `storeio` 负责通用响应存储、spool、stream、flight 与清理；稳定对象复用 `artifactcache`，签名下载地址复用 `signedtoken`。
- Linux 仓库 metadata 复用 `filerepo.GenerationManager`。包对象独立于 metadata generation，不依赖刷新成功。
- 不另建通用 HTTP cache 来替代协议判断，也不在协议包复制已有的通用资源管理实现。

## 上游与请求安全

- 每个 instance 只有一个必填 HTTP(S) `upstream`，所有上游操作只读。辅助端点、重定向及持久化状态不能扩大允许访问的 origin 或凭据范围。
- 普通请求仅允许无 body 的 `GET` / `HEAD`；只读 `POST` 仅限现有精确 Git/Cargo upload-pack 和 npm audit 端点。其他方法在认证、连接和状态变更前拒绝。
- 移除 method override 与不适用的实体 header；非 read 重定向不得改变 origin、method 或 path。
- 路径先做一次 percent decode，再按逻辑 segment 校验；在 clean/join 前拒绝编码分隔符、反斜线、NUL 和父目录段。回源重新转义，缓存身份不依赖客户端 `RawPath`。
- 保留透明 mode 的根、目录尾斜线、query 和安全 unknown 资源透传；Go/OCI 保持严格端点边界。内部 metadata 引用使用严格相对路径。

## 配置与持久化

- YAML 使用 `snake_case`，必填字段不用 `omitempty`；严格拒绝未知字段。时间与大小复用 `config.Duration`、`config.Expiration`、`config.ByteSize`。
- instance 数据隔离；持久化状态严格解码并验证 upstream、身份、路径及摘要。读取、更新、删除与 GC 使用一致的校验规则。
- 缓存键必须区分不同协议对象与上游；相同内容不能代替特定路径的可用性证明。
- freshness、内容创建时间与 retention 分开处理；条件验证绑定原 upstream。配置刷新间隔不能延长更短的上游策略、协议有效期或签名期限；遵守 `no-store`。

## 元数据一致性

- generation 内的 anchor、metadata、签名和校验文件来自同一 upstream；candidate 独立暂存，完成协议校验后原子发布。启动只恢复精确提交且验证通过的引用。
- 只有协议允许的对象在上游返回 `403` / `404` 时可省略；`429`、传输、校验或持久化失败不能发布不完整 candidate，也不能覆盖有效快照。
- canonical 与固定签名读取 current；previous 仅用于协议明确允许且能精确绑定内容版本的路径，不能按文件名猜测不可变性。RPM metadata 保持 current-only。
- 首次合格 anchor 可边透传边捕获；metadata miss 或本地缺失按协议恢复并回源，不能仅因缓存未准备好制造 `503` 或负缓存。校验失败的内容不得作为成功缓存响应。
- 修复缺失对象不能依赖 anchor 变化；普通周期检查不重建未变化的快照。显式强制验证必须等待验证结果，等待有界且不取消后台刷新。
- Debian 自动支持 standard、nested 与 flat。双 anchor 内容一致并遵守 `Valid-Until`；by-hash 索引按需验证下载，非 by-hash 构建完整 metadata snapshot。
- Debian 每个 Release entry 独立验证大小和全部 strong checksum；续传绑定精确路径与摘要。by-hash 仅在 `403` / `404` 且 entry 无歧义时回退同源 canonical，仍须通过原校验；lazy miss 不触发全量重建。
- Flatpak single-file/indexed summary 分别管理 generation；indexed summary 不预取其他架构或 subset，按需对象校验后缓存。可变 sidecar/delta 使用有限成功响应缓存，不绑定 summary generation。

## 并发与资源

- 下载流式写入临时文件，共享进程级 spool budget；handler 持有构造时获得的 Spooler。解析、解压、状态读取和动态集合必须有界。
- 同对象 miss/revalidate 合并 flight；客户端断开不取消已开始的生命周期填充。普通对象缓存写入失败时尽可能继续当前上游响应，不将缓存故障转为下载失败。
- 锁、body、spool、reader lease 和 admission 在成功、失败、取消路径均须释放；避免锁内阻塞 I/O 或关闭外部资源。上游 EOF/close 后释放传输槽，本地校验不占槽。
- admission 按实际请求 host 生效，覆盖重定向和认证请求；只有真实 `429` 建立该 host 的 cooldown，并遵守 `Retry-After`。连接、body 读取与总请求均有超时。
- scheduler 单 goroutine 串行执行，刷新平滑分布、失败有界退避；一个失败 root 不阻塞其他 root。请求热路径只触发内存状态，不做调度状态 fsync。
- GC 按检查对象数分批并用有界游标继续；保护 current、精确 previous、pending、active reader 和 grace-period 对象，删除前复查引用。清理仅作用于拥有的数据，过期临时文件和闲置动态状态应可回收。

## 测试与验收

- 行为变化必须补测试，断言当前有效行为；不保留无效测试或以已删除功能为主题的占位测试。
- 使用 `testify/require`；上游优先 `httptest.NewServer`，存储优先 `blobfs.Open(t.TempDir(), blobfs.DefaultConfig())`。
- 并发与生命周期改动跑 race，覆盖取消、提前关闭、共享资源争用和发布顺序；fuzz 限制输入、goroutine 数与等待时间。
- 每个 mode 保持原生客户端 E2E；Debian standard/flat、by-hash/canonical 分别覆盖，同时验证 cold、warm、上游更新与持久缓存离线重启。
- E2E 按 mode 拆分 case、fixture 和专用镜像，只共享基础设施；由 `test/e2e/run.sh` 编排，Makefile 只提供 target。
- E2E 的 proxy、fixture、探测及客户端都在 Docker/Podman host-network 容器运行；每阶段使用新客户端，清理仅限本次 run label 的资源。
- 代码改动最终执行格式检查、`git diff --check`、`make test`、`make test-race`、`make test-fuzz`、`make vet`、静态构建与配置严格校验；协议行为变化执行相关 E2E，共享协议路径变化执行全套 E2E。
- 纯文档改动检查链接、命令、示例与实际实现一致；涉及配置示例时执行严格校验。报告实际验证结果，未运行或失败的检查必须说明。

## 文档约束

- `README.md` 面向用户，描述当前能力、安装、配置、客户端接入及运维；配置示例使用英文注释说明用途与默认值。
- 公开行为或配置变化同步用户文档；工程边界变化同步本文件。实现细节优先由代码和测试表达，不在此重复函数、字段、算法步骤和限额表。
- 文档只保留当前有效内容，不记录修复过程、临时方案或一次性实验结果。
