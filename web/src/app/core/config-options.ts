import { BusyPolicy, CachePolicy, ListenKind, NpmResourcePolicy, OciAuthType, PackageResourceKind, ProxyMode } from './api.models';

export interface SelectOption<T extends string> {
  value: T;
  label: string;
  description: string;
}

export const PROXY_MODE_OPTIONS: SelectOption<ProxyMode>[] = [
  { value: ProxyMode.File, label: '文件代理', description: '适合软件包、静态文件和目录结构稳定的下载源。' },
  { value: ProxyMode.Oci, label: '容器镜像代理', description: '适合 Docker、containerd 和 Kubernetes 拉取镜像。' },
  { value: ProxyMode.Npm, label: 'npm 包代理', description: '适合 npm、pnpm 和 yarn 的包元数据与 tarball 缓存。' },
  { value: ProxyMode.Go, label: 'Go 模块代理', description: '适合 Go Modules 的 GOPROXY 和 SumDB 代理。' },
  { value: ProxyMode.Maven, label: 'Maven/Gradle 代理', description: '适合 Java 依赖仓库、Maven Central 和 Gradle Maven 仓库。' },
  { value: ProxyMode.Cargo, label: 'Cargo 包代理', description: '适合 Rust sparse registry 和 crate 下载缓存。' },
  { value: ProxyMode.PyPI, label: 'PyPI 包代理', description: '适合 Python Simple API、wheel 和 sdist 下载缓存。' },
  { value: ProxyMode.Apk, label: 'APK 仓库代理', description: '适合 Alpine APKINDEX 和 .apk 下载缓存。' },
  { value: ProxyMode.Deb, label: 'DEB 仓库代理', description: '适合 Debian/Ubuntu dists 元数据和 pool 制品缓存。' },
  { value: ProxyMode.Rpm, label: 'RPM 仓库代理', description: '适合 YUM/DNF repodata 和 RPM 包下载缓存。' },
  { value: ProxyMode.Pacman, label: 'Pacman 仓库代理', description: '适合 Arch Linux 数据库和包文件缓存。' }
];

export const CACHE_POLICY_OPTIONS: SelectOption<CachePolicy>[] = [
  { value: CachePolicy.Bypass, label: '直接转发', description: '每次都访问上游，不在本地保存对象。' },
  { value: CachePolicy.Immutable, label: '固定缓存', description: '第一次拉取后直接复用本地缓存，适合版本化资源。' },
  { value: CachePolicy.Revalidate, label: '校验缓存', description: '优先使用缓存，过期后向上游确认对象是否变更。' }
];

export const BUSY_POLICY_OPTIONS: SelectOption<BusyPolicy>[] = [
  { value: BusyPolicy.Bypass, label: '直接转发', description: '缓存正在刷新时，当前请求继续访问上游。' },
  { value: BusyPolicy.Stale, label: '使用现有缓存', description: '缓存正在刷新时，当前请求先返回旧缓存。' }
];

export const LISTEN_KIND_OPTIONS: SelectOption<ListenKind>[] = [
  { value: ListenKind.Path, label: '路径访问', description: '与管理界面共用主入口，通过路径前缀访问。' },
  { value: ListenKind.Bind, label: '独立端口', description: '单独监听一个地址，适合需要专属域名或客户端强绑定的场景。' }
];

export const OCI_AUTH_OPTIONS: SelectOption<OciAuthType>[] = [
  { value: OciAuthType.None, label: '匿名访问', description: '直接访问上游，不额外附带凭据。' },
  { value: OciAuthType.Basic, label: '用户名密码', description: '向上游发送 Basic Auth，适合私有镜像仓库。' },
  { value: OciAuthType.Bearer, label: '访问令牌', description: '使用固定 Bearer Token，适合受控服务账号。' }
];

export const NPM_RESOURCE_POLICY_OPTIONS: SelectOption<NpmResourcePolicy>[] = [
  { value: NpmResourcePolicy.All, label: '全部资源', description: '同时作用于包元数据和 tarball 下载文件。' },
  { value: NpmResourcePolicy.Metadata, label: '包信息', description: '仅作用于包清单和版本元数据。' },
  { value: NpmResourcePolicy.Tarball, label: '下载文件', description: '仅作用于 tgz 下载文件。' }
];

export const PACKAGE_RESOURCE_KIND_OPTIONS: SelectOption<PackageResourceKind>[] = [
  { value: PackageResourceKind.All, label: '全部资源', description: '同时作用于元数据、制品和关联辅助文件。' },
  { value: PackageResourceKind.Metadata, label: '元数据', description: '适合索引、仓库描述和校验清单。' },
  { value: PackageResourceKind.Artifact, label: '制品文件', description: '适合作为版本化下载对象长期复用。' },
  { value: PackageResourceKind.Auxiliary, label: '辅助文件', description: '适合签名、摘要和随主文件变化的 sidecar 元数据。' }
];
