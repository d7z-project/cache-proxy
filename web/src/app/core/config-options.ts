import { BusyPolicy, CachePolicy, ListenKind, NpmResourcePolicy, OciAuthType, OciResourcePolicy, ProxyMode } from './api.models';

export interface SelectOption<T extends string> {
  value: T;
  label: string;
  description: string;
}

export const PROXY_MODE_OPTIONS: SelectOption<ProxyMode>[] = [
  { value: ProxyMode.File, label: '文件代理', description: '适合镜像站和制品仓库静态文件。' },
  { value: ProxyMode.Oci, label: '容器镜像代理', description: '适合容器镜像仓库。' },
  { value: ProxyMode.Npm, label: 'npm 包代理', description: '适合 npm registry。' }
];

export const CACHE_POLICY_OPTIONS: SelectOption<CachePolicy>[] = [
  { value: CachePolicy.Bypass, label: '直接转发', description: '请求直接访问上游。' },
  { value: CachePolicy.Immutable, label: '固定缓存', description: '适合版本固定的资源。' },
  { value: CachePolicy.Revalidate, label: '校验缓存', description: '适合可能更新的资源。' }
];

export const BUSY_POLICY_OPTIONS: SelectOption<BusyPolicy>[] = [
  { value: BusyPolicy.Bypass, label: '直接转发', description: '并发刷新时新请求访问上游。' },
  { value: BusyPolicy.Stale, label: '使用现有缓存', description: '并发刷新时优先快速响应。' }
];

export const LISTEN_KIND_OPTIONS: SelectOption<ListenKind>[] = [
  { value: ListenKind.Path, label: '路径访问', description: '通过统一入口访问。' },
  { value: ListenKind.Bind, label: '独立端口', description: '通过专用端口访问。' }
];

export const OCI_AUTH_OPTIONS: SelectOption<OciAuthType>[] = [
  { value: OciAuthType.None, label: '匿名访问', description: '适合公开仓库。' },
  { value: OciAuthType.Basic, label: '用户名密码', description: '适合账号访问。' },
  { value: OciAuthType.Bearer, label: '访问令牌', description: '适合固定令牌访问。' }
];

export const OCI_RESOURCE_POLICY_OPTIONS: SelectOption<OciResourcePolicy>[] = [
  { value: OciResourcePolicy.All, label: '全部资源', description: '匹配 blob、manifest 和 tag。' },
  { value: OciResourcePolicy.Blob, label: 'Blob', description: '只匹配镜像层。' },
  { value: OciResourcePolicy.Manifest, label: 'Manifest', description: '只匹配清单。' },
  { value: OciResourcePolicy.Tag, label: 'Tag', description: '只匹配标签列表。' }
];

export const NPM_RESOURCE_POLICY_OPTIONS: SelectOption<NpmResourcePolicy>[] = [
  { value: NpmResourcePolicy.All, label: '全部资源', description: '匹配包信息和下载文件。' },
  { value: NpmResourcePolicy.Metadata, label: '包信息', description: '只匹配包元数据。' },
  { value: NpmResourcePolicy.Tarball, label: '下载文件', description: '只匹配 .tgz 文件。' }
];

export const FILE_DEFAULT_RULES = [
  { match: '**/*.iso', policy: CachePolicy.Immutable, freshFor: '', expireAfter: '' },
  { match: '**/*.rpm', policy: CachePolicy.Immutable, freshFor: '', expireAfter: '' },
  { match: '**/repodata/**', policy: CachePolicy.Revalidate, freshFor: '', expireAfter: '' }
];
