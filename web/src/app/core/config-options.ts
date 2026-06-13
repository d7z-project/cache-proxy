import { BusyPolicy, CachePolicy, ListenKind, NpmResourcePolicy, OciAuthType, ProxyMode } from './api.models';

export interface SelectOption<T extends string> {
  value: T;
  label: string;
}

export const PROXY_MODE_OPTIONS: SelectOption<ProxyMode>[] = [
  { value: ProxyMode.File, label: '文件代理' },
  { value: ProxyMode.Oci, label: '容器镜像代理' },
  { value: ProxyMode.Npm, label: 'npm 包代理' },
  { value: ProxyMode.Go, label: 'Go 模块代理' }
];

export const CACHE_POLICY_OPTIONS: SelectOption<CachePolicy>[] = [
  { value: CachePolicy.Bypass, label: '直接转发' },
  { value: CachePolicy.Immutable, label: '固定缓存' },
  { value: CachePolicy.Revalidate, label: '校验缓存' }
];

export const BUSY_POLICY_OPTIONS: SelectOption<BusyPolicy>[] = [
  { value: BusyPolicy.Bypass, label: '直接转发' },
  { value: BusyPolicy.Stale, label: '使用现有缓存' }
];

export const LISTEN_KIND_OPTIONS: SelectOption<ListenKind>[] = [
  { value: ListenKind.Path, label: '路径访问' },
  { value: ListenKind.Bind, label: '独立端口' }
];

export const OCI_AUTH_OPTIONS: SelectOption<OciAuthType>[] = [
  { value: OciAuthType.None, label: '匿名访问' },
  { value: OciAuthType.Basic, label: '用户名密码' },
  { value: OciAuthType.Bearer, label: '访问令牌' }
];

export const NPM_RESOURCE_POLICY_OPTIONS: SelectOption<NpmResourcePolicy>[] = [
  { value: NpmResourcePolicy.All, label: '全部资源' },
  { value: NpmResourcePolicy.Metadata, label: '包信息' },
  { value: NpmResourcePolicy.Tarball, label: '下载文件' }
];
