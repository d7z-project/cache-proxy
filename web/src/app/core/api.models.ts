export enum ProxyMode {
  File = 'file',
  Oci = 'oci',
  Npm = 'npm'
}

export enum CachePolicy {
  Bypass = 'bypass',
  Immutable = 'immutable',
  Revalidate = 'revalidate'
}

export enum BusyPolicy {
  Bypass = 'bypass',
  Stale = 'stale'
}

export enum ListenKind {
  Path = 'path',
  Bind = 'bind'
}

export enum OciAuthType {
  None = 'none',
  Basic = 'basic',
  Bearer = 'bearer'
}

export interface RuntimeInfo {
  adminBind: string;
  proxyBind: string;
  backend: string;
  metricsBind: string;
  metricsPath: string;
  gcInterval: string;
  generation: number;
  instances: number;
  servers: number;
  requests: number;
  errors: number;
  upstreams: number;
}

export interface InstanceSummary {
  name: string;
  mode: ProxyMode;
  path?: string;
  bind?: string;
}

export interface ConfigSnapshot {
  generation: number;
  config: AppConfig;
  yaml: string;
}

export interface AppConfig {
  version: number;
  server: ServerConfig;
  storage: StorageConfig;
  instances: Record<string, InstanceConfig>;
}

export interface ServerConfig {
  metrics: MetricsConfig;
}

export interface MetricsConfig {
  bind: string;
  path: string;
}

export interface StorageConfig {
  gc: { blob: string };
}

export interface InstanceConfig {
  mode: ProxyMode;
  listen: ListenConfig;
  upstreams: string[];
  transport?: TransportConfig;
  cache: CacheConfig;
  oci?: OciConfig;
  npm?: NpmConfig;
  passHeaders?: string[];
  expireAfter?: string;
}

export interface ListenConfig {
  path?: string;
  bind?: string;
}

export interface TransportConfig {
  proxy?: string;
  ua?: string;
  timeout?: string;
}

export interface CacheConfig {
  defaultPolicy?: CachePolicy;
  freshFor?: string;
  busyPolicy?: BusyPolicy;
  rules: CacheRule[];
}

export interface CacheRule {
  match: string;
  policy: CachePolicy;
}

export interface OciConfig {
  blobPolicy?: CachePolicy;
  manifestPolicy?: CachePolicy;
  tagPolicy?: CachePolicy;
  auth?: OciAuthConfig;
}

export interface NpmConfig {
  metadataPolicy?: CachePolicy;
  tarballPolicy?: CachePolicy;
}

export interface OciAuthConfig {
  type: OciAuthType;
  username?: string;
  password?: string;
  token?: string;
}

export type StorageStats = Record<string, unknown>;

export interface MetricsStats {
  total: InstanceMetrics;
  instances: Record<string, InstanceMetrics>;
}

export interface InstanceMetrics {
  mode?: ProxyMode;
  requests: number;
  errors: number;
  responseBytes: number;
  cache: Record<string, number>;
  upstreamRequests: number;
  upstreamErrors: number;
  upstreamStatus: Record<string, number>;
  activeDownloads: number;
}

export interface InstancesExport {
  generation: number;
  instances: Record<string, InstanceConfig>;
}
