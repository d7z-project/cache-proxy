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

export enum NpmResourcePolicy {
  Metadata = 'metadata',
  Tarball = 'tarball',
  All = '*'
}

export interface RuntimeInfo {
  bind: string;
  backend: string;
  authEnabled: boolean;
  metricsPath: string;
  gcInterval: string;
  generation: number;
  instances: number;
  handlers: number;
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
  freshFor?: string;
  expireAfter?: string;
}

export interface OciConfig {
  defaultPolicy?: CachePolicy;
  auth?: OciAuthConfig;
  rules: OciCacheRule[];
}

export interface OciCacheRule {
  match: string;
  policy: CachePolicy;
  freshFor?: string;
  expireAfter?: string;
}

export interface NpmConfig {
  defaultPolicy?: CachePolicy;
  rules: NpmCacheRule[];
}

export interface NpmCacheRule {
  match: string;
  resourcePolicy: NpmResourcePolicy;
  policy: CachePolicy;
  freshFor?: string;
  expireAfter?: string;
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

export interface CacheLookupResult {
  instance: string;
  mode: string;
  path: string;
  objectPath: string;
  policy: string;
  freshFor: string;
  expireAfter: string;
  generation: number;
  cached: boolean;
  cachedAt: string;
  expiresAt: string;
  fresh: boolean;
}
