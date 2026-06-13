export enum ProxyMode {
  File = 'file',
  Oci = 'oci',
  Npm = 'npm',
  Go = 'go'
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

export interface GlobalConfigResponse {
  generation: number;
  config: GlobalConfig;
}

export interface GlobalConfig {
  version: number;
  metrics: { path: string };
  storage: { gc: { blob: string } };
}

export interface InstanceCollectionResponse {
  generation: number;
  items: InstanceSummary[];
}

export interface InstanceSummary {
  name: string;
  mode: ProxyMode;
  enabled: boolean;
  path?: string;
  bind?: string;
}

export interface InstanceDocumentResponse {
  generation: number;
  spec: InstanceSpec;
}

export interface InstanceSpec {
  name: string;
  meta: InstanceMeta;
  route: InstanceRoute;
  source: InstanceSource;
  policy: ModePolicy;
}

export interface InstanceMeta {
  mode: ProxyMode;
  enabled: boolean;
  description?: string;
  expireAfter?: string;
}

export interface InstanceRoute {
  path?: string;
  bind?: string;
}

export interface InstanceSource {
  upstreams: string[];
  transport?: TransportConfig;
}

export interface TransportConfig {
  proxy?: string;
  ua?: string;
  timeout?: string;
}

export type ModePolicy = FilePolicy | OciPolicy | NpmPolicy | GoPolicy;

export interface FilePolicy {
  passHeaders?: string[];
  defaultPolicy?: CachePolicy;
  freshFor?: string;
  busyPolicy?: BusyPolicy;
  rules: FileRule[];
}

export interface FileRule {
  match: string;
  policy: CachePolicy;
  freshFor?: string;
  expireAfter?: string;
}

export interface OciPolicy {
  auth?: OciAuthConfig;
  defaultPolicy?: CachePolicy;
  freshFor?: string;
  busyPolicy?: BusyPolicy;
  rules: OciRule[];
}

export interface OciRule {
  match: string;
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

export interface NpmPolicy {
  defaultPolicy?: CachePolicy;
  freshFor?: string;
  busyPolicy?: BusyPolicy;
  rules: NpmRule[];
}

export interface NpmRule {
  match: string;
  resourcePolicy: NpmResourcePolicy;
  policy: CachePolicy;
  freshFor?: string;
  expireAfter?: string;
}

export interface GoPolicy {
  sumdb?: string;
  noSumDB?: string;
  proxiedSumDBs?: string[];
  disableModuleFetchHeader?: boolean;
}

export interface ExportBundle {
  generation: number;
  global: GlobalConfig;
  instances: InstanceSpec[];
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

export interface CacheLookupResult {
  instance: string;
  mode: string;
  policy: string;
  freshFor: string;
  expireAfter: string;
  generation: number;
  cached: boolean;
  cachedAt: string;
  expiresAt: string;
  fresh: boolean;
}
