export enum ProxyMode {
  File = 'file',
  Oci = 'oci',
  Npm = 'npm',
  Go = 'go',
  Maven = 'maven',
  Cargo = 'cargo',
  PyPI = 'pypi'
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
  configVersion: number;
  generation: number;
  instances: number;
  handlers: number;
  requests: number;
  errors: number;
  upstreams: number;
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
  publicUrl?: string;
  entryKind?: string;
  entryLabel?: string;
  entryUrl?: string;
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
  publicUrl?: string;
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

export type ModePolicy = FilePolicy | OciPolicy | NpmPolicy | GoPolicy | MavenPolicy | CargoPolicy | PyPIPolicy;

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
  sumdb?: GoSumDBConfig;
  goprivate?: string[];
  disableModuleFetchHeader?: boolean;
}

export interface MavenPolicy {
  metadataFreshFor?: string;
  metadataBusyPolicy?: BusyPolicy;
  releasePolicy?: CachePolicy;
  snapshotPolicy?: CachePolicy;
  snapshotFreshFor?: string;
  rules: MavenRule[];
}

export interface MavenRule {
  match: string;
  policy: CachePolicy;
  freshFor?: string;
  expireAfter?: string;
}

export interface CargoPolicy {
  indexFreshFor?: string;
  indexBusyPolicy?: BusyPolicy;
  cratePolicy?: CachePolicy;
  authRequired?: boolean;
}

export interface PyPIPolicy {
  simpleFreshFor?: string;
  simpleBusyPolicy?: BusyPolicy;
  filePolicy?: CachePolicy;
  proxyJson?: boolean;
  proxyCoreMetadata?: boolean;
  proxySignatures?: boolean;
}

export interface GoSumDBConfig {
  enabled: boolean;
  name?: string;
  url?: string;
}

export interface ExportBundle {
  generation: number;
  global: GlobalConfig;
  instances: InstanceSpec[];
}

export interface StorageStats {
  TxID: number;
  Tenants: number;
  Inodes: number;
  Objects: number;
  Directories: number;
  Manifests: { Active: number; Deleted: number };
  Chunks: { Active: number; GarbageCandidate: number; Deleted: number; Corrupt: number };
  Segments: { Sealed: number; Compacting: number; Deleted: number; Corrupt: number };
  Bytes: { LogicalObjectBytes: number; RawChunkBytes: number; StoredChunkBytes: number };
  GC: {
    Runs: number;
    LastEpoch: number;
    LastRunState: string;
    LastBackgroundAt?: string;
    LastBackgroundEpoch: number;
    LastBackgroundError?: string;
  };
  GeneratedAt?: string;
}

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
