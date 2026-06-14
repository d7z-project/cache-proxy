import {
  BusyPolicy,
  CachePolicy,
  CargoPolicy,
  FilePolicy,
  FileRule,
  GoPolicy,
  GoSumDBConfig,
  InstanceSpec,
  InstanceSummary,
  ListenKind,
  MavenPolicy,
  MavenRule,
  ModePolicy,
  NpmPolicy,
  NpmResourcePolicy,
  NpmRule,
  OciAuthType,
  OciPolicy,
  OciRule,
  ProxyMode,
  PyPIPolicy
} from '../../core/api.models';

export interface DraftLists {
  upstreams: string[];
  passHeaders: string[];
  goPrivatePatterns: string[];
}

export const BLOCKED_PASS_HEADERS = new Set([
  'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization', 'te', 'trailer',
  'transfer-encoding', 'upgrade', 'host', 'authorization', 'x-forwarded-for',
  'x-forwarded-host', 'x-forwarded-proto', 'x-forwarded-prefix', 'x-real-ip'
]);

export function extractDraftLists(spec: InstanceSpec): DraftLists {
  return {
    upstreams: [...(spec.source.upstreams ?? [])],
    passHeaders: [...(asFilePolicy(spec.policy)?.passHeaders ?? [])],
    goPrivatePatterns: [...(asGoPolicy(spec.policy)?.goprivate ?? [])]
  };
}

export function applyDraftDefaults(spec: InstanceSpec): InstanceSpec {
  const draft = structuredClone(spec);
  draft.meta.expireAfter = draft.meta.expireAfter ?? '720h';
  draft.source.transport = draft.source.transport ?? {};
  switch (draft.meta.mode) {
    case ProxyMode.File:
      draft.policy = {
        defaultPolicy: asFilePolicy(draft.policy)?.defaultPolicy ?? CachePolicy.Bypass,
        freshFor: asFilePolicy(draft.policy)?.freshFor ?? '30s',
        busyPolicy: asFilePolicy(draft.policy)?.busyPolicy ?? BusyPolicy.Bypass,
        passHeaders: asFilePolicy(draft.policy)?.passHeaders ?? [],
        rules: asFilePolicy(draft.policy)?.rules ?? []
      } as FilePolicy;
      break;
    case ProxyMode.Oci:
      draft.policy = {
        defaultPolicy: asOciPolicy(draft.policy)?.defaultPolicy ?? CachePolicy.Revalidate,
        freshFor: asOciPolicy(draft.policy)?.freshFor ?? '30s',
        busyPolicy: asOciPolicy(draft.policy)?.busyPolicy ?? BusyPolicy.Bypass,
        auth: asOciPolicy(draft.policy)?.auth ?? { type: OciAuthType.None },
        rules: asOciPolicy(draft.policy)?.rules ?? []
      } as OciPolicy;
      break;
    case ProxyMode.Npm:
      draft.policy = {
        defaultPolicy: asNpmPolicy(draft.policy)?.defaultPolicy ?? CachePolicy.Revalidate,
        freshFor: asNpmPolicy(draft.policy)?.freshFor ?? '30s',
        busyPolicy: asNpmPolicy(draft.policy)?.busyPolicy ?? BusyPolicy.Bypass,
        rules: asNpmPolicy(draft.policy)?.rules ?? []
      } as NpmPolicy;
      break;
    case ProxyMode.Go: {
      const goPolicy: GoPolicy = asGoPolicy(draft.policy) ?? {
        sumdb: { enabled: true, name: 'sum.golang.org', url: 'https://sum.golang.org' },
        goprivate: [],
        disableModuleFetchHeader: true
      };
      goPolicy.sumdb = normalizeGoSumDB(goPolicy.sumdb) ?? { enabled: false };
      goPolicy.goprivate = goPolicy.goprivate ?? [];
      draft.policy = goPolicy;
      break;
    }
    case ProxyMode.Maven:
      draft.policy = {
        metadataFreshFor: asMavenPolicy(draft.policy)?.metadataFreshFor ?? '30s',
        metadataBusyPolicy: asMavenPolicy(draft.policy)?.metadataBusyPolicy ?? BusyPolicy.Stale,
        releasePolicy: asMavenPolicy(draft.policy)?.releasePolicy ?? CachePolicy.Immutable,
        snapshotPolicy: asMavenPolicy(draft.policy)?.snapshotPolicy ?? CachePolicy.Revalidate,
        snapshotFreshFor: asMavenPolicy(draft.policy)?.snapshotFreshFor ?? '15s',
        rules: asMavenPolicy(draft.policy)?.rules ?? []
      } as MavenPolicy;
      break;
    case ProxyMode.Cargo:
      draft.policy = {
        indexFreshFor: asCargoPolicy(draft.policy)?.indexFreshFor ?? '30s',
        indexBusyPolicy: asCargoPolicy(draft.policy)?.indexBusyPolicy ?? BusyPolicy.Stale,
        cratePolicy: asCargoPolicy(draft.policy)?.cratePolicy ?? CachePolicy.Immutable,
        authRequired: Boolean(asCargoPolicy(draft.policy)?.authRequired)
      } as CargoPolicy;
      break;
    case ProxyMode.PyPI:
      draft.policy = {
        simpleFreshFor: asPyPIPolicy(draft.policy)?.simpleFreshFor ?? '30s',
        simpleBusyPolicy: asPyPIPolicy(draft.policy)?.simpleBusyPolicy ?? BusyPolicy.Stale,
        filePolicy: asPyPIPolicy(draft.policy)?.filePolicy ?? CachePolicy.Immutable,
        proxyJson: asPyPIPolicy(draft.policy)?.proxyJson !== false,
        proxyCoreMetadata: Boolean(asPyPIPolicy(draft.policy)?.proxyCoreMetadata),
        proxySignatures: Boolean(asPyPIPolicy(draft.policy)?.proxySignatures)
      } as PyPIPolicy;
      break;
  }
  return draft;
}

export function buildDefaultDraft(mode: ProxyMode): InstanceSpec {
  if (mode === ProxyMode.Oci) {
    return {
      name: 'dockerhub',
      meta: { mode, enabled: true, expireAfter: '720h' },
      route: { bind: '' },
      source: { upstreams: ['https://registry-1.docker.io'], transport: {} },
      policy: { defaultPolicy: CachePolicy.Revalidate, freshFor: '30s', busyPolicy: BusyPolicy.Bypass, auth: { type: OciAuthType.None }, rules: [] } as OciPolicy
    };
  }
  if (mode === ProxyMode.Npm) {
    return {
      name: 'npmjs',
      meta: { mode, enabled: true, expireAfter: '720h' },
      route: { path: '/npm' },
      source: { upstreams: ['https://registry.npmjs.org'], transport: {} },
      policy: { defaultPolicy: CachePolicy.Revalidate, freshFor: '30s', busyPolicy: BusyPolicy.Bypass, rules: [] } as NpmPolicy
    };
  }
  if (mode === ProxyMode.Go) {
    return {
      name: 'golang',
      meta: { mode, enabled: true, expireAfter: '8760h' },
      route: { path: '/go' },
      source: { upstreams: ['https://proxy.golang.org'], transport: {} },
      policy: { sumdb: { enabled: true, name: 'sum.golang.org', url: 'https://sum.golang.org' }, goprivate: [], disableModuleFetchHeader: true } as GoPolicy
    };
  }
  if (mode === ProxyMode.Maven) {
    return {
      name: 'maven-central',
      meta: { mode, enabled: true, expireAfter: '8760h' },
      route: { path: '/maven' },
      source: { upstreams: ['https://repo1.maven.org/maven2'], transport: {} },
      policy: { metadataFreshFor: '30s', metadataBusyPolicy: BusyPolicy.Stale, releasePolicy: CachePolicy.Immutable, snapshotPolicy: CachePolicy.Revalidate, snapshotFreshFor: '15s', rules: [] } as MavenPolicy
    };
  }
  if (mode === ProxyMode.Cargo) {
    return {
      name: 'crates-io',
      meta: { mode, enabled: true, expireAfter: '8760h' },
      route: { path: '/cargo' },
      source: { upstreams: ['https://index.crates.io'], transport: {} },
      policy: { indexFreshFor: '30s', indexBusyPolicy: BusyPolicy.Stale, cratePolicy: CachePolicy.Immutable, authRequired: false } as CargoPolicy
    };
  }
  if (mode === ProxyMode.PyPI) {
    return {
      name: 'pypi',
      meta: { mode, enabled: true, expireAfter: '8760h' },
      route: { path: '/pypi' },
      source: { upstreams: ['https://pypi.org'], transport: {} },
      policy: { simpleFreshFor: '30s', simpleBusyPolicy: BusyPolicy.Stale, filePolicy: CachePolicy.Immutable, proxyJson: true, proxyCoreMetadata: false, proxySignatures: false } as PyPIPolicy
    };
  }
  return {
    name: 'files',
    meta: { mode, enabled: true, expireAfter: '720h' },
    route: { path: '/files' },
    source: { upstreams: ['https://example.com'], transport: {} },
    policy: { defaultPolicy: CachePolicy.Bypass, freshFor: '30s', busyPolicy: BusyPolicy.Bypass, passHeaders: ['Accept', 'Accept-Language'], rules: [] } as FilePolicy
  };
}

export function normalizeSpec(draft: InstanceSpec, listenKind: ListenKind, lists: DraftLists): InstanceSpec {
  const spec = structuredClone(draft);
  spec.name = spec.name.trim();
  spec.meta.description = spec.meta.description?.trim() || undefined;
  spec.meta.expireAfter = normalizeDuration(spec.meta.expireAfter);
  spec.route = listenKind === ListenKind.Bind
    ? { bind: spec.route.bind?.trim() || undefined, publicUrl: spec.route.publicUrl?.trim() || undefined }
    : { path: spec.route.path?.trim() || undefined };
  spec.source.upstreams = lists.upstreams.map((value) => value.trim()).filter(Boolean);
  spec.source.transport = normalizeTransport(spec.source.transport);
  if (!spec.source.transport) delete spec.source.transport;
  switch (spec.meta.mode) {
    case ProxyMode.File:
      spec.policy = normalizeFilePolicy(asFilePolicy(spec.policy), lists.passHeaders);
      break;
    case ProxyMode.Oci:
      spec.policy = normalizeOciPolicy(asOciPolicy(spec.policy));
      break;
    case ProxyMode.Npm:
      spec.policy = normalizeNpmPolicy(asNpmPolicy(spec.policy));
      break;
    case ProxyMode.Go:
      spec.policy = normalizeGoPolicy(asGoPolicy(spec.policy), lists.goPrivatePatterns);
      break;
    case ProxyMode.Maven:
      spec.policy = normalizeMavenPolicy(asMavenPolicy(spec.policy));
      break;
    case ProxyMode.Cargo:
      spec.policy = normalizeCargoPolicy(asCargoPolicy(spec.policy));
      break;
    case ProxyMode.PyPI:
      spec.policy = normalizePyPIPolicy(asPyPIPolicy(spec.policy));
      break;
  }
  return spec;
}

export function validateSpec(draft: InstanceSpec | undefined, isCreate: boolean, listenKind: ListenKind, lists: DraftLists, items: InstanceSummary[]): string[] {
  if (!draft) return ['配置未加载完成。'];
  const errors: string[] = [];
  const name = draft.name.trim();
  if (!name) errors.push('实例名称不能为空。');
  if (name.includes('/') || name.includes('\\') || name === '.' || name === '..') errors.push('实例名称不能包含路径字符。');
  if (isCreate && items.some((item) => item.name === name)) errors.push(`实例名称 ${name} 已存在。`);

  const routeValue = listenKind === ListenKind.Bind ? draft.route.bind?.trim() : draft.route.path?.trim();
  const upstreams = lists.upstreams.map((value) => value.trim()).filter(Boolean);
  validateRoute(errors, routeValue, listenKind, draft.name, items);
  validateUpstreams(errors, upstreams, draft.meta.mode);
  validateTransport(errors, draft);
  validateModePolicy(errors, draft, listenKind, lists);
  return errors;
}

export function formState(draft: InstanceSpec | undefined, listenKind: ListenKind, lists: DraftLists): string {
  return JSON.stringify({ draft, listenKind, ...lists });
}

function normalizeTransport(transport?: InstanceSpec['source']['transport']) {
  if (!transport) return undefined;
  const next = {
    proxy: transport.proxy?.trim() || undefined,
    ua: transport.ua?.trim() || undefined,
    timeout: transport.timeout?.trim() || undefined
  };
  return next.proxy || next.ua || next.timeout ? next : undefined;
}

function normalizeDuration(value?: string): string | undefined {
  const trimmed = value?.trim();
  return trimmed || undefined;
}

function normalizeFilePolicy(policy: FilePolicy | undefined, passHeaders: string[]): FilePolicy {
  const next: FilePolicy = {
    ...policy,
    defaultPolicy: policy?.defaultPolicy ?? CachePolicy.Bypass,
    freshFor: normalizeDuration(policy?.freshFor),
    busyPolicy: policy?.busyPolicy ?? BusyPolicy.Bypass,
    passHeaders: passHeaders.map((header) => header.trim()).filter(Boolean),
    rules: (policy?.rules ?? []).map(normalizeRule)
  };
  if ((next.passHeaders?.length ?? 0) === 0) delete next.passHeaders;
  return next;
}

function normalizeOciPolicy(policy?: OciPolicy): OciPolicy {
  const auth = policy?.auth;
  return {
    ...policy,
    defaultPolicy: policy?.defaultPolicy ?? CachePolicy.Revalidate,
    freshFor: normalizeDuration(policy?.freshFor),
    busyPolicy: policy?.busyPolicy ?? BusyPolicy.Bypass,
    auth: !auth || auth.type === OciAuthType.None ? undefined : {
      type: auth.type,
      username: auth.username?.trim() || undefined,
      password: auth.password?.trim() || undefined,
      token: auth.token?.trim() || undefined
    },
    rules: (policy?.rules ?? []).map(normalizeRule)
  };
}

function normalizeNpmPolicy(policy?: NpmPolicy): NpmPolicy {
  return {
    ...policy,
    defaultPolicy: policy?.defaultPolicy ?? CachePolicy.Revalidate,
    freshFor: normalizeDuration(policy?.freshFor),
    busyPolicy: policy?.busyPolicy ?? BusyPolicy.Bypass,
    rules: (policy?.rules ?? []).map(normalizeRule)
  };
}

function normalizeGoPolicy(policy: GoPolicy | undefined, goPrivatePatterns: string[]): GoPolicy {
  return {
    ...policy,
    sumdb: normalizeGoSumDB(policy?.sumdb),
    goprivate: goPrivatePatterns.map((value) => value.trim()).filter(Boolean),
    disableModuleFetchHeader: Boolean(policy?.disableModuleFetchHeader)
  };
}

function normalizeMavenPolicy(policy?: MavenPolicy): MavenPolicy {
  return {
    ...policy,
    metadataFreshFor: normalizeDuration(policy?.metadataFreshFor),
    metadataBusyPolicy: policy?.metadataBusyPolicy ?? BusyPolicy.Stale,
    releasePolicy: policy?.releasePolicy ?? CachePolicy.Immutable,
    snapshotPolicy: policy?.snapshotPolicy ?? CachePolicy.Revalidate,
    snapshotFreshFor: normalizeDuration(policy?.snapshotFreshFor),
    rules: (policy?.rules ?? []).map(normalizeRule)
  };
}

function normalizeCargoPolicy(policy?: CargoPolicy): CargoPolicy {
  return {
    ...policy,
    indexFreshFor: normalizeDuration(policy?.indexFreshFor),
    indexBusyPolicy: policy?.indexBusyPolicy ?? BusyPolicy.Stale,
    cratePolicy: policy?.cratePolicy ?? CachePolicy.Immutable,
    authRequired: Boolean(policy?.authRequired)
  };
}

function normalizePyPIPolicy(policy?: PyPIPolicy): PyPIPolicy {
  return {
    ...policy,
    simpleFreshFor: normalizeDuration(policy?.simpleFreshFor),
    simpleBusyPolicy: policy?.simpleBusyPolicy ?? BusyPolicy.Stale,
    filePolicy: policy?.filePolicy ?? CachePolicy.Immutable,
    proxyJson: policy?.proxyJson !== false,
    proxyCoreMetadata: Boolean(policy?.proxyCoreMetadata),
    proxySignatures: Boolean(policy?.proxySignatures)
  };
}

function normalizeGoSumDB(sumdb?: GoSumDBConfig): GoSumDBConfig | undefined {
  if (!sumdb) return undefined;
  if (sumdb.enabled === false) return { enabled: false };
  return {
    enabled: true,
    name: sumdb.name?.trim() || 'sum.golang.org',
    url: sumdb.url?.trim() || 'https://sum.golang.org'
  };
}

function normalizeRule<T extends FileRule | OciRule | NpmRule | MavenRule>(rule: T): T {
  return {
    ...rule,
    match: rule.match.trim(),
    freshFor: normalizeDuration(rule.freshFor),
    expireAfter: normalizeDuration(rule.expireAfter)
  };
}

function validateRoute(errors: string[], routeValue: string | undefined, listenKind: ListenKind, currentName: string, items: InstanceSummary[]): void {
  const owners = items.filter((item) => item.name !== currentName);
  if (!routeValue) {
    errors.push(listenKind === ListenKind.Bind ? '监听地址不能为空。' : '路径前缀不能为空。');
    return;
  }
  if (listenKind === ListenKind.Path) {
    if (!routeValue.startsWith('/')) errors.push('路径前缀必须以 / 开头。');
    if (/\s/.test(routeValue)) errors.push('路径前缀不能包含空格。');
    if (routeValue.includes('//')) errors.push('路径前缀不能包含连续 /。');
    const normalized = '/' + routeValue.replace(/^\/+|\/+$/g, '');
    const owner = owners.find((item) => item.path === normalized);
    if (owner) errors.push(`路径前缀 ${normalized} 已被 ${owner.name} 使用。`);
    return;
  }
  if (!/^[^:]+:\d+$/.test(routeValue)) errors.push('监听地址需要使用 host:port 格式。');
  const owner = owners.find((item) => item.bind === routeValue);
  if (owner) errors.push(`监听地址 ${routeValue} 已被 ${owner.name} 使用。`);
}

function validateUpstreams(errors: string[], upstreams: string[], mode: ProxyMode): void {
  if (upstreams.length === 0) errors.push('至少需要一个上游地址。');
  if ((mode === ProxyMode.Oci || mode === ProxyMode.Npm || mode === ProxyMode.Cargo || mode === ProxyMode.PyPI) && upstreams.length !== 1) {
    errors.push('当前模式需要且只能配置一个上游地址。');
  }
  for (const upstream of upstreams) {
    try {
      const url = new URL(upstream);
      if (url.protocol !== 'http:' && url.protocol !== 'https:') errors.push(`上游 ${upstream} 必须是 HTTP/HTTPS。`);
    } catch {
      errors.push(`上游 ${upstream} 地址格式需要调整。`);
    }
  }
}

function validateTransport(errors: string[], draft: InstanceSpec): void {
  const transport = draft.source.transport;
  if (transport?.proxy?.trim()) {
    try {
      const url = new URL(transport.proxy.trim());
      if (url.protocol !== 'http:' && url.protocol !== 'https:' && url.protocol !== 'socks5:') errors.push('上游代理请选择 http、https 或 socks5。');
    } catch {
      errors.push('上游代理地址格式需要调整。');
    }
  }
  if (draft.meta.expireAfter?.trim() && !isValidDuration(draft.meta.expireAfter.trim())) errors.push('缓存保留时间格式无效。');
  if (transport?.timeout?.trim() && !isValidDuration(transport.timeout.trim())) errors.push('连接超时格式无效。');
}

function validateModePolicy(errors: string[], draft: InstanceSpec, listenKind: ListenKind, lists: DraftLists): void {
  if (listenKind === ListenKind.Path && draft.route.publicUrl?.trim()) {
    errors.push('公开地址仅用于独立端口实例。');
  }
  if (listenKind === ListenKind.Bind && draft.route.publicUrl?.trim()) {
    try {
      const url = new URL(draft.route.publicUrl.trim());
      if (url.protocol !== 'http:' && url.protocol !== 'https:') errors.push('公开地址必须是 HTTP/HTTPS。');
    } catch {
      errors.push('公开地址格式需要调整。');
    }
  }
  switch (draft.meta.mode) {
    case ProxyMode.File:
      validateFilePolicy(errors, asFilePolicy(draft.policy), lists.passHeaders);
      break;
    case ProxyMode.Oci:
      validateOciPolicy(errors, asOciPolicy(draft.policy), listenKind);
      break;
    case ProxyMode.Npm:
      validateNpmPolicy(errors, asNpmPolicy(draft.policy));
      break;
    case ProxyMode.Go:
      validateGoPolicy(errors, asGoPolicy(draft.policy), lists.goPrivatePatterns);
      break;
    case ProxyMode.Maven:
      validateMavenPolicy(errors, asMavenPolicy(draft.policy));
      break;
    case ProxyMode.Cargo:
      validateCargoPolicy(errors, asCargoPolicy(draft.policy));
      break;
    case ProxyMode.PyPI:
      validatePyPIPolicy(errors, asPyPIPolicy(draft.policy));
      break;
  }
}

function validateFilePolicy(errors: string[], policy: FilePolicy | undefined, passHeaders: string[]): void {
  if (!policy) return;
  if (policy.freshFor?.trim() && !isValidDuration(policy.freshFor.trim())) errors.push('文件模式快速命中时间格式无效。');
  for (const header of passHeaders.map((item) => item.trim()).filter(Boolean)) {
    if (/[\s\r\n:]/.test(header)) errors.push(`请求头 ${header} 格式无效。`);
    if (BLOCKED_PASS_HEADERS.has(header.toLowerCase())) errors.push(`请求头 ${header} 不能透传。`);
  }
  for (const [index, rule] of policy.rules.entries()) {
    if (!rule.match.trim()) errors.push(`文件规则 #${index + 1} 的匹配模式不能为空。`);
    if (rule.freshFor?.trim() && !isValidDuration(rule.freshFor.trim())) errors.push(`文件规则 #${index + 1} 的快速命中时间格式无效。`);
    if (rule.expireAfter?.trim() && !isValidDuration(rule.expireAfter.trim())) errors.push(`文件规则 #${index + 1} 的缓存保留时间格式无效。`);
  }
}

function validateOciPolicy(errors: string[], policy: OciPolicy | undefined, listenKind: ListenKind): void {
  if (!policy) return;
  if (listenKind !== ListenKind.Bind) errors.push('镜像代理必须使用独立端口。');
  if (policy.freshFor?.trim() && !isValidDuration(policy.freshFor.trim())) errors.push('镜像模式快速命中时间格式无效。');
  for (const [index, rule] of policy.rules.entries()) {
    if (!rule.match.trim()) errors.push(`仓库规则 #${index + 1} 的匹配模式不能为空。`);
  }
  const auth = policy.auth;
  if (auth?.type === OciAuthType.Basic && (!auth.username?.trim() || !auth.password?.trim())) errors.push('基础认证需要用户名和密码。');
  if (auth?.type === OciAuthType.Bearer && !auth.token?.trim()) errors.push('Bearer 认证需要令牌。');
}

function validateNpmPolicy(errors: string[], policy?: NpmPolicy): void {
  if (!policy) return;
  if (policy.freshFor?.trim() && !isValidDuration(policy.freshFor.trim())) errors.push('npm 模式快速命中时间格式无效。');
  for (const [index, rule] of policy.rules.entries()) {
    if (!rule.match.trim()) errors.push(`包规则 #${index + 1} 的匹配模式不能为空。`);
  }
}

function validateGoPolicy(errors: string[], policy: GoPolicy | undefined, goPrivatePatterns: string[]): void {
  if (!policy) return;
  const sumdb = policy.sumdb;
  if (sumdb?.enabled !== false) {
    if (sumdb?.name?.includes('\n') || sumdb?.name?.includes('\r') || sumdb?.name?.includes(' ')) errors.push('Go SumDB 名称不能包含空白字符。');
    if (sumdb?.url?.includes('\n') || sumdb?.url?.includes('\r')) errors.push('Go SumDB 上游地址不能包含换行。');
    if (sumdb?.url?.trim()) {
      try {
        const url = new URL(sumdb.url.trim());
        if (url.protocol !== 'http:' && url.protocol !== 'https:') errors.push('Go SumDB 上游必须是 HTTP/HTTPS。');
      } catch {
        errors.push('Go SumDB 上游地址格式需要调整。');
      }
    }
  }
  for (const pattern of goPrivatePatterns.map((item) => item.trim()).filter(Boolean)) {
    if (pattern.includes('\n') || pattern.includes('\r')) errors.push(`GOPRIVATE 规则 ${pattern} 不能包含换行。`);
  }
}

function validateMavenPolicy(errors: string[], policy?: MavenPolicy): void {
  if (!policy) return;
  if (policy.metadataFreshFor?.trim() && !isValidDuration(policy.metadataFreshFor.trim())) errors.push('Maven 元数据快速命中时间格式无效。');
  if (policy.snapshotFreshFor?.trim() && !isValidDuration(policy.snapshotFreshFor.trim())) errors.push('Maven SNAPSHOT 快速命中时间格式无效。');
  for (const [index, rule] of policy.rules.entries()) {
    if (!rule.match.trim()) errors.push(`Maven 规则 #${index + 1} 的匹配模式不能为空。`);
  }
}

function validateCargoPolicy(errors: string[], policy?: CargoPolicy): void {
  if (!policy) return;
  if (policy.indexFreshFor?.trim() && !isValidDuration(policy.indexFreshFor.trim())) errors.push('Cargo 索引快速命中时间格式无效。');
}

function validatePyPIPolicy(errors: string[], policy?: PyPIPolicy): void {
  if (!policy) return;
  if (policy.simpleFreshFor?.trim() && !isValidDuration(policy.simpleFreshFor.trim())) errors.push('PyPI Simple API 快速命中时间格式无效。');
}

function isValidDuration(value: string): boolean {
  return /^(\d+(ns|us|ms|s|m|h))+$/.test(value) && !value.startsWith('-');
}

function asFilePolicy(policy: ModePolicy): FilePolicy | undefined { return policy as FilePolicy | undefined; }
function asOciPolicy(policy: ModePolicy): OciPolicy | undefined { return policy as OciPolicy | undefined; }
function asNpmPolicy(policy: ModePolicy): NpmPolicy | undefined { return policy as NpmPolicy | undefined; }
function asGoPolicy(policy: ModePolicy): GoPolicy | undefined { return policy as GoPolicy | undefined; }
function asMavenPolicy(policy: ModePolicy): MavenPolicy | undefined { return policy as MavenPolicy | undefined; }
function asCargoPolicy(policy: ModePolicy): CargoPolicy | undefined { return policy as CargoPolicy | undefined; }
function asPyPIPolicy(policy: ModePolicy): PyPIPolicy | undefined { return policy as PyPIPolicy | undefined; }
