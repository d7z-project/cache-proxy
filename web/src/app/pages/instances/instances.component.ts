import { Component, OnInit, inject } from '@angular/core';
import { NgFor, NgIf } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { forkJoin } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { BusyPolicy, CachePolicy, ConfigSnapshot, InstanceConfig, ListenKind, OciAuthType, ProxyMode, RuntimeInfo } from '../../core/api.models';
import { BUSY_POLICY_OPTIONS, CACHE_POLICY_OPTIONS, FILE_DEFAULT_RULES, LISTEN_KIND_OPTIONS, OCI_AUTH_OPTIONS, PROXY_MODE_OPTIONS } from '../../core/config-options';

@Component({
  selector: 'app-instances',
  imports: [NgFor, NgIf, FormsModule],
  templateUrl: './instances.component.html'
})
export class InstancesComponent implements OnInit {
  private readonly api = inject(ApiService);

  snapshot?: ConfigSnapshot;
  runtime?: RuntimeInfo;
  selectedName = '';
  draftName = '';
  draft?: InstanceConfig;
  listenKind = ListenKind.Path;
  upstreamText = '';
  passHeaderText = '';
  importText = '';
  importReplace = false;
  message = '';
  errors: string[] = [];
  loading = true;

  readonly proxyModes = PROXY_MODE_OPTIONS;
  readonly cachePolicies = CACHE_POLICY_OPTIONS;
  readonly busyPolicies = BUSY_POLICY_OPTIONS;
  readonly listenKinds = LISTEN_KIND_OPTIONS;
  readonly ociAuthOptions = OCI_AUTH_OPTIONS;
  readonly ProxyMode = ProxyMode;
  readonly ListenKind = ListenKind;
  readonly OciAuthType = OciAuthType;

  ngOnInit(): void {
    this.load();
  }

  get creating(): boolean {
    return this.selectedName === '' && this.draft !== undefined;
  }

  get instanceEntries(): { name: string; config: InstanceConfig }[] {
    const instances = this.snapshot?.config.instances ?? {};
    return Object.keys(instances).sort().map((name) => ({ name, config: instances[name] }));
  }

  get availableListenKinds() {
    if (this.draft?.mode === ProxyMode.Oci) {
      return this.listenKinds.filter((kind) => kind.value === ListenKind.Bind);
    }
    return this.listenKinds;
  }

  modeLabel(mode: ProxyMode | string): string {
    return this.proxyModes.find((option) => option.value === mode)?.label ?? mode;
  }

  load(): void {
    this.loading = true;
    forkJoin({ snapshot: this.api.config(), runtime: this.api.runtime() }).subscribe({
      next: ({ snapshot, runtime }) => {
        this.snapshot = snapshot;
        this.runtime = runtime;
        this.loading = false;
        if (this.selectedName && snapshot.config.instances[this.selectedName]) {
          this.select(this.selectedName);
        } else if (this.selectedName) {
          this.selectedName = '';
          this.draftName = '';
          this.draft = undefined;
        }
      },
      error: (err) => this.fail(err, '配置加载异常')
    });
  }

  select(name: string): void {
    const instance = this.snapshot?.config.instances[name];
    if (!instance) return;
    this.selectedName = name;
    this.draftName = name;
    this.draft = structuredClone(instance);
    this.normalizeDraftDefaults();
    this.draft.transport = this.draft.transport ?? {};
    this.listenKind = this.draft.listen.bind ? ListenKind.Bind : ListenKind.Path;
    this.upstreamText = this.draft.upstreams.join('\n');
    this.passHeaderText = (this.draft.passHeaders ?? []).join('\n');
    this.message = '';
    this.errors = [];
  }

  private normalizeDraftDefaults(): void {
    if (!this.draft) return;
    this.draft.listen = this.draft.listen ?? {};
    this.draft.upstreams = this.draft.upstreams ?? [];
    this.draft.cache = this.draft.cache ?? { rules: [] };
    this.draft.cache.rules = this.draft.cache.rules ?? [];
    this.draft.cache.busyPolicy = this.draft.cache.busyPolicy ?? BusyPolicy.Bypass;
    if (this.draft.mode === ProxyMode.File) {
      this.draft.cache.defaultPolicy = this.draft.cache.defaultPolicy ?? CachePolicy.Bypass;
      return;
    }
    this.draft.cache.rules = [];
    delete this.draft.cache.defaultPolicy;
    if (this.draft.mode === ProxyMode.Oci) {
      this.draft.oci = this.draft.oci ?? { blobPolicy: CachePolicy.Immutable, manifestPolicy: CachePolicy.Revalidate, tagPolicy: CachePolicy.Revalidate };
      this.draft.oci.blobPolicy = this.draft.oci.blobPolicy ?? CachePolicy.Immutable;
      this.draft.oci.manifestPolicy = this.draft.oci.manifestPolicy ?? CachePolicy.Revalidate;
      this.draft.oci.tagPolicy = this.draft.oci.tagPolicy ?? CachePolicy.Revalidate;
      this.draft.oci.auth = this.draft.oci.auth ?? { type: OciAuthType.None };
    }
    if (this.draft.mode === ProxyMode.Npm) {
      this.draft.npm = this.draft.npm ?? { metadataPolicy: CachePolicy.Revalidate, tarballPolicy: CachePolicy.Immutable };
      this.draft.npm.metadataPolicy = this.draft.npm.metadataPolicy ?? CachePolicy.Revalidate;
      this.draft.npm.tarballPolicy = this.draft.npm.tarballPolicy ?? CachePolicy.Immutable;
    }
  }

  newInstance(mode: ProxyMode): void {
    this.selectedName = '';
    this.draftName = mode === ProxyMode.Oci ? 'dockerhub' : mode === ProxyMode.Npm ? 'npmjs' : 'files';
    this.listenKind = mode === ProxyMode.Oci ? ListenKind.Bind : ListenKind.Path;
    this.upstreamText = mode === ProxyMode.Oci ? 'https://registry-1.docker.io' : mode === ProxyMode.Npm ? 'https://registry.npmjs.org' : 'https://example.com';
    this.passHeaderText = mode === ProxyMode.File ? 'Accept\nAccept-Language' : '';
    this.draft = this.defaultInstance(mode);
    this.message = '';
    this.errors = [];
  }

  changeMode(mode: ProxyMode): void {
    if (!this.draft || !this.creating) return;
    this.draft = this.defaultInstance(mode);
    this.upstreamText = this.draft.upstreams.join('\n');
    this.passHeaderText = mode === ProxyMode.File ? (this.draft.passHeaders ?? []).join('\n') : '';
    this.listenKind = mode === ProxyMode.Oci ? ListenKind.Bind : ListenKind.Path;
  }

  addRule(): void {
    this.draft?.cache.rules.push({ match: '**/*', policy: CachePolicy.Revalidate });
  }

  removeRule(index: number): void {
    this.draft?.cache.rules.splice(index, 1);
  }

  save(): void {
    if (!this.snapshot || !this.draft) return;
    this.errors = this.validateDraft();
    if (this.errors.length > 0) {
      this.message = '请检查标出的配置项。';
      return;
    }
    const name = this.draftName.trim();
    if (!name) {
      this.message = '实例名称不能为空。';
      return;
    }
    const instance = this.normalizedDraft();
    if (!instance) return;
    const next = structuredClone(this.snapshot.config);
    if (this.selectedName && this.selectedName !== name) delete next.instances[this.selectedName];
    next.instances[name] = instance;
    this.api.saveConfig(this.snapshot.generation, next).subscribe({
      next: (snapshot) => {
        this.snapshot = snapshot;
        this.selectedName = name;
        this.message = '实例配置已保存。';
        this.select(name);
      },
      error: (err) => this.fail(err, '保存操作异常')
    });
  }

  deleteSelected(): void {
    if (!this.snapshot || !this.selectedName) return;
    const next = structuredClone(this.snapshot.config);
    delete next.instances[this.selectedName];
    this.api.saveConfig(this.snapshot.generation, next).subscribe({
      next: (snapshot) => {
        this.snapshot = snapshot;
        this.selectedName = '';
        this.draft = undefined;
        this.message = '实例已删除。';
      },
      error: (err) => this.fail(err, '删除操作异常')
    });
  }

  exportSelected(): void {
    if (!this.selectedName) return;
    this.exportInstances(this.selectedName);
  }

  exportAll(): void {
    this.exportInstances();
  }

  importInstances(): void {
    if (!this.snapshot) return;
    this.errors = [];
    this.message = '';
    let payload: { instances?: Record<string, unknown> };
    try {
      payload = JSON.parse(this.importText);
    } catch {
      this.message = '导入内容格式需要调整。';
      return;
    }
    const instances = payload.instances ?? payload;
    if (!instances || typeof instances !== 'object' || Array.isArray(instances)) {
      this.message = '导入内容需要包含实例配置。';
      return;
    }
    this.api.importInstances(this.snapshot.generation, instances, this.importReplace).subscribe({
      next: (snapshot) => {
        this.snapshot = snapshot;
        this.selectedName = '';
        this.draft = undefined;
        this.importText = '';
        this.message = '实例已导入。';
        this.load();
      },
      error: (err) => this.fail(err, '导入操作异常')
    });
  }

  private exportInstances(name?: string): void {
    this.api.exportInstances(name).subscribe({
      next: (result) => {
        const text = JSON.stringify(result, null, 2);
        const blob = new Blob([text], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = name ? `cache-proxy-${name}.json` : 'cache-proxy-instances.json';
        document.body.appendChild(link);
        link.click();
        link.remove();
        setTimeout(() => URL.revokeObjectURL(url), 0);
        this.message = '实例配置已导出。';
      },
      error: (err) => this.fail(err, '导出操作异常')
    });
  }

  private defaultInstance(mode: ProxyMode): InstanceConfig {
    if (mode === ProxyMode.Oci) {
      return {
        mode,
        listen: { bind: '127.0.0.1:5000' },
        upstreams: ['https://registry-1.docker.io'],
        expireAfter: '720h',
        cache: { freshFor: '30s', busyPolicy: BusyPolicy.Bypass, rules: [] },
        oci: { blobPolicy: CachePolicy.Immutable, manifestPolicy: CachePolicy.Revalidate, tagPolicy: CachePolicy.Revalidate, auth: { type: OciAuthType.None } },
        transport: {}
      };
    }
    if (mode === ProxyMode.Npm) {
      return {
        mode,
        listen: { path: '/npm' },
        upstreams: ['https://registry.npmjs.org'],
        expireAfter: '720h',
        cache: { freshFor: '30s', busyPolicy: BusyPolicy.Bypass, rules: [] },
        npm: { metadataPolicy: CachePolicy.Revalidate, tarballPolicy: CachePolicy.Immutable },
        transport: {}
      };
    }
    return {
      mode,
      listen: { path: '/files' },
      upstreams: ['https://example.com'],
      expireAfter: '720h',
      cache: { defaultPolicy: CachePolicy.Bypass, freshFor: '30s', busyPolicy: BusyPolicy.Bypass, rules: structuredClone(FILE_DEFAULT_RULES) },
      passHeaders: ['Accept', 'Accept-Language'],
      transport: {}
    };
  }

  private normalizedDraft(): InstanceConfig | undefined {
    if (!this.draft) return undefined;
    const instance = structuredClone(this.draft);
    if (instance.mode === ProxyMode.Oci) {
      this.listenKind = ListenKind.Bind;
    }
    instance.listen = this.listenKind === ListenKind.Bind ? { bind: instance.listen.bind?.trim() } : { path: instance.listen.path?.trim() };
    instance.upstreams = this.upstreamText.split('\n').map((line) => line.trim()).filter(Boolean);
    if ((instance.mode === ProxyMode.Oci || instance.mode === ProxyMode.Npm) && instance.upstreams.length > 1) {
      instance.upstreams = instance.upstreams.slice(0, 1);
    }
    instance.passHeaders = this.passHeaderText.split('\n').map((line) => line.trim()).filter(Boolean);
    if (instance.passHeaders.length === 0) delete instance.passHeaders;
    instance.expireAfter = instance.expireAfter?.trim() || undefined;
    instance.cache.freshFor = instance.cache.freshFor?.trim() || undefined;
    instance.cache.busyPolicy = instance.cache.busyPolicy || BusyPolicy.Bypass;
    instance.transport = this.normalizeTransport(instance);
    if (instance.mode === ProxyMode.Oci) {
      instance.cache = { freshFor: instance.cache.freshFor, busyPolicy: instance.cache.busyPolicy, rules: [] };
      instance.oci = this.normalizeOci(instance);
      delete instance.passHeaders;
      delete instance.npm;
    } else if (instance.mode === ProxyMode.Npm) {
      instance.cache = { freshFor: instance.cache.freshFor, busyPolicy: instance.cache.busyPolicy, rules: [] };
      instance.npm = instance.npm ?? { metadataPolicy: CachePolicy.Revalidate, tarballPolicy: CachePolicy.Immutable };
      delete instance.oci;
      delete instance.passHeaders;
    } else {
      delete instance.oci;
      delete instance.npm;
      instance.cache.defaultPolicy = instance.cache.defaultPolicy || CachePolicy.Bypass;
    }
    return instance;
  }

  private validateDraft(): string[] {
    const errors: string[] = [];
    if (!this.snapshot || !this.draft) return ['配置未加载完成。'];
    const name = this.draftName.trim();
    if (!name) errors.push('实例名称不能为空。');
    if (name.includes('/') || name.includes('\\') || name === '.' || name === '..') errors.push('实例名称不能包含路径字符。');
    if (!this.selectedName && name && this.snapshot.config.instances[name]) errors.push(`实例名称 ${name} 已存在。`);
    if (this.selectedName && this.selectedName !== name && this.snapshot.config.instances[name]) errors.push(`实例名称 ${name} 已存在。`);
    if (this.draft.mode === ProxyMode.Oci && this.listenKind !== ListenKind.Bind) errors.push('镜像代理请选择独立端口。');
    const listenValue = this.listenKind === ListenKind.Bind ? this.draft.listen.bind?.trim() : this.draft.listen.path?.trim();
    if (!listenValue) errors.push(this.listenKind === ListenKind.Bind ? '监听地址不能为空。' : '路径前缀不能为空。');
    if (this.listenKind === ListenKind.Path && listenValue && !listenValue.startsWith('/')) errors.push('路径前缀必须以 / 开头。');
    if (!Object.values(BusyPolicy).includes(this.draft.cache.busyPolicy ?? BusyPolicy.Bypass)) errors.push('并发策略需要重新选择。');
    const upstreams = this.upstreamText.split('\n').map((line) => line.trim()).filter(Boolean);
    if (upstreams.length === 0) errors.push('至少需要一个上游地址。');
    if (this.draft.mode === ProxyMode.Oci && upstreams.length !== 1) errors.push('镜像代理需要一个上游地址。');
    if (this.draft.mode === ProxyMode.Npm && upstreams.length !== 1) errors.push('npm 代理需要一个上游地址。');
    for (const upstream of upstreams) {
      try {
        const url = new URL(upstream);
        if (url.protocol !== 'http:' && url.protocol !== 'https:') errors.push(`上游 ${upstream} 必须是 HTTP/HTTPS。`);
      } catch {
        errors.push(`上游 ${upstream} 地址格式需要调整。`);
      }
    }
    const proxy = this.draft.transport?.proxy?.trim();
    if (proxy) {
      try {
        const url = new URL(proxy);
        if (url.protocol !== 'http:' && url.protocol !== 'https:' && url.protocol !== 'socks5:') {
          errors.push('上游代理请选择 http、https 或 socks5。');
        }
        if (!url.hostname) errors.push('上游连接代理缺少 host。');
      } catch {
        errors.push('上游代理地址格式需要调整。');
      }
    }
    const bindOwners = new Map<string, string>();
    if (this.runtime?.adminBind) bindOwners.set(this.runtime.adminBind, '管理入口');
    if (this.runtime?.proxyBind) bindOwners.set(this.runtime.proxyBind, '代理入口');
    if (this.runtime?.metricsBind) bindOwners.set(this.runtime.metricsBind, '监控入口');
    const pathOwners = new Map<string, string>();
    for (const [instanceName, instance] of Object.entries(this.snapshot.config.instances)) {
      if (instanceName === this.selectedName) continue;
      if (instance.listen.bind) bindOwners.set(instance.listen.bind, instanceName);
      if (instance.listen.path) pathOwners.set(instance.listen.path, instanceName);
    }
    if (this.listenKind === ListenKind.Bind && listenValue) {
      const owner = bindOwners.get(listenValue);
      if (owner) errors.push(`监听地址 ${listenValue} 已被 ${owner} 使用。`);
    }
    if (this.listenKind === ListenKind.Path && listenValue) {
      const owner = pathOwners.get(listenValue);
      if (owner) errors.push(`路径前缀 ${listenValue} 已被 ${owner} 使用。`);
    }
    if (this.draft.mode === ProxyMode.Oci && this.draft.oci?.auth?.type === OciAuthType.Basic && !this.draft.oci.auth.username?.trim()) {
      errors.push('请填写镜像仓库用户名。');
    }
    if (this.draft.mode === ProxyMode.Oci && this.draft.oci?.auth?.type === OciAuthType.Bearer && !this.draft.oci.auth.token?.trim()) {
      errors.push('请填写镜像仓库访问令牌。');
    }
    return errors;
  }

  private normalizeOci(instance: InstanceConfig) {
    const oci = instance.oci ?? { blobPolicy: CachePolicy.Immutable, manifestPolicy: CachePolicy.Revalidate, tagPolicy: CachePolicy.Revalidate };
    const auth = oci.auth;
    if (!auth || auth.type === OciAuthType.None) {
      delete oci.auth;
      return oci;
    }
    auth.username = auth.username?.trim() || undefined;
    auth.password = auth.password || undefined;
    auth.token = auth.token?.trim() || undefined;
    if (auth.type === OciAuthType.Basic) delete auth.token;
    if (auth.type === OciAuthType.Bearer) {
      delete auth.username;
      delete auth.password;
    }
    return oci;
  }

  private normalizeTransport(instance: InstanceConfig) {
    const transport = { ...(instance.transport ?? {}) };
    transport.proxy = transport.proxy?.trim() || undefined;
    transport.ua = transport.ua?.trim() || undefined;
    transport.timeout = transport.timeout?.trim() || undefined;
    return transport.proxy || transport.ua || transport.timeout ? transport : undefined;
  }

  private fail(err: { error?: { error?: string } }, fallback: string): void {
    this.loading = false;
    this.message = err.error?.error || fallback;
  }
}
