import { Component, OnInit, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router, ActivatedRoute } from '@angular/router';
import { forkJoin, Observable } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { AppConfig, BusyPolicy, CachePolicy, ConfigSnapshot, InstanceConfig, ListenKind, NpmResourcePolicy, OciAuthType, ProxyMode, RuntimeInfo } from '../../core/api.models';
import { BUSY_POLICY_OPTIONS, CACHE_POLICY_OPTIONS, OCI_AUTH_OPTIONS, LISTEN_KIND_OPTIONS, NPM_RESOURCE_POLICY_OPTIONS } from '../../core/config-options';
import { ToastService } from '../../shared/toast.service';
import { ModalService } from '../../shared/modal.service';
import { CanComponentDeactivate } from '../../core/form-deactivate.guard';
import { ModeLabelPipe } from '../../shared/mode-label.pipe';

const CLEAR_SENTINEL = '-';
const BLOCKED_PASS_HEADERS = new Set([
  'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization', 'te', 'trailer',
  'transfer-encoding', 'upgrade', 'host', 'authorization', 'x-forwarded-for',
  'x-forwarded-host', 'x-forwarded-proto', 'x-forwarded-prefix', 'x-real-ip'
]);

@Component({
  selector: 'app-instance-form',
  imports: [FormsModule, ModeLabelPipe],
  templateUrl: './instance-form.component.html'
})
export class InstanceFormComponent implements OnInit, CanComponentDeactivate {
  private readonly api = inject(ApiService);
  private readonly router = inject(Router);
  private readonly route = inject(ActivatedRoute);
  private readonly toast = inject(ToastService);
  private readonly modal = inject(ModalService);

  isCreate = true;
  snapshot?: ConfigSnapshot;
  runtime?: RuntimeInfo;
  errors: string[] = [];
  loading = true;
  saving = false;

  draftName = '';
  draft?: InstanceConfig;
  private savedFormState = '';
  listenKind = ListenKind.Path;
  upstreams: string[] = [];
  passHeaders: string[] = [];

  readonly cachePolicies = CACHE_POLICY_OPTIONS;
  readonly busyPolicies = BUSY_POLICY_OPTIONS;
  readonly listenKinds = LISTEN_KIND_OPTIONS;
  readonly ociAuthOptions = OCI_AUTH_OPTIONS;
  readonly npmResourcePolicies = NPM_RESOURCE_POLICY_OPTIONS;
  readonly ProxyMode = ProxyMode;
  readonly ListenKind = ListenKind;
  readonly OciAuthType = OciAuthType;

  ngOnInit(): void { this.load(); }

  get dirty(): boolean {
    return this.formState() !== this.savedFormState;
  }

  isDirty(): boolean { return this.dirty; }

  confirmDeactivate(): Observable<boolean> {
    return this.modal.confirm({
      title: '放弃更改',
      message: '有未保存的更改，确定放弃？',
      confirmLabel: '放弃',
      danger: true
    });
  }

  addUpstream(): void { this.upstreams.push(''); }
  removeUpstream(index: number): void { if (this.upstreams.length > 1) this.upstreams.splice(index, 1); }
  addPassHeader(): void { this.passHeaders.push(''); }
  removePassHeader(index: number): void { this.passHeaders.splice(index, 1); }

  addRule(): void {
    this.draft?.cache.rules.push({ match: '**/*', policy: CachePolicy.Revalidate, freshFor: '', expireAfter: '' });
  }
  removeRule(index: number): void { this.draft?.cache.rules.splice(index, 1); }

  addOciRule(): void {
    this.draft?.oci?.rules.push({ match: 'library/*', policy: CachePolicy.Immutable, freshFor: '', expireAfter: '' });
  }
  removeOciRule(index: number): void { this.draft?.oci?.rules.splice(index, 1); }

  addNpmRule(): void {
    this.draft?.npm?.rules.push({ match: '**', resourcePolicy: NpmResourcePolicy.All, policy: CachePolicy.Immutable, freshFor: '', expireAfter: '' });
  }
  removeNpmRule(index: number): void { this.draft?.npm?.rules.splice(index, 1); }

  back(): void {
    if (!this.dirty) { this.router.navigate(['/instances']); return; }
    this.modal.confirm({
      title: '放弃更改',
      message: '有未保存的更改，确定放弃？',
      confirmLabel: '放弃',
      danger: true
    }).subscribe(ok => { if (ok) this.router.navigate(['/instances']); });
  }

  save(): void {
    if (!this.snapshot || !this.draft) return;
    this.errors = this.validateForm();
    if (this.errors.length > 0) return;

    const name = this.draftName.trim();
    const instance = this.normalize();
    const next = structuredClone(this.snapshot.config);
    if (!this.isCreate) {
      const oldName = this.route.snapshot.paramMap.get('name')!;
      if (oldName !== name) delete next.instances[oldName];
    }
    next.instances[name] = instance;

    this.saving = true;
    this.api.validateConfig(next).subscribe({
      next: () => this.doSave(name, instance, next),
      error: (err) => { this.saving = false; this.toast.error(err.error?.error || '配置校验失败，请检查表单。'); }
    });
  }

  private doSave(name: string, instance: InstanceConfig, next: AppConfig): void {
    const mode = this.draft!.mode;
    if ((mode === ProxyMode.Oci || mode === ProxyMode.Npm) && this.upstreams.filter(u => u.trim()).length > 1) {
      this.toast.success('已保存。注意：' + (mode === ProxyMode.Oci ? '镜像' : 'npm') + '代理仅使用第一个上游地址。');
    }
    this.api.saveConfig(this.snapshot!.generation, next).subscribe({
      next: (snapshot) => {
        this.saving = false;
        this.snapshot = snapshot;
        this.draftName = name;
        this.draft = structuredClone(instance);
        this.listenKind = instance.listen.bind ? ListenKind.Bind : ListenKind.Path;
        this.upstreams = [...instance.upstreams];
        this.passHeaders = [...(instance.passHeaders ?? [])];
        this.markSaved();
        this.toast.success(name + ' 已保存。');
        if (this.isCreate) {
          this.isCreate = false;
          this.router.navigate(['/instances', name], { replaceUrl: true });
        }
      },
      error: (err) => {
        this.saving = false;
        if (err.status === 409) {
          this.modal.confirm({
            title: '配置冲突',
            message: '配置已被其他人修改，是否重新加载并重试？',
            confirmLabel: '重新加载',
            danger: false
          }).subscribe(ok => {
            if (ok) this.load();
          });
        } else {
          this.toast.error(err.error?.error || '保存操作异常');
        }
      }
    });
  }

  private load(): void {
    this.loading = true;
    forkJoin({ snapshot: this.api.config(), runtime: this.api.runtime() }).subscribe({
      next: ({ snapshot, runtime }) => {
        this.snapshot = snapshot;
        this.runtime = runtime;
        const name = this.route.snapshot.paramMap.get('name');
        if (name) {
          this.initEdit(name);
        } else {
          const mode = (this.route.snapshot.queryParamMap.get('mode') as ProxyMode) || ProxyMode.File;
          const copyName = this.route.snapshot.queryParamMap.get('copy');
          this.initCreate(mode, copyName);
        }
        this.loading = false;
      },
      error: (err) => { this.loading = false; this.toast.error(err.error?.error || '配置加载异常'); }
    });
  }

  private initCreate(mode: ProxyMode, copyName: string | null): void {
    this.isCreate = true;
    if (copyName) {
      const source = this.snapshot!.config.instances[copyName];
      if (source) {
        let base = copyName + '-copy';
        let i = 1;
        while (this.snapshot!.config.instances[base]) { i++; base = copyName + '-copy-' + i; }
        this.loadDraft(base, source);
        this.draftName = base;
        return;
      }
    }
    this.draftName = mode === ProxyMode.Oci ? 'dockerhub' : mode === ProxyMode.Npm ? 'npmjs' : 'files';
    this.listenKind = mode === ProxyMode.Oci ? ListenKind.Bind : ListenKind.Path;
    this.draft = this.defaultDraft(mode);
    this.upstreams = mode === ProxyMode.Oci ? ['https://registry-1.docker.io']
      : mode === ProxyMode.Npm ? ['https://registry.npmjs.org']
      : ['https://example.com'];
    this.passHeaders = mode === ProxyMode.File ? ['Accept', 'Accept-Language'] : [];
    this.markSaved();
  }

  private initEdit(name: string): void {
    const instance = this.snapshot!.config.instances[name];
    if (!instance) { this.toast.error(`实例 ${name} 不存在。`); this.router.navigate(['/instances']); return; }
    this.isCreate = false;
    this.loadDraft(name, instance);
  }

  private loadDraft(name: string, instance: InstanceConfig): void {
    this.draftName = name;
    this.draft = structuredClone(instance);
    this.listenKind = this.draft.listen.bind ? ListenKind.Bind : ListenKind.Path;
    this.upstreams = [...(this.draft.upstreams ?? [])];
    this.passHeaders = [...(this.draft.passHeaders ?? [])];
    this.normalizeDraftDefaults();
    this.markSaved();
  }

  private formState(): string {
    return JSON.stringify({
      name: this.draftName,
      draft: this.draft,
      listenKind: this.listenKind,
      upstreams: this.upstreams,
      passHeaders: this.passHeaders
    });
  }

  private markSaved(): void {
    this.savedFormState = this.formState();
  }

  private normalizeDraftDefaults(): void {
    if (!this.draft) return;
    this.draft.listen = this.draft.listen ?? {};
    this.draft.transport = this.draft.transport ?? {};
    this.draft.cache = this.draft.cache ?? { rules: [] };
    this.draft.cache.rules = this.draft.cache.rules ?? [];
    this.draft.cache.busyPolicy = this.draft.cache.busyPolicy ?? BusyPolicy.Bypass;
    if (this.draft.mode === ProxyMode.File) {
      this.draft.cache.defaultPolicy = this.draft.cache.defaultPolicy ?? CachePolicy.Bypass;
      return;
    }
    if (this.draft.mode === ProxyMode.Oci) {
      this.draft.oci = this.draft.oci ?? { defaultPolicy: CachePolicy.Revalidate, rules: [] };
      this.draft.oci.defaultPolicy = this.draft.oci.defaultPolicy ?? CachePolicy.Revalidate;
      this.draft.oci.rules = this.draft.oci.rules ?? [];
      this.draft.oci.auth = this.draft.oci.auth ?? { type: OciAuthType.None };
    }
    if (this.draft.mode === ProxyMode.Npm) {
      this.draft.npm = this.draft.npm ?? { defaultPolicy: CachePolicy.Revalidate, rules: [] };
      this.draft.npm.defaultPolicy = this.draft.npm.defaultPolicy ?? CachePolicy.Revalidate;
      this.draft.npm.rules = this.draft.npm.rules ?? [];
    }
  }

  private defaultDraft(mode: ProxyMode): InstanceConfig {
    if (mode === ProxyMode.Oci) {
      return {
        mode, listen: { bind: '' }, upstreams: ['https://registry-1.docker.io'], expireAfter: '720h',
        cache: { freshFor: '30s', busyPolicy: BusyPolicy.Bypass, rules: [] },
        oci: { defaultPolicy: CachePolicy.Revalidate, rules: [], auth: { type: OciAuthType.None } },
        transport: {}
      };
    }
    if (mode === ProxyMode.Npm) {
      return {
        mode, listen: { path: '/npm' }, upstreams: ['https://registry.npmjs.org'], expireAfter: '720h',
        cache: { freshFor: '30s', busyPolicy: BusyPolicy.Bypass, rules: [] },
        npm: { defaultPolicy: CachePolicy.Revalidate, rules: [] }, transport: {}
      };
    }
    return {
      mode, listen: { path: '/files' }, upstreams: ['https://example.com'], expireAfter: '720h',
      cache: { defaultPolicy: CachePolicy.Bypass, freshFor: '30s', busyPolicy: BusyPolicy.Bypass, rules: [] },
      passHeaders: ['Accept', 'Accept-Language'], transport: {}
    };
  }

  private normalize(): InstanceConfig {
    if (!this.draft) throw new Error('no draft');
    const instance = structuredClone(this.draft);
    instance.listen = this.listenKind === ListenKind.Bind
      ? { bind: instance.listen.bind?.trim() }
      : { path: instance.listen.path?.trim() };
    instance.upstreams = this.upstreams.map((u) => u.trim()).filter(Boolean);
    if ((instance.mode === ProxyMode.Oci || instance.mode === ProxyMode.Npm) && instance.upstreams.length > 1) {
      instance.upstreams = instance.upstreams.slice(0, 1);
    }
    instance.passHeaders = this.passHeaders.map((h) => h.trim()).filter(Boolean);
    if (instance.passHeaders.length === 0) delete instance.passHeaders;
    instance.expireAfter = this.normalizeDuration(instance.expireAfter);
    instance.cache.freshFor = this.normalizeDuration(instance.cache.freshFor);
    instance.cache.rules = this.normalizeRules(instance.cache.rules);
    instance.cache.busyPolicy = instance.cache.busyPolicy || BusyPolicy.Bypass;
    instance.transport = this.normalizeTransport(instance.transport);
    if (instance.mode === ProxyMode.Oci) {
      instance.cache = { freshFor: instance.cache.freshFor, busyPolicy: instance.cache.busyPolicy, rules: [] };
      instance.oci = this.normalizeOci(instance.oci);
      delete instance.passHeaders;
      delete instance.npm;
    } else if (instance.mode === ProxyMode.Npm) {
      instance.cache = { freshFor: instance.cache.freshFor, busyPolicy: instance.cache.busyPolicy, rules: [] };
      instance.npm = this.normalizeNpm(instance.npm);
      delete instance.oci;
      delete instance.passHeaders;
    } else {
      delete instance.oci;
      delete instance.npm;
      instance.cache.defaultPolicy = instance.cache.defaultPolicy || CachePolicy.Bypass;
    }
    return instance;
  }

  private normalizeDuration(value: string | undefined): string | undefined {
    const trimmed = value?.trim();
    return trimmed || undefined;
  }

  private normalizeRules<T extends { match: string; freshFor?: string; expireAfter?: string }>(rules: T[] = []): T[] {
    return (rules ?? []).map(r => ({
      ...r,
      match: r.match?.trim(),
      freshFor: this.normalizeDuration(r.freshFor),
      expireAfter: this.normalizeDuration(r.expireAfter),
    }));
  }

  private normalizeOci(oci: InstanceConfig['oci']): NonNullable<InstanceConfig['oci']> {
    if (!oci) return { defaultPolicy: CachePolicy.Revalidate, rules: [] };
    const auth = oci.auth;
    if (!auth || auth.type === OciAuthType.None) { delete oci.auth; return oci; }
    auth.username = auth.username?.trim() || undefined;
    if (auth.password === '' || auth.password === CLEAR_SENTINEL) {
      auth.password = CLEAR_SENTINEL;
    }
    auth.token = auth.token?.trim() || undefined;
    if (auth.token === '' || auth.token === CLEAR_SENTINEL) {
      auth.token = CLEAR_SENTINEL;
    }
    if (auth.type === OciAuthType.Basic) delete auth.token;
    if (auth.type === OciAuthType.Bearer) { delete auth.username; delete auth.password; }
    oci.rules = this.normalizeRules(oci.rules);
    return oci;
  }

  private normalizeNpm(npm: InstanceConfig['npm']): NonNullable<InstanceConfig['npm']> {
    if (!npm) return { defaultPolicy: CachePolicy.Revalidate, rules: [] };
    npm.rules = this.normalizeRules(npm.rules);
    return npm;
  }

  private normalizeTransport(t: InstanceConfig['transport']) {
    if (!t) return undefined;
    t.proxy = t.proxy?.trim() || undefined;
    t.ua = t.ua?.trim() || undefined;
    t.timeout = t.timeout?.trim() || undefined;
    return t.proxy || t.ua || t.timeout ? t : undefined;
  }

  private validateForm(): string[] {
    const e: string[] = [];
    if (!this.snapshot || !this.draft) return ['配置未加载完成。'];
    const name = this.draftName.trim();
    if (!name) e.push('实例名称不能为空。');
    if (name.includes('/') || name.includes('\\') || name === '.' || name === '..') e.push('实例名称不能包含路径字符。');
    if (this.isCreate && this.snapshot.config.instances[name]) e.push(`实例名称 ${name} 已存在。`);
    if (!this.isCreate) {
      const oldName = this.route.snapshot.paramMap.get('name')!;
      if (oldName !== name && this.snapshot.config.instances[name]) e.push(`实例名称 ${name} 已存在。`);
    }
    const listenValue = this.listenKind === ListenKind.Bind ? this.draft.listen.bind?.trim() : this.draft.listen.path?.trim();
    if (!listenValue) e.push(this.listenKind === ListenKind.Bind ? '监听地址不能为空。' : '路径前缀不能为空。');
    if (this.listenKind === ListenKind.Path && listenValue && !listenValue.startsWith('/')) e.push('路径前缀必须以 / 开头。');
    if (this.listenKind === ListenKind.Path && listenValue && /\s/.test(listenValue)) e.push('路径前缀不能包含空格。');
    if (this.listenKind === ListenKind.Path && listenValue && listenValue.includes('//')) e.push('路径前缀不能包含连续 /。');
    if (this.listenKind === ListenKind.Path && listenValue && /[^a-zA-Z0-9/_\-]/.test(listenValue)) e.push('路径前缀只能包含字母、数字、/、_、-。');
    if (this.listenKind === ListenKind.Path && listenValue && `/${listenValue.replace(/^\/+|\/+$/g, '')}` === '/') e.push('路径前缀不能使用根路径 /。');
    if (!Object.values(BusyPolicy).includes(this.draft.cache.busyPolicy ?? BusyPolicy.Bypass)) e.push('并发策略需要重新选择。');
    const validUpstreams = this.upstreams.map((u) => u.trim()).filter(Boolean);
    if (validUpstreams.length === 0) e.push('至少需要一个上游地址。');
    if (this.draft.mode === ProxyMode.Oci && validUpstreams.length !== 1) e.push('镜像代理需要一个上游地址。');
    if (this.draft.mode === ProxyMode.Npm && validUpstreams.length !== 1) e.push('npm 代理需要一个上游地址。');
    for (const u of validUpstreams) {
      try { const url = new URL(u); if (url.protocol !== 'http:' && url.protocol !== 'https:') e.push(`上游 ${u} 必须是 HTTP/HTTPS。`); }
      catch { e.push(`上游 ${u} 地址格式需要调整。`); }
    }
    const proxy = this.draft.transport?.proxy?.trim();
    if (proxy) {
      try { const url = new URL(proxy); if (url.protocol !== 'http:' && url.protocol !== 'https:' && url.protocol !== 'socks5:') e.push('上游代理请选择 http、https 或 socks5。'); if (!url.hostname) e.push('上游连接代理缺少 host。'); }
      catch { e.push('上游代理地址格式需要调整。'); }
    }
    if (this.draft.mode === ProxyMode.Oci && this.listenKind !== ListenKind.Bind) e.push('镜像代理请选择独立端口。');
    if (this.draft.expireAfter?.trim() && !this.isValidDuration(this.draft.expireAfter.trim())) e.push('缓存保留时间格式无效，示例：720h、168h。');
    if (this.draft.cache.freshFor?.trim() && !this.isValidDuration(this.draft.cache.freshFor.trim())) e.push('快速命中时间格式无效，示例：30s、5m。');
    if (this.draft.transport?.timeout?.trim() && !this.isValidDuration(this.draft.transport.timeout.trim())) e.push('连接超时格式无效，示例：3s、30s。');
    if (this.draft.mode === ProxyMode.File) {
      for (const header of this.passHeaders.map((h) => h.trim()).filter(Boolean)) {
        const lower = header.toLowerCase();
        if (/[\s\r\n:]/.test(header)) e.push(`请求头 ${header} 格式无效。`);
        if (BLOCKED_PASS_HEADERS.has(lower)) e.push(`请求头 ${header} 不能透传。`);
      }
    }
    if (this.draft.mode === ProxyMode.File) {
      for (let i = 0; i < this.draft.cache.rules.length; i++) {
        const rule = this.draft.cache.rules[i];
        if (!rule.match?.trim()) e.push(`文件规则 #${i + 1} 的匹配模式不能为空。`);
        if (rule.freshFor?.trim() && !this.isValidDuration(rule.freshFor.trim())) e.push(`文件规则 #${i + 1} 的快速命中时间格式无效。`);
        if (rule.expireAfter?.trim() && !this.isValidDuration(rule.expireAfter.trim())) e.push(`文件规则 #${i + 1} 的缓存保留时间格式无效。`);
      }
    }
    if (this.draft.mode === ProxyMode.Oci && this.draft.oci) {
      for (let i = 0; i < this.draft.oci.rules.length; i++) {
        const rule = this.draft.oci.rules[i];
        if (!rule.match?.trim()) e.push(`仓库规则 #${i + 1} 的匹配模式不能为空。`);
        if (rule.freshFor?.trim() && !this.isValidDuration(rule.freshFor.trim())) e.push(`仓库规则 #${i + 1} 的快速命中时间格式无效。`);
        if (rule.expireAfter?.trim() && !this.isValidDuration(rule.expireAfter.trim())) e.push(`仓库规则 #${i + 1} 的缓存保留时间格式无效。`);
      }
    }
    if (this.draft.mode === ProxyMode.Npm && this.draft.npm) {
      for (let i = 0; i < this.draft.npm.rules.length; i++) {
        const rule = this.draft.npm.rules[i];
        if (!rule.match?.trim()) e.push(`包规则 #${i + 1} 的匹配模式不能为空。`);
        if (rule.freshFor?.trim() && !this.isValidDuration(rule.freshFor.trim())) e.push(`包规则 #${i + 1} 的快速命中时间格式无效。`);
        if (rule.expireAfter?.trim() && !this.isValidDuration(rule.expireAfter.trim())) e.push(`包规则 #${i + 1} 的缓存保留时间格式无效。`);
      }
    }

    const bindOwners = new Map<string, string>();
    if (this.runtime?.bind) bindOwners.set(this.runtime.bind, '主监听');
    const pathOwners = new Map<string, string>();
    const currentName = this.isCreate ? undefined : this.route.snapshot.paramMap.get('name')!;
    for (const [iname, inst] of Object.entries(this.snapshot.config.instances)) {
      if (iname === currentName) continue;
      if (inst.listen.bind) bindOwners.set(inst.listen.bind, iname);
      if (inst.listen.path) pathOwners.set('/' + inst.listen.path.replace(/^\/+|\/+$/g, ''), iname);
    }
    if (this.listenKind === ListenKind.Bind && listenValue) {
      const owner = bindOwners.get(listenValue);
      if (owner) e.push(`监听地址 ${listenValue} 已被 ${owner} 使用。`);
    }
    if (this.listenKind === ListenKind.Path && listenValue) {
      const normalizedPath = '/' + listenValue.replace(/^\/+|\/+$/g, '');
      const owner = pathOwners.get(normalizedPath);
      if (owner) e.push(`路径前缀 ${normalizedPath} 已被 ${owner} 使用。`);
    }
    if (this.draft.mode === ProxyMode.Oci && this.draft.oci?.auth?.type === OciAuthType.Basic && !this.draft.oci.auth.username?.trim()) e.push('请填写镜像仓库用户名。');
    if (this.draft.mode === ProxyMode.Oci && this.draft.oci?.auth?.type === OciAuthType.Basic && !this.draft.oci.auth.password?.trim()) e.push('请填写镜像仓库密码。');
    if (this.draft.mode === ProxyMode.Oci && this.draft.oci?.auth?.type === OciAuthType.Bearer && !this.draft.oci.auth.token?.trim()) e.push('请填写镜像仓库访问令牌。');
    return e;
  }

  private isValidDuration(value: string): boolean {
    return /^(\d+(ns|us|ms|s|m|h))+$/.test(value) && !value.startsWith('-');
  }
}
