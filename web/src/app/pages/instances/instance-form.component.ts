import { Component, OnInit, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router, ActivatedRoute } from '@angular/router';
import { forkJoin, Observable, of } from 'rxjs';
import { ApiService } from '../../core/api.service';
import {
  BusyPolicy,
  CachePolicy,
  FilePolicy,
  FileRule,
  GoPolicy,
  InstanceCollectionResponse,
  InstanceDocumentResponse,
  InstanceSpec,
  ListenKind,
  ModePolicy,
  NpmPolicy,
  NpmResourcePolicy,
  NpmRule,
  OciAuthConfig,
  OciAuthType,
  OciPolicy,
  OciRule,
  ProxyMode,
  RuntimeInfo
} from '../../core/api.models';
import { BUSY_POLICY_OPTIONS, CACHE_POLICY_OPTIONS, LISTEN_KIND_OPTIONS, NPM_RESOURCE_POLICY_OPTIONS, OCI_AUTH_OPTIONS } from '../../core/config-options';
import { ToastService } from '../../shared/toast.service';
import { ModalService } from '../../shared/modal.service';
import { CanComponentDeactivate } from '../../core/form-deactivate.guard';
import { ModeLabelPipe } from '../../shared/mode-label.pipe';

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
  loading = true;
  saving = false;
  errors: string[] = [];

  generation = 0;
  collection?: InstanceCollectionResponse;
  runtime?: RuntimeInfo;
  draft?: InstanceSpec;
  listenKind = ListenKind.Path;
  upstreams: string[] = [];
  passHeaders: string[] = [];
  proxiedSumDBs: string[] = [];
  private savedFormState = '';

  readonly cachePolicies = CACHE_POLICY_OPTIONS;
  readonly busyPolicies = BUSY_POLICY_OPTIONS;
  readonly listenKinds = LISTEN_KIND_OPTIONS;
  readonly ociAuthOptions = OCI_AUTH_OPTIONS;
  readonly npmResourcePolicies = NPM_RESOURCE_POLICY_OPTIONS;
  readonly ProxyMode = ProxyMode;
  readonly ListenKind = ListenKind;
  readonly OciAuthType = OciAuthType;

  ngOnInit(): void { this.load(); }

  get dirty(): boolean { return this.formState() !== this.savedFormState; }
  get filePolicy(): FilePolicy | undefined { return this.draft?.meta.mode === ProxyMode.File ? this.draft.policy as FilePolicy : undefined; }
  get ociPolicy(): OciPolicy | undefined { return this.draft?.meta.mode === ProxyMode.Oci ? this.draft.policy as OciPolicy : undefined; }
  get npmPolicy(): NpmPolicy | undefined { return this.draft?.meta.mode === ProxyMode.Npm ? this.draft.policy as NpmPolicy : undefined; }
  get goPolicy(): GoPolicy | undefined { return this.draft?.meta.mode === ProxyMode.Go ? this.draft.policy as GoPolicy : undefined; }

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
  addProxiedSumDB(): void { this.proxiedSumDBs.push(''); }
  removeProxiedSumDB(index: number): void { this.proxiedSumDBs.splice(index, 1); }
  addFileRule(): void { this.filePolicy?.rules.push({ match: '**/*', policy: CachePolicy.Revalidate, freshFor: '', expireAfter: '' }); }
  removeFileRule(index: number): void { this.filePolicy?.rules.splice(index, 1); }
  addOciRule(): void { this.ociPolicy?.rules.push({ match: 'library/*', policy: CachePolicy.Immutable, freshFor: '', expireAfter: '' }); }
  removeOciRule(index: number): void { this.ociPolicy?.rules.splice(index, 1); }
  addNpmRule(): void { this.npmPolicy?.rules.push({ match: '**', resourcePolicy: NpmResourcePolicy.All, policy: CachePolicy.Immutable, freshFor: '', expireAfter: '' }); }
  removeNpmRule(index: number): void { this.npmPolicy?.rules.splice(index, 1); }

  back(): void {
    if (!this.dirty) {
      this.router.navigate(['/instances']);
      return;
    }
    this.confirmDeactivate().subscribe((ok) => {
      if (ok) this.router.navigate(['/instances']);
    });
  }

  save(): void {
    if (!this.draft) return;
    this.errors = this.validateForm();
    if (this.errors.length > 0) return;
    const spec = this.normalize();
    this.saving = true;
    const request = this.isCreate
      ? this.api.createInstance(this.generation, spec)
      : this.api.updateInstance(this.generation, spec.name, spec);
    request.subscribe({
      next: (response) => {
        this.saving = false;
        this.generation = response.generation;
        this.draft = structuredClone(response.spec);
        this.loadDraftState(response.spec);
        this.markSaved();
        this.toast.success(`${response.spec.name} 已保存。`);
        if (this.isCreate) {
          this.isCreate = false;
          this.router.navigate(['/instances', response.spec.name], { replaceUrl: true });
        }
      },
      error: (err) => {
        this.saving = false;
        this.toast.error(err.error?.error || '保存操作异常');
      }
    });
  }

  private load(): void {
    this.loading = true;
    const name = this.route.snapshot.paramMap.get('name');
    const copyName = this.route.snapshot.queryParamMap.get('copy');
    const mode = (this.route.snapshot.queryParamMap.get('mode') as ProxyMode) || ProxyMode.File;
    const draftRequest = name
      ? this.api.instance(name)
      : copyName
        ? this.api.instance(copyName)
        : of(undefined);
    forkJoin({
      runtime: this.api.runtime(),
      collection: this.api.instancesCollection(),
      draftDoc: draftRequest
    }).subscribe({
      next: ({ runtime, collection, draftDoc }) => {
        this.runtime = runtime;
        this.collection = collection;
        this.generation = collection.generation;
        if (name && draftDoc) {
          this.isCreate = false;
          this.draft = structuredClone((draftDoc as InstanceDocumentResponse).spec);
          this.loadDraftState(this.draft);
        } else if (copyName && draftDoc) {
          this.isCreate = true;
          this.draft = structuredClone((draftDoc as InstanceDocumentResponse).spec);
          this.draft.name = this.suggestCopyName(this.draft.name);
          this.loadDraftState(this.draft);
        } else {
          this.isCreate = true;
          this.draft = this.defaultDraft(mode);
          this.loadDraftState(this.draft);
        }
        this.markSaved();
        this.loading = false;
      },
      error: (err) => {
        this.loading = false;
        this.toast.error(err.error?.error || '实例配置加载异常');
      }
    });
  }

  private suggestCopyName(base: string): string {
    const existing = new Set((this.collection?.items ?? []).map((item) => item.name));
    let next = `${base}-copy`;
    let counter = 2;
    while (existing.has(next)) {
      next = `${base}-copy-${counter}`;
      counter++;
    }
    return next;
  }

  private loadDraftState(spec: InstanceSpec): void {
    this.listenKind = spec.route.bind ? ListenKind.Bind : ListenKind.Path;
    this.upstreams = [...(spec.source.upstreams ?? [])];
    this.passHeaders = [...(this.asFilePolicy(spec.policy)?.passHeaders ?? [])];
    this.proxiedSumDBs = [...(this.asGoPolicy(spec.policy)?.proxiedSumDBs ?? [])];
    this.normalizeDraftDefaults();
  }

  private normalizeDraftDefaults(): void {
    if (!this.draft) return;
    this.draft.meta.expireAfter = this.draft.meta.expireAfter ?? '720h';
    this.draft.source.transport = this.draft.source.transport ?? {};
    switch (this.draft.meta.mode) {
      case ProxyMode.File:
        this.draft.policy = {
          defaultPolicy: this.filePolicy?.defaultPolicy ?? CachePolicy.Bypass,
          freshFor: this.filePolicy?.freshFor ?? '30s',
          busyPolicy: this.filePolicy?.busyPolicy ?? BusyPolicy.Bypass,
          passHeaders: this.filePolicy?.passHeaders ?? [],
          rules: this.filePolicy?.rules ?? []
        } as FilePolicy;
        break;
      case ProxyMode.Oci:
        this.draft.policy = {
          defaultPolicy: this.ociPolicy?.defaultPolicy ?? CachePolicy.Revalidate,
          freshFor: this.ociPolicy?.freshFor ?? '30s',
          busyPolicy: this.ociPolicy?.busyPolicy ?? BusyPolicy.Bypass,
          auth: this.ociPolicy?.auth ?? { type: OciAuthType.None },
          rules: this.ociPolicy?.rules ?? []
        } as OciPolicy;
        break;
      case ProxyMode.Npm:
        this.draft.policy = {
          defaultPolicy: this.npmPolicy?.defaultPolicy ?? CachePolicy.Revalidate,
          freshFor: this.npmPolicy?.freshFor ?? '30s',
          busyPolicy: this.npmPolicy?.busyPolicy ?? BusyPolicy.Bypass,
          rules: this.npmPolicy?.rules ?? []
        } as NpmPolicy;
        break;
      case ProxyMode.Go: {
        const goPolicy: GoPolicy = this.goPolicy ?? { sumdb: 'sum.golang.org', proxiedSumDBs: ['sum.golang.org'], disableModuleFetchHeader: true };
        goPolicy.sumdb = goPolicy.sumdb ?? 'sum.golang.org';
        goPolicy.proxiedSumDBs = goPolicy.proxiedSumDBs ?? [];
        this.draft.policy = goPolicy;
        break;
      }
    }
  }

  private defaultDraft(mode: ProxyMode): InstanceSpec {
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
        policy: { sumdb: 'sum.golang.org', proxiedSumDBs: ['sum.golang.org'], disableModuleFetchHeader: true } as GoPolicy
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

  private normalize(): InstanceSpec {
    const spec = structuredClone(this.draft!);
    spec.name = spec.name.trim();
    spec.meta.description = spec.meta.description?.trim() || undefined;
    spec.meta.expireAfter = this.normalizeDuration(spec.meta.expireAfter);
    spec.route = this.listenKind === ListenKind.Bind
      ? { bind: spec.route.bind?.trim() || undefined }
      : { path: spec.route.path?.trim() || undefined };
    spec.source.upstreams = this.upstreams.map((value) => value.trim()).filter(Boolean);
    spec.source.transport = this.normalizeTransport(spec.source.transport);
    if (!spec.source.transport) delete spec.source.transport;
    switch (spec.meta.mode) {
      case ProxyMode.File:
        spec.policy = this.normalizeFilePolicy(this.asFilePolicy(spec.policy));
        break;
      case ProxyMode.Oci:
        spec.policy = this.normalizeOciPolicy(this.asOciPolicy(spec.policy));
        break;
      case ProxyMode.Npm:
        spec.policy = this.normalizeNpmPolicy(this.asNpmPolicy(spec.policy));
        break;
      case ProxyMode.Go:
        spec.policy = this.normalizeGoPolicy(this.asGoPolicy(spec.policy));
        break;
    }
    return spec;
  }

  private normalizeTransport(transport?: InstanceSpec['source']['transport']) {
    if (!transport) return undefined;
    const next = {
      proxy: transport.proxy?.trim() || undefined,
      ua: transport.ua?.trim() || undefined,
      timeout: transport.timeout?.trim() || undefined
    };
    return next.proxy || next.ua || next.timeout ? next : undefined;
  }

  private normalizeDuration(value?: string): string | undefined {
    const trimmed = value?.trim();
    return trimmed || undefined;
  }

  private normalizeFilePolicy(policy?: FilePolicy): FilePolicy {
    const next: FilePolicy = {
      ...policy,
      defaultPolicy: policy?.defaultPolicy ?? CachePolicy.Bypass,
      freshFor: this.normalizeDuration(policy?.freshFor),
      busyPolicy: policy?.busyPolicy ?? BusyPolicy.Bypass,
      passHeaders: this.passHeaders.map((header) => header.trim()).filter(Boolean),
      rules: (policy?.rules ?? []).map((rule) => this.normalizeFileRule(rule))
    };
    if ((next.passHeaders?.length ?? 0) === 0) delete next.passHeaders;
    return next;
  }

  private normalizeOciPolicy(policy?: OciPolicy): OciPolicy {
    const auth = policy?.auth;
    return {
      ...policy,
      defaultPolicy: policy?.defaultPolicy ?? CachePolicy.Revalidate,
      freshFor: this.normalizeDuration(policy?.freshFor),
      busyPolicy: policy?.busyPolicy ?? BusyPolicy.Bypass,
      auth: !auth || auth.type === OciAuthType.None ? undefined : {
        type: auth.type,
        username: auth.username?.trim() || undefined,
        password: auth.password?.trim() || undefined,
        token: auth.token?.trim() || undefined
      },
      rules: (policy?.rules ?? []).map((rule) => this.normalizeOciRule(rule))
    };
  }

  private normalizeNpmPolicy(policy?: NpmPolicy): NpmPolicy {
    return {
      ...policy,
      defaultPolicy: policy?.defaultPolicy ?? CachePolicy.Revalidate,
      freshFor: this.normalizeDuration(policy?.freshFor),
      busyPolicy: policy?.busyPolicy ?? BusyPolicy.Bypass,
      rules: (policy?.rules ?? []).map((rule) => this.normalizeNpmRule(rule))
    };
  }

  private normalizeGoPolicy(policy?: GoPolicy): GoPolicy {
    const next: GoPolicy = {
      ...policy,
      sumdb: policy?.sumdb?.trim() || undefined,
      noSumDB: policy?.noSumDB?.trim() || undefined,
      proxiedSumDBs: this.proxiedSumDBs.map((value) => value.trim()).filter(Boolean),
      disableModuleFetchHeader: Boolean(policy?.disableModuleFetchHeader)
    };
    if ((next.proxiedSumDBs?.length ?? 0) === 0) delete next.proxiedSumDBs;
    return next;
  }

  private normalizeFileRule(rule: FileRule): FileRule {
    return {
      ...rule,
      match: rule.match.trim(),
      freshFor: this.normalizeDuration(rule.freshFor),
      expireAfter: this.normalizeDuration(rule.expireAfter)
    };
  }

  private normalizeOciRule(rule: OciRule): OciRule {
    return {
      ...rule,
      match: rule.match.trim(),
      freshFor: this.normalizeDuration(rule.freshFor),
      expireAfter: this.normalizeDuration(rule.expireAfter)
    };
  }

  private normalizeNpmRule(rule: NpmRule): NpmRule {
    return {
      ...rule,
      match: rule.match.trim(),
      freshFor: this.normalizeDuration(rule.freshFor),
      expireAfter: this.normalizeDuration(rule.expireAfter)
    };
  }

  private formState(): string {
    return JSON.stringify({
      draft: this.draft,
      listenKind: this.listenKind,
      upstreams: this.upstreams,
      passHeaders: this.passHeaders,
      proxiedSumDBs: this.proxiedSumDBs
    });
  }

  private markSaved(): void {
    this.savedFormState = this.formState();
  }

  private validateForm(): string[] {
    if (!this.draft) return ['配置未加载完成。'];
    const errors: string[] = [];
    const name = this.draft.name.trim();
    if (!name) errors.push('实例名称不能为空。');
    if (name.includes('/') || name.includes('\\') || name === '.' || name === '..') errors.push('实例名称不能包含路径字符。');
    if (this.isCreate && this.collection?.items.some((item) => item.name === name)) errors.push(`实例名称 ${name} 已存在。`);

    const routeValue = this.listenKind === ListenKind.Bind ? this.draft.route.bind?.trim() : this.draft.route.path?.trim();
    if (!routeValue) errors.push(this.listenKind === ListenKind.Bind ? '监听地址不能为空。' : '路径前缀不能为空。');
    if (this.listenKind === ListenKind.Path && routeValue && !routeValue.startsWith('/')) errors.push('路径前缀必须以 / 开头。');
    if (this.listenKind === ListenKind.Path && routeValue && /\s/.test(routeValue)) errors.push('路径前缀不能包含空格。');
    if (this.listenKind === ListenKind.Path && routeValue && routeValue.includes('//')) errors.push('路径前缀不能包含连续 /。');
    if (this.listenKind === ListenKind.Bind && routeValue && !/^[^:]+:\d+$/.test(routeValue)) errors.push('监听地址需要使用 host:port 格式。');

    const owners = this.collection?.items.filter((item) => item.name !== this.draft!.name) ?? [];
    if (this.listenKind === ListenKind.Path && routeValue) {
      const normalized = '/' + routeValue.replace(/^\/+|\/+$/g, '');
      const owner = owners.find((item) => item.path === normalized);
      if (owner) errors.push(`路径前缀 ${normalized} 已被 ${owner.name} 使用。`);
    }
    if (this.listenKind === ListenKind.Bind && routeValue) {
      const owner = owners.find((item) => item.bind === routeValue);
      if (owner) errors.push(`监听地址 ${routeValue} 已被 ${owner.name} 使用。`);
    }

    const upstreams = this.upstreams.map((value) => value.trim()).filter(Boolean);
    if (upstreams.length === 0) errors.push('至少需要一个上游地址。');
    if ((this.draft.meta.mode === ProxyMode.Oci || this.draft.meta.mode === ProxyMode.Npm) && upstreams.length !== 1) errors.push('当前模式需要且只能配置一个上游地址。');
    for (const upstream of upstreams) {
      try {
        const url = new URL(upstream);
        if (url.protocol !== 'http:' && url.protocol !== 'https:') errors.push(`上游 ${upstream} 必须是 HTTP/HTTPS。`);
      } catch {
        errors.push(`上游 ${upstream} 地址格式需要调整。`);
      }
    }

    const transport = this.draft.source.transport;
    if (transport?.proxy?.trim()) {
      try {
        const url = new URL(transport.proxy.trim());
        if (url.protocol !== 'http:' && url.protocol !== 'https:' && url.protocol !== 'socks5:') errors.push('上游代理请选择 http、https 或 socks5。');
      } catch {
        errors.push('上游代理地址格式需要调整。');
      }
    }
    if (this.draft.meta.expireAfter?.trim() && !this.isValidDuration(this.draft.meta.expireAfter.trim())) errors.push('缓存保留时间格式无效。');
    if (transport?.timeout?.trim() && !this.isValidDuration(transport.timeout.trim())) errors.push('连接超时格式无效。');

    if (this.draft.meta.mode === ProxyMode.File && this.filePolicy) {
      if (this.filePolicy.freshFor?.trim() && !this.isValidDuration(this.filePolicy.freshFor.trim())) errors.push('文件模式快速命中时间格式无效。');
      for (const header of this.passHeaders.map((item) => item.trim()).filter(Boolean)) {
        if (/[\s\r\n:]/.test(header)) errors.push(`请求头 ${header} 格式无效。`);
        if (BLOCKED_PASS_HEADERS.has(header.toLowerCase())) errors.push(`请求头 ${header} 不能透传。`);
      }
      for (const [index, rule] of this.filePolicy.rules.entries()) {
        if (!rule.match.trim()) errors.push(`文件规则 #${index + 1} 的匹配模式不能为空。`);
        if (rule.freshFor?.trim() && !this.isValidDuration(rule.freshFor.trim())) errors.push(`文件规则 #${index + 1} 的快速命中时间格式无效。`);
        if (rule.expireAfter?.trim() && !this.isValidDuration(rule.expireAfter.trim())) errors.push(`文件规则 #${index + 1} 的缓存保留时间格式无效。`);
      }
    }

    if (this.draft.meta.mode === ProxyMode.Oci && this.ociPolicy) {
      if (this.listenKind !== ListenKind.Bind) errors.push('镜像代理必须使用独立端口。');
      if (this.ociPolicy.freshFor?.trim() && !this.isValidDuration(this.ociPolicy.freshFor.trim())) errors.push('镜像模式快速命中时间格式无效。');
      for (const [index, rule] of this.ociPolicy.rules.entries()) {
        if (!rule.match.trim()) errors.push(`仓库规则 #${index + 1} 的匹配模式不能为空。`);
      }
      const auth = this.ociPolicy.auth;
      if (auth?.type === OciAuthType.Basic && (!auth.username?.trim() || !auth.password?.trim())) errors.push('基础认证需要用户名和密码。');
      if (auth?.type === OciAuthType.Bearer && !auth.token?.trim()) errors.push('Bearer 认证需要令牌。');
    }

    if (this.draft.meta.mode === ProxyMode.Npm && this.npmPolicy) {
      if (this.npmPolicy.freshFor?.trim() && !this.isValidDuration(this.npmPolicy.freshFor.trim())) errors.push('npm 模式快速命中时间格式无效。');
      for (const [index, rule] of this.npmPolicy.rules.entries()) {
        if (!rule.match.trim()) errors.push(`包规则 #${index + 1} 的匹配模式不能为空。`);
      }
    }

    if (this.draft.meta.mode === ProxyMode.Go && this.goPolicy) {
      if (this.goPolicy.sumdb?.includes('\n') || this.goPolicy.sumdb?.includes('\r')) errors.push('Go SumDB 不能包含换行。');
      for (const value of this.proxiedSumDBs.map((item) => item.trim()).filter(Boolean)) {
        if (value.includes('\n') || value.includes('\r')) errors.push(`Go SumDB 代理项 ${value} 不能包含换行。`);
      }
    }
    return errors;
  }

  private isValidDuration(value: string): boolean {
    return /^(\d+(ns|us|ms|s|m|h))+$/.test(value) && !value.startsWith('-');
  }

  private asFilePolicy(policy: ModePolicy): FilePolicy | undefined {
    return policy as FilePolicy | undefined;
  }

  private asOciPolicy(policy: ModePolicy): OciPolicy | undefined {
    return policy as OciPolicy | undefined;
  }

  private asNpmPolicy(policy: ModePolicy): NpmPolicy | undefined {
    return policy as NpmPolicy | undefined;
  }

  private asGoPolicy(policy: ModePolicy): GoPolicy | undefined {
    return policy as GoPolicy | undefined;
  }
}
