import { Component, OnInit, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router, ActivatedRoute } from '@angular/router';
import { forkJoin, Observable, of } from 'rxjs';
import { ApiService } from '../../core/api.service';
import {
  BusyPolicy,
  CachePolicy,
  FilePolicy,
  GoPolicy,
  MavenPolicy,
  CargoPolicy,
  PyPIPolicy,
  InstanceCollectionResponse,
  InstanceDocumentResponse,
  InstanceSpec,
  ListenKind,
  NpmPolicy,
  NpmResourcePolicy,
  OciAuthType,
  OciPolicy,
  ProxyMode,
  RuntimeInfo
} from '../../core/api.models';
import { BUSY_POLICY_OPTIONS, CACHE_POLICY_OPTIONS, LISTEN_KIND_OPTIONS, NPM_RESOURCE_POLICY_OPTIONS, OCI_AUTH_OPTIONS, SelectOption } from '../../core/config-options';
import { ToastService } from '../../shared/toast.service';
import { ModalService } from '../../shared/modal.service';
import { CanComponentDeactivate } from '../../core/form-deactivate.guard';
import { ModeLabelPipe } from '../../shared/mode-label.pipe';
import { applyDraftDefaults, buildDefaultDraft, extractDraftLists, formState, normalizeSpec, validateSpec } from './instance-form.utils';

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
  goPrivatePatterns: string[] = [];
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
  get mavenPolicy(): MavenPolicy | undefined { return this.draft?.meta.mode === ProxyMode.Maven ? this.draft.policy as MavenPolicy : undefined; }
  get cargoPolicy(): CargoPolicy | undefined { return this.draft?.meta.mode === ProxyMode.Cargo ? this.draft.policy as CargoPolicy : undefined; }
  get pypiPolicy(): PyPIPolicy | undefined { return this.draft?.meta.mode === ProxyMode.PyPI ? this.draft.policy as PyPIPolicy : undefined; }

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
  addGoPrivatePattern(): void { this.goPrivatePatterns.push(''); }
  removeGoPrivatePattern(index: number): void { this.goPrivatePatterns.splice(index, 1); }
  addFileRule(): void { this.filePolicy?.rules.push({ match: '**/*', policy: CachePolicy.Revalidate, freshFor: '', expireAfter: '' }); }
  removeFileRule(index: number): void { this.filePolicy?.rules.splice(index, 1); }
  addOciRule(): void { this.ociPolicy?.rules.push({ match: 'library/*', policy: CachePolicy.Immutable, freshFor: '', expireAfter: '' }); }
  removeOciRule(index: number): void { this.ociPolicy?.rules.splice(index, 1); }
  addNpmRule(): void { this.npmPolicy?.rules.push({ match: '**', resourcePolicy: NpmResourcePolicy.All, policy: CachePolicy.Immutable, freshFor: '', expireAfter: '' }); }
  removeNpmRule(index: number): void { this.npmPolicy?.rules.splice(index, 1); }
  addMavenRule(): void { this.mavenPolicy?.rules.push({ match: '**/maven-metadata.xml', policy: CachePolicy.Revalidate, freshFor: '', expireAfter: '' }); }
  removeMavenRule(index: number): void { this.mavenPolicy?.rules.splice(index, 1); }

  optionDescription<T extends string>(options: Array<SelectOption<T>>, value: T | undefined): string {
    return options.find((option) => option.value === value)?.description ?? '';
  }

  listenKindDescription(): string {
    return this.optionDescription(this.listenKinds, this.listenKind);
  }

  cachePolicyDescription(value: CachePolicy | undefined): string {
    return this.optionDescription(this.cachePolicies, value);
  }

  busyPolicyDescription(value: BusyPolicy | undefined): string {
    return this.optionDescription(this.busyPolicies, value);
  }

  ociAuthDescription(value: OciAuthType | undefined): string {
    return this.optionDescription(this.ociAuthOptions, value);
  }

  npmResourceDescription(value: NpmResourcePolicy | undefined): string {
    return this.optionDescription(this.npmResourcePolicies, value);
  }

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
    this.listenKind = spec.meta.mode === ProxyMode.Oci || spec.route.bind ? ListenKind.Bind : ListenKind.Path;
    const lists = extractDraftLists(spec);
    this.upstreams = lists.upstreams;
    this.passHeaders = lists.passHeaders;
    this.goPrivatePatterns = lists.goPrivatePatterns;
    this.draft = applyDraftDefaults(spec);
  }

  private defaultDraft(mode: ProxyMode): InstanceSpec {
    return buildDefaultDraft(mode);
  }

  private normalize(): InstanceSpec {
    return normalizeSpec(this.draft!, this.listenKind, {
      upstreams: this.upstreams,
      passHeaders: this.passHeaders,
      goPrivatePatterns: this.goPrivatePatterns
    });
  }

  private formState(): string {
    return formState(this.draft, this.listenKind, {
      upstreams: this.upstreams,
      passHeaders: this.passHeaders,
      goPrivatePatterns: this.goPrivatePatterns
    });
  }

  private markSaved(): void {
    this.savedFormState = this.formState();
  }

  private validateForm(): string[] {
    return validateSpec(this.draft, this.isCreate, this.listenKind, {
      upstreams: this.upstreams,
      passHeaders: this.passHeaders,
      goPrivatePatterns: this.goPrivatePatterns
    }, this.collection?.items ?? []);
  }
}
