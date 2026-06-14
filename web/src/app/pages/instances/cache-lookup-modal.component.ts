import { Component, Input, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ApiService } from '../../core/api.service';
import { CacheLookupResult, ProxyMode } from '../../core/api.models';
import { ModeLabelPipe } from '../../shared/mode-label.pipe';
import { ToastService } from '../../shared/toast.service';

@Component({
  selector: 'app-cache-lookup-modal',
  imports: [FormsModule, ModeLabelPipe],
  template: `
    <div class="modal-header">
      <div>
        <h5 class="modal-title mb-1">缓存查询</h5>
        <p class="text-muted small mb-0">按当前实例的协议路径查询缓存状态。</p>
      </div>
      <button type="button" class="btn-close" (click)="activeModal.dismiss()"></button>
    </div>

    <div class="modal-body">
      <div class="row g-3">
        <div class="col-12">
          <label class="form-label">实例名称</label>
          <input class="form-control" [value]="instanceName" disabled>
        </div>
        <div class="col-12">
          <label class="form-label">{{ lookupLabel() }}</label>
          <p class="form-text mb-2">{{ lookupHint() }}</p>
          <input
            class="form-control"
            name="lookupPath"
            [(ngModel)]="lookupPath"
            [placeholder]="lookupPlaceholder()"
            (keyup.enter)="lookupCache()"
          >
        </div>
      </div>

      <div class="d-flex justify-content-between align-items-center mt-3 gap-3 flex-wrap">
        <p class="text-muted small mb-0">输入应与客户端实际请求路径一致，不需要再补实例入口前缀。</p>
        <button type="button" class="btn btn-primary" (click)="lookupCache()" [disabled]="!lookupPath.trim() || lookupRunning">
          @if (lookupRunning) {
            <span class="spinner-border spinner-border-sm me-2" aria-hidden="true"></span>
            查询中...
          } @else {
            查询缓存状态
          }
        </button>
      </div>

      @if (lookupResult) {
        <div class="lookup-result mt-4" [class.cached]="lookupResult.cached" [class.fresh]="lookupResult.fresh">
          <div class="d-flex align-items-center gap-3 mb-3">
            <strong>{{ lookupResult.cached ? '已缓存' : '未缓存' }}</strong>
            @if (lookupResult.cached) {
              <span
                class="badge"
                [class.bg-success-subtle]="lookupResult.fresh"
                [class.text-success-emphasis]="lookupResult.fresh"
                [class.bg-warning-subtle]="!lookupResult.fresh"
                [class.text-warning-emphasis]="!lookupResult.fresh"
              >
                {{ lookupResult.fresh ? '有效' : '可刷新' }}
              </span>
            }
          </div>
          <dl class="kv-list">
            <div><dt>缓存策略</dt><dd>{{ lookupResult.policy | modeLabel:'policy' }}</dd></div>
            <div><dt>快速命中</dt><dd>{{ lookupResult.freshFor || '未设置' }}</dd></div>
            <div><dt>缓存保留</dt><dd>{{ lookupResult.expireAfter || '未设置' }}</dd></div>
            @if (lookupResult.cached) {
              <div><dt>缓存时间</dt><dd>{{ lookupResult.cachedAt }}</dd></div>
              <div><dt>过期时间</dt><dd>{{ lookupResult.expiresAt }}</dd></div>
            }
          </dl>
        </div>
      }
    </div>

    <div class="modal-footer">
      <button type="button" class="btn btn-outline-secondary" (click)="activeModal.dismiss()">关闭</button>
    </div>
  `
})
export class CacheLookupModalComponent {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);

  readonly activeModal = inject(NgbActiveModal);

  @Input({ required: true }) instanceName = '';
  @Input({ required: true }) mode: ProxyMode = ProxyMode.File;

  lookupPath = '';
  lookupRunning = false;
  lookupResult?: CacheLookupResult;

  lookupLabel(): string {
    switch (this.mode) {
      case ProxyMode.Npm: return '包名';
      case ProxyMode.Oci: return '镜像名称';
      case ProxyMode.Go: return 'Go 模块缓存路径';
      case ProxyMode.Maven: return 'Maven 仓库路径';
      case ProxyMode.Cargo: return 'Cargo registry 路径';
      case ProxyMode.PyPI: return 'PyPI Simple 或文件路径';
      default: return '文件路径';
    }
  }

  lookupHint(): string {
    switch (this.mode) {
      case ProxyMode.Npm: return '输入 npm 包名，例如 @angular/core 或 lodash。';
      case ProxyMode.Oci: return '输入镜像名称，例如 library/alpine:latest 或 nginx:1.25。';
      case ProxyMode.Go: return '输入 GOPROXY 路径，例如 golang.org/x/mod/@v/list。';
      case ProxyMode.Maven: return '输入 Maven 仓库路径，例如 com/google/guava/guava/maven-metadata.xml。';
      case ProxyMode.Cargo: return '输入 sparse registry 路径，例如 config.json 或 se/rd/serde。';
      case ProxyMode.PyPI: return '输入 Simple API 或文件路径，例如 simple/requests/。';
      default: return '输入客户端请求的文件路径，例如 nginx.conf 或 assets/logo.png。';
    }
  }

  lookupPlaceholder(): string {
    switch (this.mode) {
      case ProxyMode.Npm: return '@angular/core';
      case ProxyMode.Oci: return 'library/alpine:latest';
      case ProxyMode.Go: return 'golang.org/x/mod/@v/list';
      case ProxyMode.Maven: return 'com/google/guava/guava/maven-metadata.xml';
      case ProxyMode.Cargo: return 'config.json';
      case ProxyMode.PyPI: return 'simple/requests/';
      default: return 'nginx.conf';
    }
  }

  lookupCache(): void {
    if (!this.instanceName || !this.lookupPath.trim()) return;
    this.lookupRunning = true;
    this.lookupResult = undefined;
    this.api.cacheLookup(this.instanceName, this.lookupPath.trim()).subscribe({
      next: (result) => {
        this.lookupResult = result;
        this.lookupRunning = false;
      },
      error: (err) => {
        this.lookupRunning = false;
        this.toast.error(err.error?.error || '缓存查询异常');
      }
    });
  }
}
