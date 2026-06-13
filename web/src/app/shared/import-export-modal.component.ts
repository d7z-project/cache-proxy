import { Component, EventEmitter, Input, Output, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../core/api.service';
import { ToastService } from './toast.service';

@Component({
  selector: 'app-import-export-modal',
  imports: [FormsModule],
  template: `
    @if (show) {
      <div class="modal-backdrop fade show" (click)="close()"></div>
      <div class="modal d-block" tabindex="-1" (click)="close()">
        <div class="modal-dialog modal-dialog-centered modal-lg" (click)="$event.stopPropagation()">
          <div class="modal-content">
            <div class="modal-header">
              <h5 class="modal-title">导入导出</h5>
              <button type="button" class="btn-close" (click)="close()"></button>
            </div>
            <div class="modal-body">
              <div class="d-flex flex-column gap-3">
                <div class="p-3 rounded" style="background: #f1f5f9;">
                  <div class="d-flex align-items-center gap-3 mb-3">
                    <span class="d-inline-flex align-items-center justify-content-center rounded fw-bold" style="width:36px;height:36px;background:#eff6ff;color:#2563eb;font-size:18px;">↓</span>
                    <div><h6 class="mb-0">导出配置</h6><p class="text-muted small mb-0">下载所有实例配置为 JSON 文件</p></div>
                  </div>
                  <button type="button" class="btn btn-outline-primary w-100" (click)="exportInstances()">↓ 下载 JSON 文件</button>
                </div>

                <hr class="my-0">

                <div class="p-3 rounded" style="background: #f1f5f9;">
                  <div class="d-flex align-items-center gap-3 mb-3">
                    <span class="d-inline-flex align-items-center justify-content-center rounded fw-bold" style="width:36px;height:36px;background:#ecfdf5;color:#065f46;font-size:18px;">↑</span>
                    <div><h6 class="mb-0">导入配置</h6><p class="text-muted small mb-0">粘贴 JSON 格式的实例配置</p></div>
                  </div>
                  <textarea
                    class="form-control font-monospace"
                    style="min-height: 120px; resize: vertical;"
                    name="importText"
                    [(ngModel)]="importText"
                    placeholder="粘贴导出的 JSON 配置，支持 { &quot;instances&quot;: { ... } } 格式"
                  ></textarea>
                  <div class="form-check my-3">
                    <input class="form-check-input" type="checkbox" name="importReplace" [(ngModel)]="importReplace" id="importReplace">
                    <label class="form-check-label" for="importReplace">覆盖同名实例</label>
                  </div>
                  <button
                    type="button"
                    class="btn btn-primary w-100"
                    [disabled]="!importText.trim() || saving"
                    (click)="importInstances()"
                  >
                    @if (saving) {
                      <span class="spinner-border spinner-border-sm me-1"></span>
                      导入中...
                    } @else {
                      ↑ 导入实例
                    }
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    }
  `
})
export class ImportExportModalComponent {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);

  @Input() show = false;
  @Input() generation = 0;
  @Output() showChange = new EventEmitter<boolean>();
  @Output() imported = new EventEmitter<void>();

  importText = '';
  importReplace = false;
  saving = false;

  close(): void {
    this.show = false;
    this.showChange.emit(false);
  }

  exportInstances(): void {
    this.api.exportInstances().subscribe({
      next: (result) => {
        const text = JSON.stringify(result, null, 2);
        const blob = new Blob([text], { type: 'application/json' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = 'cache-proxy-instances.json';
        document.body.appendChild(a);
        a.click();
        a.remove();
        setTimeout(() => URL.revokeObjectURL(a.href), 0);
        this.toast.success('实例配置已导出。');
      },
      error: (err) => this.toast.error(err.error?.error || '导出操作异常')
    });
  }

  importInstances(): void {
    let payload: { instances?: Record<string, unknown> };
    try { payload = JSON.parse(this.importText); } catch {
      this.toast.error('导入内容格式需要调整。');
      return;
    }
    const instances = payload.instances ?? payload;
    if (!instances || typeof instances !== 'object' || Array.isArray(instances)) {
      this.toast.error('导入内容需要包含实例配置。');
      return;
    }
    const invalid: string[] = [];
    for (const [name, inst] of Object.entries(instances)) {
      const candidate = inst as Record<string, unknown> | null;
      if (!candidate || typeof candidate !== 'object' || !candidate['mode'] || !candidate['listen'] || !candidate['upstreams']) {
        invalid.push(name);
      }
    }
    if (invalid.length > 0) {
      this.toast.error('以下实例配置不完整：' + invalid.join(', '));
      return;
    }
    this.saving = true;
    this.api.importInstances(this.generation, instances, this.importReplace).subscribe({
      next: () => {
        this.saving = false;
        this.importText = '';
        this.close();
        this.toast.success('实例已导入。');
        this.imported.emit();
      },
      error: (err) => { this.saving = false; this.toast.error(err.error?.error || '导入操作异常'); }
    });
  }
}
