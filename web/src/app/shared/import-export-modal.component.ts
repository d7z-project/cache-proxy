import { Component, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ApiService } from '../core/api.service';
import { ExportBundle, InstanceSpec } from '../core/api.models';
import { ToastService } from './toast.service';

@Component({
  selector: 'app-import-export-modal',
  imports: [FormsModule],
  template: `
    <div class="modal-header">
      <div>
        <h5 class="modal-title mb-1">导入导出实例</h5>
        <p class="text-muted small mb-0">导出当前实例配置包，或导入已有实例资源。</p>
      </div>
      <button type="button" class="btn-close" (click)="activeModal.dismiss()"></button>
    </div>

    <div class="modal-body">
      <div class="row g-3">
        <div class="col-lg-5">
          <div class="border rounded-3 bg-body-tertiary p-3 h-100">
            <h6 class="fw-semibold mb-1">导出实例</h6>
            <p class="text-muted small mb-3">下载当前实例配置包，适合备份或迁移到另一套环境。</p>
            <button type="button" class="btn btn-outline-primary w-100" (click)="exportInstances()">下载 JSON 文件</button>
          </div>
        </div>

        <div class="col-lg-7">
          <div class="border rounded-3 bg-body-tertiary p-3 h-100">
            <h6 class="fw-semibold mb-1">导入实例</h6>
            <p class="text-muted small mb-3">支持导出包结构 <code>&#123; "instances": [...] &#125;</code>，也支持直接粘贴实例数组。</p>
            <textarea
              class="form-control font-monospace text-break"
              style="min-height: 120px;"
              name="importText"
              [(ngModel)]="importText"
              placeholder="粘贴实例 JSON 内容"
            ></textarea>
            <div class="form-check mt-3">
              <input class="form-check-input" type="checkbox" name="importReplace" [(ngModel)]="importReplace" id="importReplace">
              <label class="form-check-label" for="importReplace">覆盖同名实例</label>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div class="modal-footer">
      <button type="button" class="btn btn-outline-secondary" (click)="activeModal.dismiss()">关闭</button>
      <button
        type="button"
        class="btn btn-primary"
        [disabled]="!importText.trim() || saving"
        (click)="importInstances()"
      >
        @if (saving) {
          <span class="spinner-border spinner-border-sm me-2" aria-hidden="true"></span>
          导入中...
        } @else {
          导入实例
        }
      </button>
    </div>
  `
})
export class ImportExportModalComponent {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);

  readonly activeModal = inject(NgbActiveModal);

  generation = 0;
  importText = '';
  importReplace = false;
  saving = false;

  exportInstances(): void {
    this.api.exportInstances().subscribe({
      next: (bundle) => {
        const text = JSON.stringify(bundle, null, 2);
        const blob = new Blob([text], { type: 'application/json' });
        const link = document.createElement('a');
        link.href = URL.createObjectURL(blob);
        link.download = 'cache-proxy-instances.json';
        document.body.appendChild(link);
        link.click();
        link.remove();
        setTimeout(() => URL.revokeObjectURL(link.href), 0);
        this.toast.success('实例配置已导出。');
      },
      error: (err) => this.toast.error(err.error?.error || '导出操作异常')
    });
  }

  importInstances(): void {
    let payload: ExportBundle | { instances?: InstanceSpec[] } | InstanceSpec[];
    try {
      payload = JSON.parse(this.importText);
    } catch {
      this.toast.error('导入内容格式需要调整。');
      return;
    }

    const instances = Array.isArray(payload) ? payload : payload.instances;
    if (!Array.isArray(instances) || instances.length === 0) {
      this.toast.error('导入内容需要包含实例数组。');
      return;
    }

    const invalid = instances.filter((item) => !item?.name || !item?.meta?.mode || !item?.route || !item?.source || !item?.policy);
    if (invalid.length > 0) {
      this.toast.error('存在不完整的实例配置。');
      return;
    }

    this.saving = true;
    this.api.importInstances(this.generation, instances, this.importReplace).subscribe({
      next: () => {
        this.saving = false;
        this.toast.success('实例已导入。');
        this.activeModal.close(true);
      },
      error: (err) => {
        this.saving = false;
        this.toast.error(err.error?.error || '导入操作异常');
      }
    });
  }
}
