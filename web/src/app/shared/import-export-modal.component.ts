import { Component, EventEmitter, Input, Output, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../core/api.service';
import { ToastService } from './toast.service';

@Component({
  selector: 'app-import-export-modal',
  imports: [FormsModule],
  template: `
    @if (show) {
      <div class="modal-backdrop" (click)="close()">
        <div class="modal-card card modal-card-wide" (click)="$event.stopPropagation()">
          <div class="ie-header">
            <div>
              <h3>导入导出</h3>
              <p class="hint">用于迁移实例配置。</p>
            </div>
            <button type="button" class="ie-close" (click)="close()">×</button>
          </div>

          <div class="ie-sections">
            <!-- 导出区域 -->
            <div class="ie-section ie-export">
              <div class="ie-section-header">
                <span class="ie-icon">↓</span>
                <div>
                  <h4>导出配置</h4>
                  <p class="hint">下载所有实例配置为 JSON 文件</p>
                </div>
              </div>
              <button type="button" class="ie-export-btn" (click)="exportInstances()">
                <span class="ie-download-icon">↓</span>
                下载 JSON 文件
              </button>
            </div>

            <div class="ie-divider"></div>

            <!-- 导入区域 -->
            <div class="ie-section ie-import">
              <div class="ie-section-header">
                <span class="ie-icon">↑</span>
                <div>
                  <h4>导入配置</h4>
                  <p class="hint">粘贴 JSON 格式的实例配置</p>
                </div>
              </div>
              <textarea
                class="ie-textarea"
                name="importText"
                [(ngModel)]="importText"
                placeholder="粘贴导出的 JSON 配置，支持 { &quot;instances&quot;: { ... } } 格式"
              ></textarea>
              <div class="ie-import-options">
                <label class="inline-check">
                  <input type="checkbox" name="importReplace" [(ngModel)]="importReplace">
                  <span>覆盖同名实例</span>
                </label>
              </div>
              <button
                type="button"
                class="ie-import-btn"
                [disabled]="!importText.trim() || saving"
                (click)="importInstances()"
              >
                @if (saving) {
                  <span class="ie-spinner"></span>
                  导入中...
                } @else {
                  <span class="ie-upload-icon">↑</span>
                  导入实例
                }
              </button>
            </div>
          </div>
        </div>
      </div>
    }
  `,
  styles: `
    .ie-header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      margin-bottom: var(--space-xl);
    }
    .ie-header h3 { margin: 0; }
    .ie-close {
      width: 32px;
      height: 32px;
      padding: 0;
      display: grid;
      place-items: center;
      border: none;
      background: transparent;
      font-size: 24px;
      color: var(--color-text-muted);
      cursor: pointer;
      border-radius: var(--radius-xs);
      transition: background .15s, color .15s;
    }
    .ie-close:hover { background: var(--color-bg-hover); color: var(--color-text); }

    .ie-sections { display: flex; flex-direction: column; gap: 0; }

    .ie-section {
      padding: var(--space-lg);
      border-radius: var(--radius-sm);
      background: var(--color-bg);
    }
    .ie-section-header {
      display: flex;
      align-items: center;
      gap: var(--space-md);
      margin-bottom: var(--space-lg);
    }
    .ie-section-header h4 { margin: 0; font-size: var(--text-base); }
    .ie-section-header .hint { margin-top: 2px; }

    .ie-icon {
      width: 36px;
      height: 36px;
      display: grid;
      place-items: center;
      border-radius: var(--radius-xs);
      font-size: 18px;
      font-weight: 700;
      flex-shrink: 0;
    }
    .ie-export .ie-icon { background: var(--color-primary-light); color: var(--color-primary); }
    .ie-import .ie-icon { background: var(--color-success-light); color: var(--color-success); }

    .ie-divider {
      height: 1px;
      background: var(--color-border);
      margin: var(--space-lg) 0;
    }

    .ie-export-btn, .ie-import-btn {
      width: 100%;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: var(--space-sm);
      padding: 10px 16px;
      font-weight: 600;
      border-radius: var(--radius-xs);
      cursor: pointer;
      transition: all .15s;
    }
    .ie-export-btn {
      background: var(--color-bg-card);
      border: 1px solid var(--color-border);
      color: var(--color-text);
    }
    .ie-export-btn:hover { border-color: var(--color-primary); color: var(--color-primary); }

    .ie-import-btn {
      background: var(--color-primary);
      border: 1px solid var(--color-primary);
      color: #fff;
    }
    .ie-import-btn:hover:not(:disabled) { background: var(--color-primary-hover); }
    .ie-import-btn:disabled { opacity: .5; cursor: not-allowed; }

    .ie-download-icon, .ie-upload-icon { font-size: 16px; }

    .ie-textarea {
      width: 100%;
      min-height: 120px;
      padding: var(--space-md);
      border: 1px solid var(--color-border);
      border-radius: var(--radius-xs);
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: var(--text-sm);
      line-height: 1.5;
      resize: vertical;
      background: var(--color-bg-card);
      margin-bottom: var(--space-md);
    }
    .ie-textarea:focus {
      outline: none;
      border-color: var(--color-primary);
      box-shadow: 0 0 0 3px rgba(37, 99, 235, .1);
    }
    .ie-textarea::placeholder { color: var(--color-text-disabled); }

    .ie-import-options {
      display: flex;
      align-items: center;
      gap: var(--space-lg);
      margin-bottom: var(--space-md);
    }

    .ie-spinner {
      width: 14px;
      height: 14px;
      border: 2px solid rgba(255,255,255,.3);
      border-top-color: #fff;
      border-radius: 50%;
      animation: spin .6s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg); } }
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
