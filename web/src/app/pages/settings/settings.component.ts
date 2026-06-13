import { Component, OnInit, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { forkJoin } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { GlobalConfig, GlobalConfigResponse, RuntimeInfo } from '../../core/api.models';
import { ToastService } from '../../shared/toast.service';
import { ModalService } from '../../shared/modal.service';

@Component({
  selector: 'app-settings',
  imports: [FormsModule],
  templateUrl: './settings.component.html'
})
export class SettingsComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);
  private readonly modal = inject(ModalService);

  runtime?: RuntimeInfo;
  global?: GlobalConfigResponse;
  draft?: GlobalConfig;
  loading = true;
  saving = false;

  ngOnInit(): void { this.load(); }

  load(): void {
    this.loading = true;
    forkJoin({ runtime: this.api.runtime(), global: this.api.globalConfig() }).subscribe({
      next: ({ runtime, global }) => {
        this.runtime = runtime;
        this.global = global;
        this.draft = structuredClone(global.config);
        this.loading = false;
      },
      error: (err) => {
        this.loading = false;
        this.toast.error(err.error?.error || '设置加载异常');
      }
    });
  }

  save(): void {
    if (!this.global || !this.draft) return;
    this.saving = true;
    this.api.saveGlobalConfig(this.global.generation, this.normalizeDraft()).subscribe({
      next: (global) => {
        this.global = global;
        this.draft = structuredClone(global.config);
        this.saving = false;
        this.toast.success('全局设置已保存。');
      },
      error: (err) => {
        this.saving = false;
        this.toast.error(err.error?.error || '保存操作异常');
      }
    });
  }

  promptReset(): void {
    this.modal.confirm({
      title: '确认重置',
      message: '此操作将恢复全局配置和实例默认值，不可撤销。',
      confirmLabel: '重置全部',
      danger: true
    }).subscribe((ok) => {
      if (!ok) return;
      this.api.resetSystem().subscribe({
        next: () => {
          this.toast.success('已恢复默认配置。');
          this.load();
        },
        error: (err) => this.toast.error(err.error?.error || '重置操作异常')
      });
    });
  }

  private normalizeDraft(): GlobalConfig {
    const next = structuredClone(this.draft!);
    next.metrics.path = next.metrics.path.trim();
    next.storage.gc.blob = next.storage.gc.blob.trim();
    return next;
  }
}
