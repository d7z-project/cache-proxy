import { Component, OnInit, inject } from '@angular/core';
import { RuntimeInfo } from '../../core/api.models';
import { ApiService } from '../../core/api.service';
import { ToastService } from '../../shared/toast.service';
import { ModalService } from '../../shared/modal.service';

@Component({
  selector: 'app-settings',
  templateUrl: './settings.component.html'
})
export class SettingsComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);
  private readonly modal = inject(ModalService);

  runtime?: RuntimeInfo;
  loading = true;

  ngOnInit(): void { this.load(); }

  load(): void {
    this.loading = true;
    this.api.runtime().subscribe({
      next: (runtime) => {
        this.runtime = runtime;
        this.loading = false;
      },
      error: (err) => {
        this.loading = false;
        this.toast.error(err.error?.error || '设置加载异常');
      }
    });
  }

  promptReset(): void {
    this.modal.confirm({
      title: '确认重置',
      message: '此操作将恢复实例默认配置，不可撤销。',
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
}
