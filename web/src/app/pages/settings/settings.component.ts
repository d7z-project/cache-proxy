import { Component, OnInit, inject } from '@angular/core';
import { forkJoin } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { ConfigSnapshot, RuntimeInfo } from '../../core/api.models';
import { ToastService } from '../../shared/toast.service';
import { ModalService } from '../../shared/modal.service';

@Component({
  selector: 'app-settings',
  imports: [],
  templateUrl: './settings.component.html'
})
export class SettingsComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);
  private readonly modal = inject(ModalService);

  snapshot?: ConfigSnapshot;
  runtime?: RuntimeInfo;
  loading = true;

  ngOnInit(): void { this.load(); }

  load(): void {
    this.loading = true;
    forkJoin({ runtime: this.api.runtime(), snapshot: this.api.config() }).subscribe({
      next: ({ runtime, snapshot }) => { this.runtime = runtime; this.snapshot = snapshot; this.loading = false; },
      error: (err) => { this.loading = false; this.toast.error(err.error?.error || '配置加载异常'); }
    });
  }

  promptReset(): void {
    this.modal.confirm({
      title: '确认重置',
      message: '此操作将清空所有实例配置并恢复默认，不可撤销。',
      confirmLabel: '重置全部',
      danger: true
    }).subscribe(ok => {
      if (ok) this.api.resetConfig().subscribe({
        next: (snapshot) => { this.snapshot = snapshot; this.toast.success('已恢复默认实例配置。'); },
        error: (err) => this.toast.error(err.error?.error || '重置操作异常')
      });
    });
  }
}
