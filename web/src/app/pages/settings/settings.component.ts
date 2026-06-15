import { Component, OnInit, inject } from '@angular/core';
import { AsyncPipe, DecimalPipe } from '@angular/common';
import { BehaviorSubject, switchMap, tap } from 'rxjs';
import { RuntimeInfo, StorageStats } from '../../core/api.models';
import { ApiService } from '../../core/api.service';
import { ToastService } from '../../shared/toast.service';
import { ModalService } from '../../shared/modal.service';

@Component({
  selector: 'app-settings',
  imports: [AsyncPipe, DecimalPipe],
  templateUrl: './settings.component.html'
})
export class SettingsComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);
  private readonly modal = inject(ModalService);

  private readonly refreshStorage = new BehaviorSubject<void>(undefined);
  readonly storageStats$ = this.refreshStorage.pipe(
    switchMap(() => this.api.storageStats()),
    tap(stats => this.storageStats = stats)
  );

  runtime?: RuntimeInfo;
  storageStats?: StorageStats;
  loading = true;
  gcRunning = false;
  gcNotice = '';

  ngOnInit(): void { this.load(); }

  load(): void {
    this.loading = true;
    this.api.runtime().subscribe({
      next: (runtime) => {
        this.runtime = runtime;
        this.loading = false;
        this.refreshStorage.next();
      },
      error: (err) => {
        this.loading = false;
        this.toast.error(err.error?.error || '系统信息加载异常');
      }
    });
  }

  refreshStorageStats(): void { this.refreshStorage.next(); }

  runGc(): void {
    this.gcRunning = true;
    this.gcNotice = '';
    this.api.runGc().subscribe({
      next: () => {
        this.gcNotice = '已触发一次手动 GC。';
        this.toast.success('GC 清理已完成。');
        this.gcRunning = false;
        this.refreshStorage.next();
      },
      error: (err) => {
        this.gcRunning = false;
        this.toast.error(err.error?.error || '清理操作异常');
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

  bytesText(value?: number): string {
    if (!value) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let next = value;
    let index = 0;
    while (next >= 1024 && index < units.length - 1) {
      next /= 1024;
      index++;
    }
    return `${next.toFixed(next >= 10 || index === 0 ? 0 : 1)} ${units[index]}`;
  }

  timeText(value?: string): string { return value || '暂无记录'; }
  stateText(value?: string): string { return value || 'none'; }
  errorText(value?: string): string { return value || '无'; }
}
