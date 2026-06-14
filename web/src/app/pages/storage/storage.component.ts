import { Component, inject, OnInit } from '@angular/core';
import { AsyncPipe } from '@angular/common';
import { BehaviorSubject, switchMap } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { StorageStats } from '../../core/api.models';
import { ToastService } from '../../shared/toast.service';

@Component({
  selector: 'app-storage',
  imports: [AsyncPipe],
  templateUrl: './storage.component.html'
})
export class StorageComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);

  private readonly refreshTrigger = new BehaviorSubject<void>(undefined);
  readonly stats$ = this.refreshTrigger.pipe(switchMap(() => this.api.storageStats()));

  gcRunning = false;
  gcNotice = '';

  ngOnInit(): void {
    this.refreshTrigger.next();
  }

  refresh(): void { this.refreshTrigger.next(); }

  runGc(): void {
    this.gcRunning = true;
    this.api.runGc().subscribe({
      next: () => {
        this.gcNotice = '已触发一次手动 GC，并刷新当前状态。';
        this.toast.success('GC 清理已完成。');
        this.gcRunning = false;
        this.refresh();
      },
      error: (err) => {
        this.gcRunning = false;
        this.toast.error(err.error?.error || '清理操作异常');
      }
    });
  }

  bytesText(value: number): string {
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

  timeText(value?: string): string {
    return value || '暂无记录';
  }

  stateText(value?: string): string {
    return value || 'none';
  }

  errorText(value?: string): string {
    return value || '无';
  }

  statsCards(stats: StorageStats): Array<{ label: string; value: string }> {
    return [
      { label: '租户', value: String(stats.Tenants) },
      { label: '对象', value: String(stats.Objects) },
      { label: '目录', value: String(stats.Directories) },
      { label: '逻辑容量', value: this.bytesText(stats.Bytes.LogicalObjectBytes) },
      { label: '存储容量', value: this.bytesText(stats.Bytes.StoredChunkBytes) },
      { label: '元数据版本', value: String(stats.TxID) }
    ];
  }
}
