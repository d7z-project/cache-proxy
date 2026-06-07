import { Component, inject } from '@angular/core';
import { AsyncPipe, KeyValuePipe, NgFor, NgIf } from '@angular/common';
import { ApiService } from '../../core/api.service';
import { StorageStats } from '../../core/api.models';

@Component({
  selector: 'app-storage',
  imports: [AsyncPipe, KeyValuePipe, NgFor, NgIf],
  templateUrl: './storage.component.html'
})
export class StorageComponent {
  private readonly api = inject(ApiService);
  private readonly labels: Record<string, string> = {
    TxID: '版本',
    Tenants: '租户',
    Inodes: '索引节点',
    Objects: '对象',
    Directories: '目录',
    Manifests: '清单',
    Chunks: '数据块',
    Segments: '数据段',
    Bytes: '容量',
    GC: '清理',
    GeneratedAt: '统计时间',
    Active: '活跃',
    Deleted: '已清理',
    GarbageCandidate: '待清理',
    Corrupt: '异常',
    Sealed: '可用',
    Compacting: '整理中',
    LogicalObjectBytes: '对象大小',
    RawChunkBytes: '原始大小',
    StoredChunkBytes: '存储大小',
    Runs: '运行次数',
    LastEpoch: '最近批次',
    LastRunState: '最近状态',
    LastBackgroundAt: '最近后台时间',
    LastBackgroundEpoch: '最近后台批次',
    LastBackgroundError: '最近后台状态'
  };

  stats$ = this.api.storageStats();
  gcResult?: StorageStats;
  message = '';

  refresh(): void {
    this.stats$ = this.api.storageStats();
  }

  runGc(): void {
    this.api.runGc().subscribe({
      next: (result) => {
        this.gcResult = result;
        this.message = '清理已完成。';
        this.refresh();
      },
      error: (err) => this.message = err.error?.error || '清理操作异常'
    });
  }

  valueText(value: unknown): string {
    if (value === null || value === undefined) return '-';
    if (typeof value === 'object') return this.objectText(value as Record<string, unknown>);
    return String(value);
  }

  labelText(key: string): string {
    return this.labels[key] ?? key;
  }

  private objectText(value: Record<string, unknown>): string {
    const entries = Object.entries(value ?? {}).filter(([, item]) => item !== null && item !== undefined && item !== '');
    if (entries.length === 0) return '-';
    return entries.map(([key, item]) => {
      const text = typeof item === 'object' ? this.objectText(item as Record<string, unknown>) : String(item);
      return `${this.labelText(key)}: ${text}`;
    }).join(' · ');
  }
}
