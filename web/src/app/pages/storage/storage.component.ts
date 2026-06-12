import { Component, inject, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { AsyncPipe, KeyValuePipe } from '@angular/common';
import { BehaviorSubject, switchMap } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { CacheLookupResult, StorageStats } from '../../core/api.models';
import { ToastService } from '../../shared/toast.service';
import { ModeLabelPipe } from '../../shared/mode-label.pipe';

const LABELS: Record<string, string> = {
  TxID: '版本', Tenants: '租户', Inodes: '索引节点', Objects: '对象', Directories: '目录',
  Manifests: '清单', Chunks: '数据块', Segments: '数据段', Bytes: '容量', GC: '清理',
  GeneratedAt: '统计时间', Active: '活跃', Deleted: '已清理', GarbageCandidate: '待清理',
  Corrupt: '异常', Sealed: '可用', Compacting: '整理中', LogicalObjectBytes: '对象大小',
  RawChunkBytes: '原始大小', StoredChunkBytes: '存储大小', Runs: '运行次数', LastEpoch: '最近批次',
  LastRunState: '最近状态', LastBackgroundAt: '最近后台时间', LastBackgroundEpoch: '最近后台批次',
  LastBackgroundError: '最近后台状态'
};

@Component({
  selector: 'app-storage',
  imports: [FormsModule, AsyncPipe, KeyValuePipe, ModeLabelPipe],
  templateUrl: './storage.component.html'
})
export class StorageComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);

  private readonly refreshTrigger = new BehaviorSubject<void>(undefined);
  readonly stats$ = this.refreshTrigger.pipe(switchMap(() => this.api.storageStats()));

  gcResult?: StorageStats;
  gcRunning = false;

  instances: string[] = [];
  lookupInstance = '';
  lookupPath = '';
  lookupRunning = false;
  lookupResult?: CacheLookupResult;

  ngOnInit(): void {
    this.refreshTrigger.next();
    this.loadInstances();
  }

  private loadInstances(): void {
    this.api.instances().subscribe({
      next: (list) => { this.instances = list.map(i => i.name); },
      error: () => {}
    });
  }

  refresh(): void { this.refreshTrigger.next(); }

  runGc(): void {
    this.gcRunning = true;
    this.api.runGc().subscribe({
      next: (result) => {
        this.gcResult = result;
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

  lookupCache(): void {
    if (!this.lookupInstance || !this.lookupPath.trim()) return;
    this.lookupRunning = true;
    this.lookupResult = undefined;
    this.api.cacheLookup(this.lookupInstance, this.lookupPath.trim()).subscribe({
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

  labelText(key: string): string { return LABELS[key] ?? key; }

  valueText(value: unknown): string {
    if (value === null || value === undefined) return '-';
    if (typeof value === 'object') return this.objectText(value as Record<string, unknown>, 0);
    return String(value);
  }

  private objectText(value: Record<string, unknown>, depth: number): string {
    if (depth > 2) return '...';
    const entries = Object.entries(value).filter(([, v]) => v !== null && v !== undefined && v !== '');
    if (entries.length === 0) return '-';
    return entries.map(([k, v]) => {
      const text = typeof v === 'object' ? this.objectText(v as Record<string, unknown>, depth + 1) : String(v);
      return `${this.labelText(k)}: ${text}`;
    }).join(' · ');
  }
}
