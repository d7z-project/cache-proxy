import { Component, OnInit, HostListener, inject } from '@angular/core';
import { NgFor, NgIf } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { forkJoin } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { ConfigSnapshot, InstanceConfig } from '../../core/api.models';
import { PROXY_MODE_OPTIONS } from '../../core/config-options';
import { ToastService } from '../../shared/toast.service';
import { ModalService } from '../../shared/modal.service';

@Component({
  selector: 'app-instance-list',
  imports: [NgFor, NgIf, FormsModule],
  templateUrl: './instance-list.component.html'
})
export class InstanceListComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly router = inject(Router);
  private readonly toast = inject(ToastService);
  private readonly modal = inject(ModalService);

  snapshot?: ConfigSnapshot;
  loading = true;

  dropdownOpen = false;
  showImportExport = false;
  importText = '';
  importReplace = false;
  saving = false;

  readonly proxyModes = PROXY_MODE_OPTIONS;

  ngOnInit(): void { this.load(); }

  get instances(): { name: string; config: InstanceConfig }[] {
    const map = this.snapshot?.config.instances ?? {};
    return Object.keys(map).sort().map((name) => ({ name, config: map[name] }));
  }

  modeLabel(mode: string): string { return this.proxyModes.find((o) => o.value === mode)?.label ?? mode; }

  toggleDropdown(): void { this.dropdownOpen = !this.dropdownOpen; }

  @HostListener('document:click', ['$event'])
  onDocClick(event: MouseEvent): void {
    const target = event.target as HTMLElement;
    if (!target.closest('.dropdown')) this.dropdownOpen = false;
  }

  startCreate(mode: string): void {
    this.dropdownOpen = false;
    this.router.navigate(['/instances/new'], { queryParams: { mode } });
  }

  startEdit(name: string): void { this.router.navigate(['/instances', name]); }

  copyInstance(name: string): void {
    const inst = this.snapshot?.config.instances[name];
    if (!inst) return;
    this.router.navigate(['/instances/new'], { queryParams: { mode: inst.mode, copy: name } });
  }

  promptDelete(name: string): void {
    this.modal.confirm({
      title: '确认删除',
      message: `确定删除实例 "${name}"？此操作不可撤销。`,
      confirmLabel: '删除',
      danger: true
    }).subscribe(confirmed => {
      if (confirmed && this.snapshot) {
        const next = structuredClone(this.snapshot.config);
        delete next.instances[name];
        this.api.saveConfig(this.snapshot.generation, next).subscribe({
          next: (snapshot) => { this.snapshot = snapshot; this.toast.success('实例已删除。'); },
          error: (err) => this.toast.error(err.error?.error || '删除操作异常')
        });
      }
    });
  }

  openImportExport(): void { this.showImportExport = true; }

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
    this.api.importInstances(this.snapshot!.generation, instances, this.importReplace).subscribe({
      next: (snapshot) => {
        this.saving = false;
        this.snapshot = snapshot;
        this.importText = '';
        this.showImportExport = false;
        this.toast.success('实例已导入。');
      },
      error: (err) => { this.saving = false; this.toast.error(err.error?.error || '导入操作异常'); }
    });
  }

  private load(): void {
    this.loading = true;
    forkJoin({ snapshot: this.api.config(), runtime: this.api.runtime() }).subscribe({
      next: ({ snapshot }) => { this.snapshot = snapshot; this.loading = false; },
      error: (err) => { this.loading = false; this.toast.error(err.error?.error || '配置加载异常'); }
    });
  }
}
