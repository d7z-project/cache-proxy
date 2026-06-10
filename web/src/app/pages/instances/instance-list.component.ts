import { Component, OnInit, HostListener, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { forkJoin } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { ConfigSnapshot, InstanceConfig } from '../../core/api.models';
import { ToastService } from '../../shared/toast.service';
import { ModalService } from '../../shared/modal.service';
import { ModeLabelPipe } from '../../shared/mode-label.pipe';
import { ImportExportModalComponent } from '../../shared/import-export-modal.component';

@Component({
  selector: 'app-instance-list',
  imports: [FormsModule, ModeLabelPipe, ImportExportModalComponent],
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

  ngOnInit(): void { this.load(); }

  get instances(): { name: string; config: InstanceConfig }[] {
    const map = this.snapshot?.config.instances ?? {};
    return Object.keys(map).sort().map((name) => ({ name, config: map[name] }));
  }

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

  load(): void {
    this.loading = true;
    forkJoin({ snapshot: this.api.config(), runtime: this.api.runtime() }).subscribe({
      next: ({ snapshot }) => { this.snapshot = snapshot; this.loading = false; },
      error: (err) => { this.loading = false; this.toast.error(err.error?.error || '配置加载异常'); }
    });
  }
}
