import { Component, OnInit, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { NgbDropdown, NgbDropdownToggle, NgbDropdownMenu, NgbDropdownItem } from '@ng-bootstrap/ng-bootstrap';
import { ApiService } from '../../core/api.service';
import { CacheLookupResult, InstanceCollectionResponse, InstanceSummary, ProxyMode } from '../../core/api.models';
import { ToastService } from '../../shared/toast.service';
import { ModalService } from '../../shared/modal.service';
import { ModeLabelPipe } from '../../shared/mode-label.pipe';
import { ImportExportModalComponent } from '../../shared/import-export-modal.component';

@Component({
  selector: 'app-instance-list',
  imports: [FormsModule, ModeLabelPipe, NgbDropdown, NgbDropdownToggle, NgbDropdownMenu, NgbDropdownItem, ImportExportModalComponent],
  templateUrl: './instance-list.component.html'
})
export class InstanceListComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly router = inject(Router);
  private readonly toast = inject(ToastService);
  private readonly modal = inject(ModalService);

  collection?: InstanceCollectionResponse;
  loading = true;
  showImportExport = false;

  showLookupModal = false;
  lookupInstanceName = '';
  lookupMode: ProxyMode = ProxyMode.File;
  lookupPath = '';
  lookupRunning = false;
  lookupResult?: CacheLookupResult;

  readonly ProxyMode = ProxyMode;

  ngOnInit(): void { this.load(); }

  get instances(): InstanceSummary[] {
    return this.collection?.items ?? [];
  }

  get generation(): number {
    return this.collection?.generation ?? 0;
  }

  get lookupPlaceholder(): string {
    switch (this.lookupMode) {
      case ProxyMode.Npm: return '@angular/core';
      case ProxyMode.Oci: return 'library/alpine:latest';
      case ProxyMode.Go: return 'golang.org/x/mod/@v/list';
      default: return 'nginx.conf';
    }
  }

  startCreate(mode: ProxyMode): void {
    this.router.navigate(['/instances/new'], { queryParams: { mode } });
  }

  startEdit(name: string): void {
    this.router.navigate(['/instances', name]);
  }

  copyInstance(name: string): void {
    const item = this.instances.find((instance) => instance.name === name);
    if (!item) return;
    this.router.navigate(['/instances/new'], { queryParams: { mode: item.mode, copy: name } });
  }

  promptDelete(name: string): void {
    this.modal.confirm({
      title: '确认删除',
      message: `确定删除实例 "${name}"？此操作不可撤销。`,
      confirmLabel: '删除',
      danger: true
    }).subscribe((confirmed) => {
      if (!confirmed) return;
      this.api.deleteInstance(this.generation, name).subscribe({
        next: () => {
          this.toast.success('实例已删除。');
          this.load();
        },
        error: (err) => this.toast.error(err.error?.error || '删除操作异常')
      });
    });
  }

  openLookupModal(instanceName: string): void {
    const item = this.instances.find((instance) => instance.name === instanceName);
    this.lookupMode = item?.mode ?? ProxyMode.File;
    this.lookupInstanceName = instanceName;
    this.lookupPath = '';
    this.lookupResult = undefined;
    this.showLookupModal = true;
  }

  closeLookupModal(): void {
    this.showLookupModal = false;
    this.lookupInstanceName = '';
    this.lookupPath = '';
    this.lookupResult = undefined;
  }

  lookupCache(): void {
    if (!this.lookupInstanceName || !this.lookupPath.trim()) return;
    this.lookupRunning = true;
    this.lookupResult = undefined;
    this.api.cacheLookup(this.lookupInstanceName, this.lookupPath.trim()).subscribe({
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

  load(): void {
    this.loading = true;
    this.api.instancesCollection().subscribe({
      next: (collection) => {
        this.collection = collection;
        this.loading = false;
      },
      error: (err) => {
        this.loading = false;
        this.toast.error(err.error?.error || '实例列表加载异常');
      }
    });
  }
}
