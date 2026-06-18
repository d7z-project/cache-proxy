import { Component, OnInit, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { NgbDropdown, NgbDropdownToggle, NgbDropdownMenu, NgbDropdownItem, NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { ApiService } from '../../core/api.service';
import { InstanceCollectionResponse, InstanceSummary, ProxyMode } from '../../core/api.models';
import { ToastService } from '../../shared/toast.service';
import { ModalService } from '../../shared/modal.service';
import { ModeLabelPipe } from '../../shared/mode-label.pipe';
import { ImportExportModalComponent } from '../../shared/import-export-modal.component';
import { CacheLookupModalComponent } from '../../shared/cache-lookup-modal.component';

@Component({
  selector: 'app-instance-list',
  imports: [FormsModule, ModeLabelPipe, NgbDropdown, NgbDropdownToggle, NgbDropdownMenu, NgbDropdownItem],
  templateUrl: './instance-list.component.html'
})
export class InstanceListComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly router = inject(Router);
  private readonly toast = inject(ToastService);
  private readonly modal = inject(ModalService);
  private readonly ngbModal = inject(NgbModal);

  collection?: InstanceCollectionResponse;
  loading = true;
  searchText = '';
  filterMode: ProxyMode | '' = '';

  readonly ProxyMode = ProxyMode;
  readonly allModes: (ProxyMode | '')[] = ['', ProxyMode.File, ProxyMode.Apk, ProxyMode.Deb, ProxyMode.Rpm, ProxyMode.Pacman, ProxyMode.Oci, ProxyMode.Npm, ProxyMode.Go, ProxyMode.Maven, ProxyMode.Cargo, ProxyMode.PyPI];

  ngOnInit(): void { this.load(); }

  get instances(): InstanceSummary[] {
    const items = this.collection?.items ?? [];
    let filtered = items;
    if (this.searchText.trim()) {
      const q = this.searchText.toLowerCase();
      filtered = filtered.filter((item) => item.name.toLowerCase().includes(q));
    }
    if (this.filterMode) {
      filtered = filtered.filter((item) => item.mode === this.filterMode);
    }
    return filtered;
  }

  entryFor(item: InstanceSummary): string {
    return item.entryUrl || item.publicUrl || item.path || item.bind || '';
  }

  get generation(): number {
    return this.collection?.generation ?? 0;
  }

  startCreate(mode: ProxyMode): void {
    this.router.navigate(['/instances/new'], { queryParams: { mode } });
  }

  startEdit(name: string): void {
    this.router.navigate(['/instances', name]);
  }

  copyInstance(name: string): void {
    const item = this.collection?.items.find((instance) => instance.name === name);
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
    const item = this.collection?.items.find((instance) => instance.name === instanceName);
    const ref = this.ngbModal.open(CacheLookupModalComponent, { centered: true, size: 'lg' });
    ref.componentInstance.instanceName = instanceName;
    ref.componentInstance.mode = item?.mode ?? ProxyMode.File;
  }

  openImportExport(): void {
    const ref = this.ngbModal.open(ImportExportModalComponent, { centered: true, size: 'lg' });
    ref.componentInstance.generation = this.generation;
    ref.result.then(
      (imported) => { if (imported) this.load(); },
      () => undefined
    );
  }

  clearFilters(): void {
    this.searchText = '';
    this.filterMode = '';
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
