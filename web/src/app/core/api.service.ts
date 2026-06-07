import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { AppConfig, ConfigSnapshot, InstanceSummary, InstancesExport, MetricsStats, RuntimeInfo, StorageStats } from './api.models';

@Injectable({ providedIn: 'root' })
export class ApiService {
  private readonly http = inject(HttpClient);

  runtime(): Observable<RuntimeInfo> {
    return this.http.get<RuntimeInfo>('/api/runtime');
  }

  metricsStats(): Observable<MetricsStats> {
    return this.http.get<MetricsStats>('/api/metrics/stats');
  }

  instances(): Observable<InstanceSummary[]> {
    return this.http.get<InstanceSummary[]>('/api/instances');
  }

  exportInstances(name?: string): Observable<InstancesExport> {
    const suffix = name ? `?name=${encodeURIComponent(name)}` : '';
    return this.http.get<InstancesExport>(`/api/instances/export${suffix}`);
  }

  importInstances(generation: number, instances: Record<string, unknown>, replace: boolean): Observable<ConfigSnapshot> {
    return this.http.post<ConfigSnapshot>('/api/instances/import', { generation, instances, replace });
  }

  config(): Observable<ConfigSnapshot> {
    return this.http.get<ConfigSnapshot>('/api/config');
  }

  saveConfig(generation: number, config: AppConfig): Observable<ConfigSnapshot> {
    return this.http.put<ConfigSnapshot>('/api/config', { generation, config });
  }

  validateConfig(config: AppConfig): Observable<{ valid: boolean }> {
    return this.http.post<{ valid: boolean }>('/api/config/validate', config);
  }

  resetConfig(): Observable<ConfigSnapshot> {
    return this.http.post<ConfigSnapshot>('/api/config/reset', {});
  }

  storageStats(): Observable<StorageStats> {
    return this.http.get<StorageStats>('/api/storage/stats');
  }

  runGc(): Observable<StorageStats> {
    return this.http.post<StorageStats>('/api/storage/gc', {});
  }
}
