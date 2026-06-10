import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { AppConfig, CacheLookupResult, ConfigSnapshot, InstanceSummary, InstancesExport, MetricsStats, RuntimeInfo, StorageStats } from './api.models';

@Injectable({ providedIn: 'root' })
export class ApiService {
  private readonly http = inject(HttpClient);
  private readonly base = '/-/api';

  publicInstances(): Observable<InstanceSummary[]> {
    return this.http.get<InstanceSummary[]>(`${this.base}/public/instances`);
  }

  runtime(): Observable<RuntimeInfo> {
    return this.http.get<RuntimeInfo>(`${this.base}/runtime`, { withCredentials: true });
  }

  metricsStats(): Observable<MetricsStats> {
    return this.http.get<MetricsStats>(`${this.base}/metrics/stats`, { withCredentials: true });
  }

  instances(): Observable<InstanceSummary[]> {
    return this.http.get<InstanceSummary[]>(`${this.base}/instances`, { withCredentials: true });
  }

  exportInstances(name?: string): Observable<InstancesExport> {
    const suffix = name ? `?name=${encodeURIComponent(name)}` : '';
    return this.http.get<InstancesExport>(`${this.base}/instances/export${suffix}`, { withCredentials: true });
  }

  importInstances(generation: number, instances: Record<string, unknown>, replace: boolean): Observable<ConfigSnapshot> {
    return this.http.post<ConfigSnapshot>(`${this.base}/instances/import`, { generation, instances, replace }, { withCredentials: true });
  }

  config(): Observable<ConfigSnapshot> {
    return this.http.get<ConfigSnapshot>(`${this.base}/config`, { withCredentials: true });
  }

  saveConfig(generation: number, config: AppConfig): Observable<ConfigSnapshot> {
    return this.http.put<ConfigSnapshot>(`${this.base}/config`, { generation, config }, { withCredentials: true });
  }

  validateConfig(config: AppConfig): Observable<{ valid: boolean }> {
    return this.http.post<{ valid: boolean }>(`${this.base}/config/validate`, config, { withCredentials: true });
  }

  resetConfig(): Observable<ConfigSnapshot> {
    return this.http.post<ConfigSnapshot>(`${this.base}/config/reset`, {}, { withCredentials: true });
  }

  storageStats(): Observable<StorageStats> {
    return this.http.get<StorageStats>(`${this.base}/storage/stats`, { withCredentials: true });
  }

  runGc(): Observable<StorageStats> {
    return this.http.post<StorageStats>(`${this.base}/storage/gc`, {}, { withCredentials: true });
  }

  cacheLookup(instance: string, path: string): Observable<CacheLookupResult> {
    return this.http.get<CacheLookupResult>(`${this.base}/cache/lookup`, {
      params: { instance, path },
      withCredentials: true
    });
  }
}
