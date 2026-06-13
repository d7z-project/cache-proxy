import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, map } from 'rxjs';
import {
  CacheLookupResult,
  ExportBundle,
  GlobalConfig,
  GlobalConfigResponse,
  InstanceCollectionResponse,
  InstanceDocumentResponse,
  InstanceSpec,
  InstanceSummary,
  MetricsStats,
  RuntimeInfo,
  StorageStats
} from './api.models';

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

  globalConfig(): Observable<GlobalConfigResponse> {
    return this.http.get<GlobalConfigResponse>(`${this.base}/global-config`, { withCredentials: true });
  }

  saveGlobalConfig(generation: number, config: GlobalConfig): Observable<GlobalConfigResponse> {
    return this.http.put<GlobalConfigResponse>(`${this.base}/global-config`, { generation, config }, { withCredentials: true });
  }

  instancesCollection(): Observable<InstanceCollectionResponse> {
    return this.http.get<InstanceCollectionResponse>(`${this.base}/instances`, { withCredentials: true });
  }

  instances(): Observable<InstanceSummary[]> {
    return this.http.get<InstanceCollectionResponse>(`${this.base}/instances`, { withCredentials: true }).pipe(map((response) => response.items));
  }

  instance(name: string): Observable<InstanceDocumentResponse> {
    return this.http.get<InstanceDocumentResponse>(`${this.base}/instances/${encodeURIComponent(name)}`, { withCredentials: true });
  }

  createInstance(generation: number, spec: InstanceSpec): Observable<InstanceDocumentResponse> {
    return this.http.post<InstanceDocumentResponse>(`${this.base}/instances`, { generation, spec }, { withCredentials: true });
  }

  updateInstance(generation: number, name: string, spec: InstanceSpec): Observable<InstanceDocumentResponse> {
    return this.http.put<InstanceDocumentResponse>(`${this.base}/instances/${encodeURIComponent(name)}`, { generation, spec }, { withCredentials: true });
  }

  deleteInstance(generation: number, name: string): Observable<{ generation: number; deleted: string }> {
    return this.http.delete<{ generation: number; deleted: string }>(`${this.base}/instances/${encodeURIComponent(name)}?generation=${generation}`, { withCredentials: true });
  }

  exportInstances(): Observable<ExportBundle> {
    return this.http.get<ExportBundle>(`${this.base}/instances/export`, { withCredentials: true });
  }

  importInstances(generation: number, instances: InstanceSpec[], replace: boolean): Observable<{ generation: number; imported: number }> {
    return this.http.post<{ generation: number; imported: number }>(`${this.base}/instances/import`, { generation, instances, replace }, { withCredentials: true });
  }

  resetSystem(): Observable<{ generation: number }> {
    return this.http.post<{ generation: number }>(`${this.base}/system/reset`, {}, { withCredentials: true });
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
