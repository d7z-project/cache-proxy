import { Component, inject } from '@angular/core';
import { AsyncPipe, NgFor, NgIf } from '@angular/common';
import { forkJoin } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { InstanceMetrics } from '../../core/api.models';

interface ChartSlice {
  label: string;
  value: number;
  percent: number;
}

@Component({
  selector: 'app-dashboard',
  imports: [AsyncPipe, NgFor, NgIf],
  templateUrl: './dashboard.component.html'
})
export class DashboardComponent {
  private readonly api = inject(ApiService);
  readonly data$ = forkJoin({
    runtime: this.api.runtime(),
    instances: this.api.instances(),
    config: this.api.config(),
    metrics: this.api.metricsStats()
  });

  cacheText(metrics: InstanceMetrics): string {
    return this.mapText(metrics.cache);
  }

  upstreamText(metrics: InstanceMetrics): string {
    return this.mapText(metrics.upstreamStatus);
  }

  cacheSlices(metrics: InstanceMetrics): ChartSlice[] {
    return this.slices(metrics.cache);
  }

  upstreamSlices(metrics: InstanceMetrics): ChartSlice[] {
    return this.slices(metrics.upstreamStatus);
  }

  instanceMetrics(metrics: Record<string, InstanceMetrics>, name: string): InstanceMetrics {
    return metrics[name] ?? {
      requests: 0,
      errors: 0,
      responseBytes: 0,
      cache: {},
      upstreamRequests: 0,
      upstreamErrors: 0,
      upstreamStatus: {},
      activeDownloads: 0
    };
  }

  requestPercent(metrics: Record<string, InstanceMetrics>, name: string): number {
    const max = Math.max(1, ...Object.values(metrics ?? {}).map((item) => item.requests || 0));
    return Math.round((this.instanceMetrics(metrics, name).requests / max) * 100);
  }

  formatBytes(value: number): string {
    if (!value) return '0 B';
    const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
    let size = value;
    let unit = 0;
    while (size >= 1024 && unit < units.length - 1) {
      size /= 1024;
      unit++;
    }
    return `${size.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
  }

  private mapText(value: Record<string, number>): string {
    const entries = Object.entries(value ?? {}).sort(([left], [right]) => left.localeCompare(right));
    if (entries.length === 0) return '-';
    return entries.map(([key, count]) => `${key}: ${count}`).join(' · ');
  }

  private slices(value: Record<string, number>): ChartSlice[] {
    const entries = Object.entries(value ?? {}).filter(([, count]) => count > 0).sort(([, left], [, right]) => right - left);
    const total = entries.reduce((sum, [, count]) => sum + count, 0);
    return entries.map(([label, count]) => ({ label, value: count, percent: total === 0 ? 0 : Math.round((count / total) * 100) }));
  }
}
