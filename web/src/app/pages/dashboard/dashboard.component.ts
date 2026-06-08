import { Component, inject } from '@angular/core';
import { AsyncPipe, NgFor, NgIf } from '@angular/common';
import { BehaviorSubject, Observable, forkJoin, of, catchError, switchMap } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { InstanceMetrics, MetricsStats, RuntimeInfo, InstanceSummary, ConfigSnapshot } from '../../core/api.models';
import { PROXY_MODE_OPTIONS } from '../../core/config-options';

interface DashboardData {
  runtime: RuntimeInfo;
  instances: InstanceSummary[];
  config: ConfigSnapshot;
  metrics: MetricsStats;
}

interface ChartSlice { label: string; value: number; percent: number; }

@Component({
  selector: 'app-dashboard',
  imports: [AsyncPipe, NgFor, NgIf],
  templateUrl: './dashboard.component.html'
})
export class DashboardComponent {
  private readonly api = inject(ApiService);

  private readonly refresh$ = new BehaviorSubject<void>(undefined);
  readonly data$: Observable<DashboardData | null> = this.refresh$.pipe(
    switchMap(() => forkJoin({
      runtime: this.api.runtime(),
      instances: this.api.instances(),
      config: this.api.config(),
      metrics: this.api.metricsStats()
    }).pipe(catchError(() => of(null))))
  );

  refresh(): void { this.refresh$.next(); }

  sliceList(value: Record<string, number>): ChartSlice[] {
    const entries = Object.entries(value ?? {}).filter(([, c]) => c > 0).sort(([, l], [, r]) => r - l);
    const total = entries.reduce((s, [, c]) => s + c, 0);
    return entries.map(([label, count]) => ({ label, value: count, percent: total === 0 ? 0 : Math.round((count / total) * 100) }));
  }

  cacheText(metrics: InstanceMetrics): string {
    const entries = Object.entries(metrics.cache ?? {}).sort(([l], [r]) => l.localeCompare(r));
    return entries.length === 0 ? '-' : entries.map(([k, c]) => `${k}: ${c}`).join(' · ');
  }

  upstreamText(metrics: InstanceMetrics): string {
    const entries = Object.entries(metrics.upstreamStatus ?? {}).sort(([l], [r]) => l.localeCompare(r));
    return entries.length === 0 ? '-' : entries.map(([k, c]) => `${k}: ${c}`).join(' · ');
  }

  getMetrics(metrics: Record<string, InstanceMetrics>, name: string): InstanceMetrics {
    return metrics[name] ?? { requests: 0, errors: 0, responseBytes: 0, cache: {}, upstreamRequests: 0, upstreamErrors: 0, upstreamStatus: {}, activeDownloads: 0 };
  }

  requestPercent(metrics: Record<string, InstanceMetrics>, name: string): number {
    const m = this.getMetrics(metrics, name);
    const max = Math.max(1, ...Object.values(metrics ?? {}).map((item) => item.requests || 0));
    return Math.round((m.requests / max) * 100);
  }

  formatBytes(value: number): string {
    if (!value) return '0 B';
    const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
    let size = value, unit = 0;
    while (size >= 1024 && unit < units.length - 1) { size /= 1024; unit++; }
    return `${size.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
  }

  modeLabel(mode: string): string {
    return PROXY_MODE_OPTIONS.find((o) => o.value === mode)?.label ?? mode;
  }
}
