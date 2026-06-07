import { Component, OnInit, inject } from '@angular/core';
import { NgIf } from '@angular/common';
import { forkJoin } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { ConfigSnapshot, RuntimeInfo } from '../../core/api.models';

@Component({
  selector: 'app-settings',
  imports: [NgIf],
  templateUrl: './settings.component.html'
})
export class SettingsComponent implements OnInit {
  private readonly api = inject(ApiService);

  snapshot?: ConfigSnapshot;
  runtime?: RuntimeInfo;
  message = '';
  loading = true;

  ngOnInit(): void {
    this.load();
  }

  load(): void {
    this.loading = true;
    forkJoin({ runtime: this.api.runtime(), snapshot: this.api.config() }).subscribe({
      next: ({ runtime, snapshot }) => {
        this.runtime = runtime;
        this.snapshot = snapshot;
        this.loading = false;
        this.message = '';
      },
      error: (err) => this.fail(err, '配置加载异常')
    });
  }

  reset(): void {
    this.api.resetConfig().subscribe({
      next: (snapshot) => {
        this.snapshot = snapshot;
        this.message = '已恢复默认实例配置。';
      },
      error: (err) => this.fail(err, '重置操作异常')
    });
  }

  private fail(err: { error?: { error?: string } }, fallback: string): void {
    this.loading = false;
    this.message = err.error?.error || fallback;
  }
}
