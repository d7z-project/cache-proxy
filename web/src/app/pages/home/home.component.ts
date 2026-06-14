import { Component, OnInit, inject } from '@angular/core';
import { AsyncPipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { Observable, catchError, of } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { AuthService } from '../../core/auth.service';
import { InstanceSummary } from '../../core/api.models';
import { InstanceEntryComponent } from '../../shared/instance-entry.component';
import { ModeLabelPipe } from '../../shared/mode-label.pipe';
import { InstanceGuide, buildInstanceGuide } from '../../shared/proxy-guides';

@Component({
  selector: 'app-home',
  imports: [AsyncPipe, FormsModule, RouterLink, ModeLabelPipe, InstanceEntryComponent],
  templateUrl: './home.component.html'
})
export class HomeComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly auth = inject(AuthService);

  instances$?: Observable<InstanceSummary[]>;
  error = '';
  isAuth$ = this.auth.isAuth$;
  authEnabled$ = this.auth.authEnabled$;
  selectedGuides: Record<string, number> = {};

  ngOnInit(): void {
    this.instances$ = this.api.publicInstances().pipe(
      catchError((err) => {
        this.error = err.status === 0 ? '无法连接到服务。' : '实例列表加载失败。';
        return of([]);
      })
    );
  }

  guideFor(instance: InstanceSummary): InstanceGuide {
    return buildInstanceGuide(instance);
  }

  selectedGuideIndex(instance: InstanceSummary): number {
    return this.selectedGuides[instance.name] ?? 0;
  }

  setSelectedGuideIndex(instance: InstanceSummary, index: number): void {
    this.selectedGuides[instance.name] = index;
  }
}
