import { Component, OnInit, inject } from '@angular/core';
import { AsyncPipe } from '@angular/common';
import { RouterLink } from '@angular/router';
import { Observable, catchError, of } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { AuthService } from '../../core/auth.service';
import { InstanceSummary } from '../../core/api.models';
import { ModeLabelPipe } from '../../shared/mode-label.pipe';

@Component({
  selector: 'app-home',
  imports: [AsyncPipe, RouterLink, ModeLabelPipe],
  templateUrl: './home.component.html'
})
export class HomeComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly auth = inject(AuthService);

  instances$?: Observable<InstanceSummary[]>;
  error = '';
  isAuth$ = this.auth.isAuth$;
  authEnabled$ = this.auth.authEnabled$;

  ngOnInit(): void {
    this.instances$ = this.api.publicInstances().pipe(
      catchError((err) => {
        this.error = err.status === 0 ? '无法连接到服务。' : '实例列表加载失败。';
        return of([]);
      })
    );
  }
}
