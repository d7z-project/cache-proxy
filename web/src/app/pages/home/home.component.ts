import { Component, OnInit, inject } from '@angular/core';
import { AsyncPipe } from '@angular/common';
import { RouterLink } from '@angular/router';
import { NgbAccordionModule } from '@ng-bootstrap/ng-bootstrap';
import { Observable, catchError, map, of } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { AuthService } from '../../core/auth.service';
import { InstanceSummary } from '../../core/api.models';
import { ModeLabelPipe } from '../../shared/mode-label.pipe';
import { CodeBlockComponent } from '../../shared/code-block.component';
import { InstanceGuide, buildInstanceGuide } from '../../shared/proxy-guides';

interface HomeGuideItem {
  instance: InstanceSummary;
  guide: InstanceGuide;
}

@Component({
  selector: 'app-home',
  imports: [AsyncPipe, RouterLink, NgbAccordionModule, ModeLabelPipe, CodeBlockComponent],
  templateUrl: './home.component.html'
})
export class HomeComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly auth = inject(AuthService);

  items$?: Observable<HomeGuideItem[]>;
  error = '';
  isAuth$ = this.auth.isAuth$;
  authEnabled$ = this.auth.authEnabled$;
  expandedInstance = '';

  ngOnInit(): void {
    this.items$ = this.api.publicInstances().pipe(
      map((instances) => instances.map((instance) => ({ instance, guide: buildInstanceGuide(instance) }))),
      catchError((err) => {
        this.error = err.status === 0 ? '无法连接到服务。' : '实例列表加载失败。';
        return of([]);
      })
    );
  }
}
