import { Component, inject } from '@angular/core';
import { AsyncPipe } from '@angular/common';
import { NgbToast, NgbToastHeader } from '@ng-bootstrap/ng-bootstrap';
import { ToastService } from './toast.service';

@Component({
  selector: 'app-toast-container',
  imports: [AsyncPipe, NgbToast, NgbToastHeader],
  template: `
    <div class="toast-container position-fixed top-0 end-0 p-3" style="z-index: 9999;">
      @for (t of svc.toasts | async; track t.id) {
        <ngb-toast
          [class]="'text-bg-' + (t.type === 'error' ? 'danger' : t.type) + ' mb-2'"
          [autohide]="true"
          [delay]="3500"
          (hidden)="svc.dismiss(t.id)"
        >
          <ng-template ngbToastHeader>
            <strong class="me-auto">{{ t.type === 'error' ? '错误' : t.type === 'success' ? '成功' : '提示' }}</strong>
          </ng-template>
          {{ t.message }}
        </ngb-toast>
      }
    </div>
  `
})
export class ToastContainerComponent {
  readonly svc = inject(ToastService);
}
