import { Component, inject } from '@angular/core';
import { AsyncPipe } from '@angular/common';
import { ModalService } from './modal.service';

@Component({
  selector: 'app-modal',
  imports: [AsyncPipe],
  template: `
    @if (svc.state | async; as cfg) {
      <div class="modal-backdrop" (click)="svc.close(false)">
        <div class="modal-card card" (click)="$event.stopPropagation()">
          <h3>{{ cfg.title }}</h3>
          <p class="hint">{{ cfg.message }}</p>
          <div class="actions modal-actions">
            <button type="button" (click)="svc.close(false)">取消</button>
            <button type="button" [class.danger]="cfg.danger" (click)="svc.close(true)">{{ cfg.confirmLabel || '确认' }}</button>
          </div>
        </div>
      </div>
    }
  `
})
export class ModalComponent {
  readonly svc = inject(ModalService);
}
