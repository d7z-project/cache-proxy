import { Component, inject } from '@angular/core';
import { NgIf, AsyncPipe } from '@angular/common';
import { ModalService } from './modal.service';

@Component({
  selector: 'app-modal',
  imports: [NgIf, AsyncPipe],
  template: `
    <div class="modal-backdrop" *ngIf="svc.state | async as cfg" (click)="svc.close(false)">
      <div class="modal-card card" (click)="$event.stopPropagation()">
        <h3>{{ cfg.title }}</h3>
        <p class="hint">{{ cfg.message }}</p>
        <div class="actions">
          <button type="button" [class.danger]="cfg.danger" (click)="svc.close(true)">{{ cfg.confirmLabel || '确认' }}</button>
          <button type="button" (click)="svc.close(false)">取消</button>
        </div>
      </div>
    </div>
  `
})
export class ModalComponent {
  readonly svc = inject(ModalService);
}
