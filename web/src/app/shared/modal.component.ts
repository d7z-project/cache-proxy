import { Component, Input } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

@Component({
  selector: 'app-modal',
  template: `
    <div class="modal-header">
      <h5 class="modal-title">{{ title }}</h5>
      <button type="button" class="btn-close" (click)="activeModal.dismiss()"></button>
    </div>
    <div class="modal-body">
      <p class="text-muted mb-0">{{ message }}</p>
    </div>
    <div class="modal-footer">
      <button type="button" class="btn btn-outline-secondary" (click)="activeModal.close(false)">取消</button>
      <button type="button" [class.btn-danger]="danger" [class.btn-primary]="!danger" class="btn" (click)="activeModal.close(true)">{{ confirmLabel }}</button>
    </div>
  `
})
export class ModalComponent {
  @Input() title = '';
  @Input() message = '';
  @Input() confirmLabel = '确认';
  @Input() danger = false;

  constructor(readonly activeModal: NgbActiveModal) {}
}
