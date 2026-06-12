import { Injectable, inject } from '@angular/core';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { Observable } from 'rxjs';
import { ModalComponent } from './modal.component';

export interface ModalConfig {
  title: string;
  message: string;
  confirmLabel?: string;
  danger?: boolean;
}

@Injectable({ providedIn: 'root' })
export class ModalService {
  private readonly ngbModal = inject(NgbModal);

  confirm(cfg: ModalConfig): Observable<boolean> {
    const ref = this.ngbModal.open(ModalComponent, { centered: true, size: 'sm' });
    ref.componentInstance.title = cfg.title;
    ref.componentInstance.message = cfg.message;
    ref.componentInstance.confirmLabel = cfg.confirmLabel ?? '确认';
    ref.componentInstance.danger = cfg.danger ?? false;
    return new Observable<boolean>(observer => {
      ref.result.then(
        (value) => { observer.next(value); observer.complete(); },
        () => { observer.next(false); observer.complete(); }
      );
    });
  }
}
