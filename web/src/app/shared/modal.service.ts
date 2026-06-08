import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';

export interface ModalConfig {
  title: string;
  message: string;
  confirmLabel?: string;
  danger?: boolean;
}

interface ModalState extends ModalConfig {
  resolve: (value: boolean) => void;
}

@Injectable({ providedIn: 'root' })
export class ModalService {
  private state$ = new BehaviorSubject<ModalState | null>(null);
  readonly state = this.state$.asObservable();

  confirm(cfg: ModalConfig): Observable<boolean> {
    return new Observable<boolean>(observer => {
      this.state$.next({ ...cfg, resolve: (v: boolean) => { observer.next(v); observer.complete(); } });
    });
  }

  close(value: boolean) {
    const s = this.state$.value;
    if (s) { this.state$.next(null); s.resolve(value); }
  }
}
