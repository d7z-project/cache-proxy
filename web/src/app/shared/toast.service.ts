import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

export interface Toast {
  id: number;
  message: string;
  type: 'success' | 'error' | 'info';
}

@Injectable({ providedIn: 'root' })
export class ToastService {
  private state$ = new BehaviorSubject<Toast[]>([]);
  readonly toasts = this.state$.asObservable();
  private nextId = 0;

  success(msg: string) { this.push(msg, 'success'); }
  error(msg: string) { this.push(msg, 'error'); }
  info(msg: string) { this.push(msg, 'info'); }

  private push(message: string, type: Toast['type']) {
    const id = this.nextId++;
    this.state$.next([...this.state$.value, { id, message, type }]);
    setTimeout(() => this.dismiss(id), 3500);
  }

  dismiss(id: number) {
    this.state$.next(this.state$.value.filter(t => t.id !== id));
  }
}
