import { Component, inject } from '@angular/core';
import { AsyncPipe } from '@angular/common';
import { ToastService } from './toast.service';

@Component({
  selector: 'app-toast-container',
  imports: [AsyncPipe],
  template: `
    <div class="toast-stack">
      @for (t of svc.toasts | async; track t.id) {
        <div class="toast-item" [class]="t.type">
          <span class="toast-message">{{ t.message }}</span>
          <button class="toast-close" (click)="svc.dismiss(t.id); $event.stopPropagation()">×</button>
        </div>
      }
    </div>
  `,
  styles: [`
    .toast-stack { position: fixed; top: 1rem; right: 1rem; z-index: var(--z-toast); display: grid; gap: 0.5rem; max-width: 380px; pointer-events: none; }
    .toast-item { display: flex; align-items: center; gap: 8px; padding: 12px 16px; border-radius: var(--radius-sm); font-weight: 700; box-shadow: var(--shadow-toast); pointer-events: auto; animation: toastIn .25s ease; }
    .toast-message { flex: 1; }
    .toast-close { background: none; border: none; padding: 0; font-size: 18px; line-height: 1; cursor: pointer; opacity: 0.6; transition: opacity .15s; width: auto; height: auto; }
    .toast-close:hover { opacity: 1; background: none; border: none; }
    .toast-item.success { background: var(--color-success-light); color: var(--color-success); border: 1px solid var(--color-success-border); }
    .toast-item.success .toast-close { color: var(--color-success); }
    .toast-item.error { background: var(--color-danger-hover); color: #991b1b; border: 1px solid var(--color-danger-border); }
    .toast-item.error .toast-close { color: #991b1b; }
    .toast-item.info { background: var(--color-primary-light); color: var(--color-primary-hover); border: 1px solid var(--color-primary-border); }
    .toast-item.info .toast-close { color: var(--color-primary-hover); }
    @keyframes toastIn { from { transform: translateX(100%); opacity: 0; } }
  `]
})
export class ToastContainerComponent {
  readonly svc = inject(ToastService);
}
