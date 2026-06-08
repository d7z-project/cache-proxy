import { Component, inject } from '@angular/core';
import { NgFor, AsyncPipe } from '@angular/common';
import { ToastService } from './toast.service';

@Component({
  selector: 'app-toast-container',
  imports: [NgFor, AsyncPipe],
  template: `
    <div class="toast-stack">
      <div class="toast" *ngFor="let t of svc.toasts | async" [class]="'toast-item ' + t.type" (click)="svc.dismiss(t.id)">
        {{ t.message }}
      </div>
    </div>
  `,
  styles: [`
    .toast-stack { position: fixed; top: 1rem; right: 1rem; z-index: 9999; display: grid; gap: 0.5rem; max-width: 380px; pointer-events: none; }
    .toast-item { padding: 12px 16px; border-radius: 12px; font-weight: 700; cursor: pointer; box-shadow: 0 4px 20px rgba(0,0,0,.12); pointer-events: auto; animation: toastIn .25s ease; }
    .toast-item.success { background: #ecfdf5; color: #065f46; border: 1px solid #a7f3d0; }
    .toast-item.error { background: #fef2f2; color: #991b1b; border: 1px solid #fecaca; }
    .toast-item.info { background: #eff6ff; color: #1d4ed8; border: 1px solid #bfdbfe; }
    @keyframes toastIn { from { transform: translateX(100%); opacity: 0; } }
  `]
})
export class ToastContainerComponent {
  readonly svc = inject(ToastService);
}
