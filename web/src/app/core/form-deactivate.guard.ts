import { CanDeactivateFn } from '@angular/router';
import { Observable } from 'rxjs';

export interface CanComponentDeactivate {
  isDirty(): boolean;
  confirmDeactivate(): Observable<boolean>;
}

export const formDeactivateGuard: CanDeactivateFn<CanComponentDeactivate> = (component) => {
  if (!component.isDirty()) return true;
  return component.confirmDeactivate();
};
