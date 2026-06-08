import { Injectable, inject } from '@angular/core';
import { CanActivate, Router } from '@angular/router';
import { AuthService } from './auth.service';
import { filter, map, take } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class LoginGuard implements CanActivate {
  private readonly auth = inject(AuthService);
  private readonly router = inject(Router);

  canActivate() {
    return this.auth.authEnabled$.pipe(
      filter((v): v is boolean => v !== null),
      take(1),
      map((enabled) => enabled ? true as const : this.router.parseUrl('/'))
    );
  }
}
