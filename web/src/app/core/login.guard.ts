import { Injectable, inject } from '@angular/core';
import { CanActivate, Router } from '@angular/router';
import { AuthService } from './auth.service';
import { filter, map, take, race, timer } from 'rxjs';

const GUARD_TIMEOUT = 5000;

@Injectable({ providedIn: 'root' })
export class LoginGuard implements CanActivate {
  private readonly auth = inject(AuthService);
  private readonly router = inject(Router);

  canActivate() {
    return race([
      this.auth.authEnabled$.pipe(filter((v): v is boolean => v !== null)),
      timer(GUARD_TIMEOUT).pipe(map(() => true as const))
    ]).pipe(
      take(1),
      map((enabled) => enabled ? true as const : this.router.parseUrl('/'))
    );
  }
}
