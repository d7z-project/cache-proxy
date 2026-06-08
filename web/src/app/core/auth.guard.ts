import { Injectable, inject } from '@angular/core';
import { CanActivate, Router } from '@angular/router';
import { AuthService } from './auth.service';
import { filter, map, take, race, timer } from 'rxjs';

const GUARD_TIMEOUT = 3000;

@Injectable({ providedIn: 'root' })
export class AuthGuard implements CanActivate {
  private readonly auth = inject(AuthService);
  private readonly router = inject(Router);

  canActivate() {
    return race([
      this.auth.isAuth$.pipe(filter((v): v is boolean => v !== null)),
      timer(GUARD_TIMEOUT).pipe(map(() => true as const))
    ]).pipe(
      take(1),
      map((v) => v === true ? true as const : this.router.parseUrl('/login'))
    );
  }
}
