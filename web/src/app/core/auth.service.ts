import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { BehaviorSubject, map, catchError, of, timeout } from 'rxjs';
import { RuntimeInfo } from './api.models';

const AUTH_INIT_TIMEOUT = 5000;

@Injectable({ providedIn: 'root' })
export class AuthService {
  private readonly http = inject(HttpClient);
  private isAuth = new BehaviorSubject<boolean | null>(null);
  readonly isAuth$ = this.isAuth.asObservable();
  private authEnabled = new BehaviorSubject<boolean | null>(null);
  readonly authEnabled$ = this.authEnabled.asObservable();

  constructor() {
    this.http.get<RuntimeInfo>('/-/api/runtime', { withCredentials: true }).pipe(
      timeout(AUTH_INIT_TIMEOUT),
      map((info) => {
        this.authEnabled.next(info.authEnabled);
        this.isAuth.next(true);
        return null;
      }),
      catchError((err) => {
        if (err.status === 401) {
          this.authEnabled.next(true);
          this.isAuth.next(false);
        }
        return of(null);
      })
    ).subscribe();
  }

  login(password: string) {
    return this.http.post<{ ok: boolean }>(
      '/-/api/login', { password }, { withCredentials: true }
    ).pipe(
      map(() => { this.isAuth.next(true); return true; }),
      catchError(() => { this.isAuth.next(false); return of(false); })
    );
  }

  logout() {
    return this.http.post('/-/api/logout', {}, { withCredentials: true }).pipe(
      map(() => { this.isAuth.next(false); })
    );
  }
}
