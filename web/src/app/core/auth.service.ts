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
      }),
      catchError(() => {
        this.authEnabled.next(true);
        this.isAuth.next(false);
        return of(null);
      })
    ).subscribe();
  }

  login(password: string) {
    return this.http.post<{ ok: boolean }>(
      '/-/api/login', { password }, { withCredentials: true }
    ).pipe(
      map(() => { this.isAuth.next(true); return { ok: true as const, error: '' }; }),
      catchError((err) => {
        this.isAuth.next(false);
        let error = '登录失败';
        if (err.status === 401) error = '密码错误';
        else if (err.status === 429) error = '登录尝试过于频繁，请稍后再试';
        else if (err.status === 0) error = '无法连接到服务';
        return of({ ok: false as const, error });
      })
    );
  }

  logout() {
    return this.http.post('/-/api/logout', {}, { withCredentials: true }).pipe(
      map(() => { this.isAuth.next(false); }),
      catchError(() => { this.isAuth.next(false); return of(null); })
    );
  }
}
