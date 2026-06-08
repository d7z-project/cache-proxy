import { inject } from '@angular/core';
import { Router } from '@angular/router';
import { HttpInterceptorFn } from '@angular/common/http';
import { catchError, throwError } from 'rxjs';

export const authInterceptor: HttpInterceptorFn = (req, next) => {
  const router = inject(Router);
  return next(req).pipe(
    catchError((err) => {
      if (err.status === 401 && !req.url.includes('/-/api/login') && router.url !== '/login') {
        router.navigate(['/login']);
      }
      return throwError(() => err);
    })
  );
};
