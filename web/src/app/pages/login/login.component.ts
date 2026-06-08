import { Component, OnInit, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router, ActivatedRoute } from '@angular/router';
import { AuthService } from '../../core/auth.service';
import { NgIf } from '@angular/common';

@Component({
  selector: 'app-login',
  imports: [FormsModule, NgIf],
  templateUrl: './login.component.html'
})
export class LoginComponent implements OnInit {
  private readonly auth = inject(AuthService);
  private readonly router = inject(Router);
  private readonly route = inject(ActivatedRoute);

  password = '';
  error = '';
  loading = false;
  private redirect = '/dashboard';

  ngOnInit(): void {
    const raw = this.route.snapshot.queryParams['redirect'];
    if (typeof raw === 'string' && raw.startsWith('/') && !raw.includes('//') && !raw.includes('@')) {
      this.redirect = raw;
    }
  }

  submit(): void {
    this.error = '';
    this.loading = true;
    this.auth.login(this.password).subscribe({
      next: (ok) => {
        this.loading = false;
        if (ok) { this.router.navigateByUrl(this.redirect); } else { this.error = '密码错误'; }
      },
      error: () => { this.loading = false; this.error = '登录失败'; }
    });
  }
}
