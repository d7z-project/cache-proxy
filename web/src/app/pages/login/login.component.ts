import { Component, OnInit, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router, ActivatedRoute, RouterLink } from '@angular/router';
import { AuthService } from '../../core/auth.service';

@Component({
  selector: 'app-login',
  imports: [FormsModule, RouterLink],
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
    const raw = this.route.snapshot.queryParams['returnUrl'];
    if (typeof raw === 'string' && raw.startsWith('/') && !raw.includes('//') && !raw.includes('@')) {
      this.redirect = raw;
    }
  }

  submit(): void {
    this.error = '';
    this.loading = true;
    this.auth.login(this.password).subscribe({
      next: (result) => {
        this.loading = false;
        if (result.ok) {
          this.router.navigateByUrl(this.redirect);
        } else {
          this.error = result.error;
        }
      },
      error: () => { this.loading = false; this.error = '登录失败'; }
    });
  }
}
