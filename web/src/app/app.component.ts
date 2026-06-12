import { Component, inject } from '@angular/core';
import { RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';
import { AsyncPipe } from '@angular/common';
import { NgbCollapse } from '@ng-bootstrap/ng-bootstrap';
import { AuthService } from './core/auth.service';
import { ToastContainerComponent } from './shared/toast.component';

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, RouterLink, RouterLinkActive, AsyncPipe, NgbCollapse, ToastContainerComponent],
  templateUrl: './app.component.html'
})
export class AppComponent {
  private readonly auth = inject(AuthService);
  readonly isAuth$ = this.auth.isAuth$;
  readonly authEnabled$ = this.auth.authEnabled$;
  menuOpen = false;

  logout(): void {
    this.auth.logout().subscribe();
  }
}
