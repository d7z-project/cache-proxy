import { Routes } from '@angular/router';
import { HomeComponent } from './pages/home/home.component';
import { LoginComponent } from './pages/login/login.component';
import { DashboardComponent } from './pages/dashboard/dashboard.component';
import { InstanceListComponent } from './pages/instances/instance-list.component';
import { InstanceFormComponent } from './pages/instances/instance-form.component';
import { SettingsComponent } from './pages/settings/settings.component';
import { StorageComponent } from './pages/storage/storage.component';
import { AuthGuard } from './core/auth.guard';
import { LoginGuard } from './core/login.guard';
import { formDeactivateGuard } from './core/form-deactivate.guard';

export const routes: Routes = [
  { path: '', component: HomeComponent },
  { path: 'login', component: LoginComponent, canActivate: [LoginGuard] },
  { path: 'dashboard', component: DashboardComponent, canActivate: [AuthGuard] },
  { path: 'instances', component: InstanceListComponent, canActivate: [AuthGuard] },
  { path: 'instances/new', component: InstanceFormComponent, canActivate: [AuthGuard], canDeactivate: [formDeactivateGuard] },
  { path: 'instances/:name', component: InstanceFormComponent, canActivate: [AuthGuard], canDeactivate: [formDeactivateGuard] },
  { path: 'settings', component: SettingsComponent, canActivate: [AuthGuard] },
  { path: 'storage', component: StorageComponent, canActivate: [AuthGuard] },
  { path: '**', redirectTo: '' }
];
