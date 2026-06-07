import { Routes } from '@angular/router';
import { DashboardComponent } from './pages/dashboard/dashboard.component';
import { InstancesComponent } from './pages/instances/instances.component';
import { SettingsComponent } from './pages/settings/settings.component';
import { StorageComponent } from './pages/storage/storage.component';

export const routes: Routes = [
  { path: 'dashboard', component: DashboardComponent },
  { path: 'instances', component: InstancesComponent },
  { path: 'settings', component: SettingsComponent },
  { path: 'storage', component: StorageComponent },
  { path: '**', redirectTo: 'dashboard' }
];
