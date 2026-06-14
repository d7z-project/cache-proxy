import { Component, input } from '@angular/core';

@Component({
  selector: 'app-instance-entry',
  standalone: true,
  template: `
    @if (label()) {
      <div class="small text-muted mb-1">{{ label() }}</div>
    }
    <code class="instance-entry">{{ url() }}</code>
  `
})
export class InstanceEntryComponent {
  readonly label = input('');
  readonly url = input('');
}

