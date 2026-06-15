import { Component, computed, input } from '@angular/core';

@Component({
  selector: 'app-instance-entry',
  standalone: true,
  template: `
    @if (displayLabel()) {
      <div class="small text-muted mb-1">{{ displayLabel() }}</div>
    }
    <div class="form-control instance-entry font-monospace text-break">{{ url() }}</div>
  `
})
export class InstanceEntryComponent {
  readonly label = input('');
  readonly url = input('');
  readonly normalizeBindLabel = input(false);
  readonly displayLabel = computed(() => {
    const label = this.label();
    if (this.normalizeBindLabel() && (label === '独立端口' || label === '公开地址')) {
      return '访问入口';
    }
    return label;
  });
}
