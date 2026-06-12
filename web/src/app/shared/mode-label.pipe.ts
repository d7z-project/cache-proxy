import { Pipe, PipeTransform } from '@angular/core';
import { PROXY_MODE_OPTIONS, CACHE_POLICY_OPTIONS } from '../core/config-options';

const MODE_LABELS: Record<string, string> = {};
PROXY_MODE_OPTIONS.forEach(o => { MODE_LABELS[o.value] = o.label; });

const POLICY_LABELS: Record<string, string> = {};
CACHE_POLICY_OPTIONS.forEach(o => { POLICY_LABELS[o.value] = o.label; });

@Pipe({ name: 'modeLabel', pure: true })
export class ModeLabelPipe implements PipeTransform {
  transform(value: string, kind: 'mode' | 'policy' = 'mode'): string {
    if (kind === 'policy') return POLICY_LABELS[value] ?? value;
    return MODE_LABELS[value] ?? value;
  }
}
