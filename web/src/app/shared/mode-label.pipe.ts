import { Pipe, PipeTransform } from '@angular/core';
import { PROXY_MODE_OPTIONS } from '../core/config-options';

@Pipe({ name: 'modeLabel', pure: true })
export class ModeLabelPipe implements PipeTransform {
  transform(mode: string): string {
    return PROXY_MODE_OPTIONS.find(o => o.value === mode)?.label ?? mode;
  }
}
