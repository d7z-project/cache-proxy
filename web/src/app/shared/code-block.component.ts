import { Component, Input, inject } from '@angular/core';
import { NgClass } from '@angular/common';
import { ToastService } from './toast.service';

@Component({
  selector: 'app-code-block',
  standalone: true,
  imports: [NgClass],
  template: `
    <div class="code-block">
      <div class="code-block-header">
        @if (lang) {
          <span class="code-block-lang">{{ lang }}</span>
        }
        <button
          type="button"
          class="btn btn-sm btn-copy"
          [class.btn-success]="copied"
          (click)="onCopy()"
        >
          {{ copied ? '已复制' : '复制' }}
        </button>
      </div>
      <pre class="code-block-content"><code [ngClass]="'language-' + (lang || 'text')">{{ code }}</code></pre>
    </div>
  `,
  styles: [`
    .code-block {
      position: relative;
      border-radius: 0.625rem;
      overflow: hidden;
      background: #0f172a;
    }

    .code-block-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0.5rem 0.75rem;
      background: rgba(255, 255, 255, 0.05);
      border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    }

    .code-block-lang {
      font-size: 0.7rem;
      font-weight: 600;
      color: #94a3b8;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }

    .btn-copy {
      padding: 0.15rem 0.5rem;
      font-size: 0.7rem;
      font-weight: 600;
      color: #94a3b8;
      background: rgba(255, 255, 255, 0.08);
      border: 1px solid rgba(255, 255, 255, 0.15);
      border-radius: 0.25rem;
      cursor: pointer;
      transition: all 0.2s ease;
    }

    .btn-copy:hover {
      color: #e2e8f0;
      background: rgba(255, 255, 255, 0.15);
    }

    .btn-copy.btn-success {
      color: #4ade80;
      background: rgba(74, 222, 128, 0.15);
      border-color: rgba(74, 222, 128, 0.3);
    }

    .code-block-content {
      margin: 0;
      padding: 0.875rem 1rem;
      background: transparent;
      color: #e2e8f0;
      font-size: 0.8rem;
      line-height: 1.6;
      overflow-x: auto;
      white-space: pre;
      word-break: normal;
      overflow-wrap: normal;
      max-width: 100%;
    }

    .code-block-content code {
      color: inherit;
      font-size: inherit;
      background: transparent;
    }

    /* 简单的语法高亮 */
    .language-xml .tag,
    .language-html .tag { color: #f472b6; }
    .language-xml .attr,
    .language-html .attr { color: #a78bfa; }
    .language-xml .value,
    .language-html .value { color: #4ade80; }

    .language-toml .key { color: #a78bfa; }
    .language-toml .value { color: #4ade80; }
    .language-toml .comment { color: #64748b; }

    .language-bash .command { color: #4ade80; }
    .language-bash .flag { color: #a78bfa; }
    .language-bash .string { color: #f472b6; }
  `]
})
export class CodeBlockComponent {
  @Input() code = '';
  @Input() lang = '';

  copied = false;

  private toast = inject(ToastService);

  onCopy(): void {
    if (!this.code) return;

    navigator.clipboard.writeText(this.code).then(() => {
      this.copied = true;
      this.toast.success('已复制到剪贴板');
      setTimeout(() => {
        this.copied = false;
      }, 2000);
    }).catch(() => {
      this.toast.error('复制失败，请手动复制');
    });
  }
}
