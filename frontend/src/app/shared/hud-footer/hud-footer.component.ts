import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-hud-footer',
  standalone: true,
  imports: [CommonModule],
  template: `
    <footer class="hud-footer">
      <div><i class="fas fa-shield-halved" style="color:var(--neon-green);"></i> SECURITY PROTOCOLS: ACTIVE</div>
      <div>50GB LOCAL STORAGE ALLOCATED // MEMORY PERSISTED</div>
      <div>ANGULAR 19+ CORE // FASTAPI + OLLAMA QWEN3:8B</div>
    </footer>
  `,
  styles: [`
    .hud-footer {
      display: flex;
      align-items: center;
      justify-content: space-between;
      background: var(--bg-panel);
      border: 1px solid var(--border-cyan);
      border-radius: 6px;
      padding: 4px 14px;
      font-family: var(--font-data);
      font-size: 11px;
      color: var(--text-dim);
      flex-wrap: wrap;
      gap: 6px;
    }
  `]
})
export class HudFooterComponent {}
