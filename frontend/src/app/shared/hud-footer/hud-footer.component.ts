import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-hud-footer',
  standalone: true,
  imports: [CommonModule],
  template: `
    <footer class="hud-footer">
      <div><i class="fas fa-shield-halved" style="color:var(--neon-emerald);"></i> DURGA CELESTIAL SHIELD: ACTIVE</div>
      <div><i class="fas fa-book-open" style="color:var(--neon-gold);"></i> SARASWATI 50GB VAULT ALLOCATED // MEMORY PERSISTED</div>
      <div><i class="fas fa-sun" style="color:var(--neon-gold);"></i> ABHI AIOS // OLLAMA QWEN3:8B LOCAL COGNITION</div>
    </footer>
  `,
  styles: [`
    .hud-footer {
      display: flex;
      align-items: center;
      justify-content: space-between;
      background: var(--bg-panel);
      border: 1px solid var(--border-gold);
      border-radius: 8px;
      padding: 4px 14px;
      font-family: var(--font-data);
      font-size: 11px;
      color: var(--text-dim);
      flex-wrap: wrap;
      gap: 6px;
      box-shadow: 0 0 10px rgba(0, 0, 0, 0.4);
    }
  `]
})
export class HudFooterComponent {}
