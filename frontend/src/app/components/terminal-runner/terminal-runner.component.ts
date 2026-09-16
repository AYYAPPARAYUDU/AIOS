import { Component, ViewChild, ElementRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { JarvisApiService } from '../../core/services/jarvis-api.service';
import { JarvisAudioService } from '../../core/services/jarvis-audio.service';

@Component({
  selector: 'app-terminal-runner',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './terminal-runner.component.html',
  styleUrls: ['./terminal-runner.component.scss']
})
export class TerminalRunnerComponent {
  @ViewChild('termScroll') private termScrollContainer!: ElementRef;

  public commandInput: string = '';
  public terminalLines: { text: string; isError?: boolean }[] = [
    { text: 'JARVIS PowerShell Direct Console [Win32 Native Engine]' },
    { text: 'Type any PowerShell command and hit Enter...' }
  ];

  constructor(
    private apiService: JarvisApiService,
    private audioService: JarvisAudioService
  ) {}

  execute(): void {
    const cmd = this.commandInput.trim();
    if (!cmd) return;

    this.audioService.playSciFiTone('beep');
    this.terminalLines.push({ text: `PS > ${cmd}` });
    this.commandInput = '';
    this.audioService.avatarState$.next('executing');

    this.apiService.executeTerminal(cmd).subscribe({
      next: (res) => {
        this.audioService.avatarState$.next('idle');
        if (res.stdout) {
          this.terminalLines.push({ text: res.stdout });
        }
        if (res.stderr) {
          this.terminalLines.push({ text: `ERROR: ${res.stderr}`, isError: true });
        }
        this.terminalLines.push({ text: `[Process exit: ${res.exit_code} | ${res.duration_ms}ms]` });
        this.scrollToBottom();
      },
      error: (err) => {
        this.audioService.avatarState$.next('idle');
        this.terminalLines.push({ text: `EXECUTION FAILURE: ${err.message}`, isError: true });
        this.scrollToBottom();
      }
    });
  }

  private scrollToBottom(): void {
    setTimeout(() => {
      if (this.termScrollContainer) {
        this.termScrollContainer.nativeElement.scrollTop = this.termScrollContainer.nativeElement.scrollHeight;
      }
    }, 50);
  }
}
