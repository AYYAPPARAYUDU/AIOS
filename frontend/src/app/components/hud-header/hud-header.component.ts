import { Component, Input, Output, EventEmitter } from '@angular/core';
import { CommonModule } from '@angular/common';
import { JarvisAudioService } from '../../core/services/jarvis-audio.service';
import { JarvisApiService } from '../../core/services/jarvis-api.service';

@Component({
  selector: 'app-hud-header',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './hud-header.component.html',
  styleUrls: ['./hud-header.component.scss']
})
export class HudHeaderComponent {
  @Input() isConnected: boolean = false;
  @Input() netSpeedText: string = 'NET: ACTIVE';
  @Input() isSwarmOpen: boolean = false;
  @Input() isToolsOpen: boolean = false;
  @Output() screenshotCaptured = new EventEmitter<string>();
  @Output() toggleSwarm = new EventEmitter<void>();
  @Output() toggleTools = new EventEmitter<void>();

  constructor(
    public audioService: JarvisAudioService,
    private apiService: JarvisApiService
  ) {}

  onToggleSwarm(): void {
    this.audioService.playSciFiTone('beep');
    this.toggleSwarm.emit();
  }

  onToggleTools(): void {
    this.audioService.playSciFiTone('beep');
    this.toggleTools.emit();
  }

  toggleVoice(): void {
    this.audioService.speechEnabled = !this.audioService.speechEnabled;
    if (this.audioService.speechEnabled) {
      this.audioService.playSciFiTone('ack');
    } else {
      if ('speechSynthesis' in window) window.speechSynthesis.cancel();
    }
  }

  takeScreenshot(): void {
    this.audioService.playSciFiTone('ack');
    this.apiService.takeScreenshot().subscribe(res => {
      if (res.status === 'success') {
        this.screenshotCaptured.emit(res.preview_base64);
      }
    });
  }

  lockWorkstation(): void {
    if (confirm('Lock host workstation protocols?')) {
      this.audioService.playSciFiTone('ack');
      this.apiService.powerAction('lock').subscribe();
    }
  }
}
