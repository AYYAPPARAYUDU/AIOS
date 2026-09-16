import { Component, Input, Output, EventEmitter } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { SystemTelemetry } from '../../core/models/system.model';
import { JarvisApiService } from '../../core/services/jarvis-api.service';
import { JarvisAudioService } from '../../core/services/jarvis-audio.service';

@Component({
  selector: 'app-system-diagnostics',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './system-diagnostics.component.html',
  styleUrls: ['./system-diagnostics.component.scss']
})
export class SystemDiagnosticsComponent {
  @Input() telemetry: SystemTelemetry | null = null;
  @Output() appLaunched = new EventEmitter<string>();

  public volumeLevel: number = 70;
  public brightnessLevel: number = 80;

  constructor(
    private apiService: JarvisApiService,
    private audioService: JarvisAudioService
  ) {}

  onVolumeChange(val: number): void {
    this.volumeLevel = val;
    this.apiService.setVolume(val).subscribe();
  }

  onBrightnessChange(val: number): void {
    this.brightnessLevel = val;
    this.apiService.setBrightness(val).subscribe();
  }

  launchApp(app: string): void {
    this.audioService.playSciFiTone('ack');
    this.apiService.launchApp(app).subscribe();
    this.appLaunched.emit(app);
  }
}
