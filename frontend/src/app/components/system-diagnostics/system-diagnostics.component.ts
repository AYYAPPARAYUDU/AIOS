import { Component, Input, Output, EventEmitter, OnInit } from '@angular/core';
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
export class SystemDiagnosticsComponent implements OnInit {
  @Input() telemetry: SystemTelemetry | null = null;
  @Output() appLaunched = new EventEmitter<string>();
  @Output() macroTriggered = new EventEmitter<string>();

  public volumeLevel: number = 50;
  public isMuted: boolean = false;
  public brightnessLevel: number = 70;
  public macroStatusText: string = '';

  constructor(
    private apiService: JarvisApiService,
    private audioService: JarvisAudioService
  ) {}

  ngOnInit(): void {
    this.refreshHardwareState();
  }

  refreshHardwareState(): void {
    this.apiService.getSystemState().subscribe({
      next: (state) => {
        if (state) {
          if (state.volume !== undefined) this.volumeLevel = state.volume;
          if (state.muted !== undefined) this.isMuted = state.muted;
          if (state.brightness !== undefined) this.brightnessLevel = state.brightness;
        }
      },
      error: () => {}
    });
  }

  onVolumeChange(val: number): void {
    this.volumeLevel = Number(val);
    this.isMuted = false;
    this.apiService.setVolume(this.volumeLevel).subscribe();
  }

  toggleMute(): void {
    this.audioService.playSciFiTone('beep');
    this.isMuted = !this.isMuted;
    this.apiService.setVolume(null, this.isMuted).subscribe();
  }

  onBrightnessChange(val: number): void {
    this.brightnessLevel = Number(val);
    this.apiService.setBrightness(this.brightnessLevel).subscribe();
  }

  launchApp(app: string): void {
    this.audioService.playSciFiTone('ack');
    this.apiService.launchApp(app).subscribe();
    this.appLaunched.emit(app);
  }

  runMacro(macroName: string): void {
    this.audioService.playSciFiTone('boot');
    this.macroStatusText = `Executing ${macroName.toUpperCase()} protocol...`;
    this.apiService.runMacro(macroName).subscribe({
      next: (res) => {
        this.macroStatusText = res.status || `Protocol ${macroName} engaged.`;
        this.macroTriggered.emit(macroName);
      },
      error: () => {
        this.macroStatusText = `Error running ${macroName}.`;
      }
    });
  }

  purgeRam(): void {
    this.audioService.playSciFiTone('ack');
    this.macroStatusText = 'Purging working set memory...';
    this.apiService.purgeRam().subscribe({
      next: (res) => {
        this.macroStatusText = `Optimized ${res.processes_optimized} processes. RAM: ${res.current_memory_percent}%.`;
      },
      error: () => {
        this.macroStatusText = 'RAM purge completed.';
      }
    });
  }
}
