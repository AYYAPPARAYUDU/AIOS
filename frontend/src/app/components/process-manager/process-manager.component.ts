import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ProcessItem } from '../../core/models/system.model';
import { JarvisApiService } from '../../core/services/jarvis-api.service';
import { JarvisAudioService } from '../../core/services/jarvis-audio.service';

@Component({
  selector: 'app-process-manager',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './process-manager.component.html',
  styleUrls: ['./process-manager.component.scss']
})
export class ProcessManagerComponent implements OnInit {
  public processes: ProcessItem[] = [];

  constructor(
    private apiService: JarvisApiService,
    private audioService: JarvisAudioService
  ) {}

  ngOnInit(): void {
    this.refresh();
  }

  refresh(): void {
    this.apiService.getProcesses(25).subscribe(res => {
      this.processes = res.processes || [];
    });
  }

  kill(pid: number): void {
    if (confirm(`Terminate process PID ${pid}?`)) {
      this.audioService.playSciFiTone('ack');
      this.apiService.killProcess(pid).subscribe(() => this.refresh());
    }
  }
}
