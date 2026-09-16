import { Component, OnInit, ViewChild, HostListener } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HudHeaderComponent } from './components/hud-header/hud-header.component';
import { HologramViewportComponent } from './components/hologram-viewport/hologram-viewport.component';
import { SystemDiagnosticsComponent } from './components/system-diagnostics/system-diagnostics.component';
import { ChatConsoleComponent } from './components/chat-console/chat-console.component';
import { MultiAgentSwarmComponent } from './components/multi-agent-swarm/multi-agent-swarm.component';
import { StorageVaultComponent } from './components/storage-vault/storage-vault.component';
import { ProcessManagerComponent } from './components/process-manager/process-manager.component';
import { TerminalRunnerComponent } from './components/terminal-runner/terminal-runner.component';
import { HudFooterComponent } from './shared/hud-footer/hud-footer.component';
import { JarvisWebsocketService } from './core/services/jarvis-websocket.service';
import { JarvisApiService } from './core/services/jarvis-api.service';
import { JarvisAudioService } from './core/services/jarvis-audio.service';
import { SystemTelemetry } from './core/models/system.model';
import { AgentModel } from './core/models/agent.model';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [
    CommonModule,
    HudHeaderComponent,
    HologramViewportComponent,
    SystemDiagnosticsComponent,
    ChatConsoleComponent,
    MultiAgentSwarmComponent,
    StorageVaultComponent,
    ProcessManagerComponent,
    TerminalRunnerComponent,
    HudFooterComponent
  ],
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit {
  public activeTab: string = 'agents';
  public mobileView: 'diagnostics' | 'chat' | 'swarm' = 'chat';
  public isMobile: boolean = false;
  public selectedAgentId: string = 'supervisor';
  public telemetry: SystemTelemetry | null = null;
  public agents: AgentModel[] = [];
  public isConnected: boolean = false;
  public netSpeedText: string = 'NET: ACTIVE';

  @ViewChild(ChatConsoleComponent) chatConsole!: ChatConsoleComponent;

  constructor(
    private wsService: JarvisWebsocketService,
    private apiService: JarvisApiService,
    private audioService: JarvisAudioService
  ) {
    this.checkScreenSize();
  }

  @HostListener('window:resize')
  onResize(): void {
    this.checkScreenSize();
  }

  private checkScreenSize(): void {
    this.isMobile = window.innerWidth < 992;
  }

  ngOnInit(): void {
    this.wsService.isConnected$.subscribe(c => this.isConnected = c);
    this.wsService.telemetry$.subscribe(t => {
      if (t) {
        this.telemetry = t;
        if (t.network) {
          this.netSpeedText = `DN: ${t.network.download_speed_kbps} KB/s | UP: ${t.network.upload_speed_kbps} KB/s`;
        }
      }
    });
    this.wsService.agents$.subscribe(a => {
      if (a && a.length > 0) this.agents = a;
    });

    this.apiService.getTelemetry().subscribe(t => this.telemetry = t);
    this.apiService.getAgentStatus().subscribe(res => {
      if (res.agents) this.agents = res.agents;
    });

    this.audioService.playSciFiTone('boot');
  }

  setTab(tab: string): void {
    this.audioService.playSciFiTone('beep');
    this.activeTab = tab;
  }

  setMobileView(view: 'diagnostics' | 'chat' | 'swarm'): void {
    this.audioService.playSciFiTone('beep');
    this.mobileView = view;
  }

  onAgentSelected(agentId: string): void {
    this.selectedAgentId = agentId;
    if (this.chatConsole) {
      this.chatConsole.addSystemMessage('JARVIS', `Routing requests to [${agentId.toUpperCase()}], sir.`);
    }
  }

  onScreenshotCaptured(previewBase64: string): void {
    if (this.chatConsole) {
      this.chatConsole.add