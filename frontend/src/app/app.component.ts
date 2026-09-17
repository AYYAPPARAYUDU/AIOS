import { Component, OnInit, ViewChild, HostListener } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { HudHeaderComponent } from './components/hud-header/hud-header.component';
import { HologramViewportComponent } from './components/hologram-viewport/hologram-viewport.component';
import { SystemDiagnosticsComponent } from './components/system-diagnostics/system-diagnostics.component';
import { ChatConsoleComponent } from './components/chat-console/chat-console.component';
import { MultiAgentSwarmComponent } from './components/multi-agent-swarm/multi-agent-swarm.component';
import { StorageVaultComponent } from './components/storage-vault/storage-vault.component';
import { ProcessManagerComponent } from './components/process-manager/process-manager.component';
import { TerminalRunnerComponent } from './components/terminal-runner/terminal-runner.component';
import { ContactsManagerComponent } from './components/contacts-manager/contacts-manager.component';
import { EvolutionGraphComponent } from './components/evolution-graph/evolution-graph.component';
import { PerceptionHudComponent } from './components/perception-hud/perception-hud.component';
import { MediaHubComponent } from './components/media-hub/media-hub.component';
import { HudFooterComponent } from './shared/hud-footer/hud-footer.component';
import { JarvisWebsocketService } from './core/services/jarvis-websocket.service';
import { JarvisApiService } from './core/services/jarvis-api.service';
import { JarvisAudioService } from './core/services/jarvis-audio.service';
import { SystemTelemetry } from './core/models/system.model';
import { AgentModel } from './core/models/agent.model';
import { environment } from '../environments/environment';

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
    ContactsManagerComponent,
    ProcessManagerComponent,
    TerminalRunnerComponent,
    EvolutionGraphComponent,
    PerceptionHudComponent,
    MediaHubComponent,
    HudFooterComponent
  ],
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit {
  public isSwarmDrawerOpen: boolean = false;
  public isToolsDrawerOpen: boolean = false;
  public isEvolutionModalOpen: boolean = false;
  public isPerceptionModalOpen: boolean = false;
  public isMediaModalOpen: boolean = false;
  public storageAlert: string | null = null;

  public activeTab: string = 'contacts';
  public mobileView: 'diagnostics' | 'chat' | 'swarm' = 'chat';
  public isMobile: boolean = false;
  public selectedAgentId: string = 'clone';
  public telemetry: SystemTelemetry | null = null;
  public agents: AgentModel[] = [];
  public isConnected: boolean = false;
  public netSpeedText: string = 'NET: ACTIVE';

  @ViewChild(ChatConsoleComponent) chatConsole!: ChatConsoleComponent;

  constructor(
    private wsService: JarvisWebsocketService,
    private apiService: JarvisApiService,
    private audioService: JarvisAudioService,
    private http: HttpClient
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

    this.checkStorageCapacity();
    setInterval(() => this.checkStorageCapacity(), 30000);

    this.audioService.playSciFiTone('boot');
  }

  checkStorageCapacity(): void {
    this.http.get<any>(`${environment.apiUrl}/storage/capacity`).subscribe({
      next: (cap) => {
        if (cap && cap.requires_user_expansion) {
          this.storageAlert = `⚠️ 200GB VAULT AT ${cap.usage_percentage}% (CLICK TO EXPAND)`;
        } else {
          this.storageAlert = null;
        }
      },
      error: () => {}
    });
  }

  onExpandStorage(): void {
    if (confirm('Expand local dynamic database storage vault by +100GB?')) {
      this.http.post<any>(`${environment.apiUrl}/storage/expand`, { additional_gb: 100.0 }).subscribe({
        next: (res) => {
          alert(res.message);
          this.checkStorageCapacity();
        }
      });
    }
  }

  toggleSwarmDrawer(): void {
    this.isSwarmDrawerOpen = !this.isSwarmDrawerOpen;
    if (this.isSwarmDrawerOpen) {
      this.closeModals();
      this.isToolsDrawerOpen = false;
    }
  }

  toggleToolsDrawer(): void {
    this.isToolsDrawerOpen = !this.isToolsDrawerOpen;
    if (this.isToolsDrawerOpen) {
      this.closeModals();
      this.isSwarmDrawerOpen = false;
    }
  }

  toggleEvolution(): void {
    this.isEvolutionModalOpen = !this.isEvolutionModalOpen;
    if (this.isEvolutionModalOpen) {
      this.isPerceptionModalOpen = false;
      this.isMediaModalOpen = false;
      this.isSwarmDrawerOpen = false;
      this.isToolsDrawerOpen = false;
    }
  }

  togglePerception(): void {
    this.isPerceptionModalOpen = !this.isPerceptionModalOpen;
    if (this.isPerceptionModalOpen) {
      this.isEvolutionModalOpen = false;
      this.isMediaModalOpen = false;
      this.isSwarmDrawerOpen = false;
      this.isToolsDrawerOpen = false;
    }
  }

  toggleMedia(): void {
    this.isMediaModalOpen = !this.isMediaModalOpen;
    if (this.isMediaModalOpen) {
      this.isEvolutionModalOpen = false;
      this.isPerceptionModalOpen = false;
      this.isSwarmDrawerOpen = false;
      this.isToolsDrawerOpen = false;
    }
  }

  closeModals(): void {
    this.isEvolutionModalOpen = false;
    this.isPerceptionModalOpen = false;
    this.isMediaModalOpen = false;
  }

  closeAllDrawers(): void {
    this.isSwarmDrawerOpen = false;
    this.isToolsDrawerOpen = false;
    this.closeModals();
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
    this.isSwarmDrawerOpen = false;
    if (this.chatConsole) {
      this.chatConsole.addSystemMessage('ABHI', `Switched active focus to deity agent [${agentId.toUpperCase()}].`);
    }
  }

  onScreenshotCaptured(previewBase64: string): void {
    if (this.chatConsole) {
      this.chatConsole.addSystemMessage('ABHI', 'Hanuman has captured the visual display buffer and preserved it in Saraswati vault.', previewBase64);
    }
  }

  onAppLaunched(appName: string): void {
    if (this.chatConsole) {
      this.chatConsole.addSystemMessage('ABHI', `Indra has summoned application ${appName.toUpperCase()} on your workstation.`);
    }
  }

  onAgentResponse(res: any): void {
    if (res.agents) {
      this.agents = res.agents;
    }
  }
}
