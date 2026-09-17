import { Component, Input, Output, EventEmitter } from '@angular/core';
import { CommonModule } from '@angular/common';
import { AgentModel } from '../../core/models/agent.model';
import { JarvisAudioService } from '../../core/services/jarvis-audio.service';

@Component({
  selector: 'app-multi-agent-swarm',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './multi-agent-swarm.component.html',
  styleUrls: ['./multi-agent-swarm.component.scss']
})
export class MultiAgentSwarmComponent {
  @Input() agents: AgentModel[] = [];
  @Input() selectedAgentId: string = 'abhi';
  @Output() agentSelected = new EventEmitter<string>();

  constructor(private audioService: JarvisAudioService) {}

  selectAgent(agentId: string): void {
    this.audioService.playSciFiTone('ack');
    this.agentSelected.emit(agentId);
  }

  getDeityIcon(agentId: string): string {
    switch (agentId.toLowerCase()) {
      case 'clone':
        return 'fa-fingerprint';
      case 'abhi':
      case 'supervisor':
        return 'fa-crown';
      case 'indra':
      case 'os_controller':
        return 'fa-bolt-lightning';
      case 'saraswati':
      case 'storage_agent':
        return 'fa-book-open';
      case 'narada':
      case 'researcher':
        return 'fa-globe';
      case 'hanuman':
      case 'automation_agent':
        return 'fa-fire';
      case 'lakshmi':
      case 'resource_agent':
        return 'fa-heart';
      case 'durga':
      case 'security_agent':
        return 'fa-shield-halved';
      case 'vishwakarma':
      case 'architect_agent':
        return 'fa-cubes';
      default:
        return 'fa-user-astronaut';
    }
  }
}
