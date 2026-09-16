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
  @Input() selectedAgentId: string = 'supervisor';
  @Output() agentSelected = new EventEmitter<string>();

  constructor(private audioService: JarvisAudioService) {}

  selectAgent(agentId: string): void {
    this.audioService.playSciFiTone('ack');
    this.agentSelected.emit(agentId);
  }
}
