import { Component, Input, Output, EventEmitter, ViewChild, ElementRef, AfterViewChecked } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ChatMessage } from '../../core/models/chat.model';
import { JarvisApiService } from '../../core/services/jarvis-api.service';
import { JarvisAudioService } from '../../core/services/jarvis-audio.service';
import { Subscription } from 'rxjs';

@Component({
  selector: 'app-chat-console',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './chat-console.component.html',
  styleUrls: ['./chat-console.component.scss']
})
export class ChatConsoleComponent implements AfterViewChecked {
  @Input() selectedAgent: string = 'abhi';
  @Output() agentResponse = new EventEmitter<any>();
  @ViewChild('chatScroll') private chatScrollContainer!: ElementRef;

  public messages: ChatMessage[] = [
    {
      sender: 'ABHI',
      role: 'assistant',
      content: 'I am ABHI, your Supreme Cognitive Orchestrator. The Puranic deity swarm (Indra, Saraswati, Narada, Hanuman, Lakshmi, Durga, Vishwakarma) stands ready to execute your command with divine precision.'
    }
  ];
  public inputText: string = '';
  public sessionStatusMessage: string = '';
  private subs: Subscription[] = [];

  constructor(
    private apiService: JarvisApiService,
    public audioService: JarvisAudioService
  ) {
    this.subs.push(
      this.audioService.speechResult$.subscribe(transcript => {
        this.inputText = transcript;
        this.sendMessage();
      })
    );
  }

  ngAfterViewChecked(): void {
    this.scrollToBottom();
  }

  toggleMic(): void {
    if (this.audioService.isListening) {
      this.audioService.stopListening();
    } else {
      this.audioService.startListening();
    }
  }

  saveSessionToVault(): void {
    this.audioService.playSciFiTone('beep');
    this.apiService.exportSession('main_session').subscribe({
      next: (res) => {
        if (res.status === 'success') {
          this.sessionStatusMessage = `Session archived to 50GB Vault: ${res.file}`;
          this.audioService.speak('Current session transcript has been safely archived in the Saraswati Knowledge Vault.');
        } else {
          this.sessionStatusMessage = 'No messages to archive yet.';
        }
        setTimeout(() => this.sessionStatusMessage = '', 5000);
      },
      error: (err) => {
        this.sessionStatusMessage = `Error saving session: ${err.message}`;
        setTimeout(() => this.sessionStatusMessage = '', 5000);
      }
    });
  }

  clearSessionMemory(): void {
    if (confirm('Clear current session memory?')) {
      this.audioService.playSciFiTone('beep');
      this.apiService.clearSession('main_session').subscribe({
        next: () => {
          this.messages = [
            {
              sender: 'ABHI',
              role: 'assistant',
              content: 'Memory cleared for new session. All divine deities stand ready for your command.'
            }
          ];
          this.sessionStatusMessage = 'Session memory cleared successfully.';
          setTimeout(() => this.sessionStatusMessage = '', 4000);
        }
      });
    }
  }

  sendQuickPrompt(promptText: string): void {
    this.inputText = promptText;
    this.sendMessage();
  }

  sendMessage(): void {
    const text = this.inputText.trim();
    if (!text) return;

    this.audioService.playSciFiTone('beep');
    this.messages.push({ sender: 'User', role: 'user', content: text });
    this.inputText = '';
    this.audioService.avatarState$.next('thinking');


    this.apiService.chatWithAbhi(text, this.selectedAgent).subscribe({
      next: (res) => {
        this.audioService.avatarState$.next('idle');
        const answer = res.response || 'Divine task completed with absolute precision.';
        const allTools = res.tool_calls || (res.tool_call ? [res.tool_call] : []);
        this.messages.push({
          sender: 'ABHI',
          role: 'assistant',
          content: answer,
          toolCall: res.tool_call,
          toolCalls: allTools
        });
        this.audioService.speak(answer);
        this.agentResponse.emit(res);
      },
      error: (err) => {

        this.audioService.avatarState$.next('idle');
        this.audioService.playSciFiTone('error');
        this.messages.push({
          sender: 'ABHI',
          role: 'assistant',
          content: `Celestial anomaly encountered: ${err.message}`
        });
      }
    });
  }

  public addSystemMessage(sender: string, content: string, imagePreview: string | null = null): void {
    this.messages.push({
      sender,
      role: 'assistant',
      content,
      imagePreview
    });
  }

  private scrollToBottom(): void {
    try {
      if (this.chatScrollContainer) {
        this.chatScrollContainer.nativeElement.scrollTop = this.chatScrollContainer.nativeElement.scrollHeight;
      }
    } catch (err) {}
  }
}
