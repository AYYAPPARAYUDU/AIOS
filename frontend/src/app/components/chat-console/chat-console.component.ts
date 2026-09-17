import { Component, Input, Output, EventEmitter, ViewChild, ElementRef, AfterViewChecked, OnInit, OnDestroy } from '@angular/core';
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
export class ChatConsoleComponent implements OnInit, AfterViewChecked, OnDestroy {
  @Input() selectedAgent: string = 'abhi';
  @Output() agentResponse = new EventEmitter<any>();
  @ViewChild('chatScroll') private chatScrollContainer!: ElementRef<HTMLDivElement>;

  public messages: ChatMessage[] = [
    {
      sender: 'ABHI',
      role: 'assistant',
      content: 'JARVIS AIOS Neural Core and AI Clone Swarm initialized. All native OS controllers, 200GB dynamic knowledge vault, and ML clone learning loops are active.'
    }
  ];
  public inputText: string = '';
  public sessionStatusMessage: string = '';
  public isUserScrolledUp: boolean = false;
  public unreadWhileScrolled: number = 0;
  private shouldScrollToBottom: boolean = true;
  private subs: Subscription[] = [];

  constructor(
    private apiService: JarvisApiService,
    public audioService: JarvisAudioService
  ) {}

  ngOnInit(): void {
    this.subs.push(
      this.audioService.speechResult$.subscribe(transcript => {
        this.inputText = transcript;
        this.sendMessage();
      })
    );
  }

  ngOnDestroy(): void {
    this.subs.forEach(s => s.unsubscribe());
  }

  ngAfterViewChecked(): void {
    if (this.shouldScrollToBottom) {
      this.forceScrollToBottom();
    }
  }

  public onScroll(): void {
    if (!this.chatScrollContainer) return;
    const el = this.chatScrollContainer.nativeElement;
    const threshold = 80;
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight <= threshold;

    if (atBottom) {
      this.isUserScrolledUp = false;
      this.shouldScrollToBottom = true;
      this.unreadWhileScrolled = 0;
    } else {
      this.isUserScrolledUp = true;
      this.shouldScrollToBottom = false;
    }
  }

  public scrollToBottomSmooth(): void {
    this.isUserScrolledUp = false;
    this.shouldScrollToBottom = true;
    this.unreadWhileScrolled = 0;
    this.forceScrollToBottom(true);
  }

  private forceScrollToBottom(smooth: boolean = false): void {
    try {
      if (this.chatScrollContainer) {
        const el = this.chatScrollContainer.nativeElement;
        if (smooth) {
          el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' });
        } else {
          el.scrollTop = el.scrollHeight;
        }
        this.shouldScrollToBottom = false;
      }
    } catch (err) {}
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
          this.sessionStatusMessage = `Session archived to Knowledge Vault: ${res.file}`;
          this.audioService.speak('Current session transcript has been safely archived in the Knowledge Vault.');
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
              content: 'Memory buffer cleared for new session. All divine deities and AI Clone stand ready.'
            }
          ];
          this.sessionStatusMessage = 'Session memory cleared successfully.';
          this.shouldScrollToBottom = true;
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
    this.shouldScrollToBottom = true;
    this.audioService.avatarState$.next('thinking');

    this.apiService.chatWithAbhi(text, this.selectedAgent).subscribe({
      next: (res) => {
        this.audioService.avatarState$.next('idle');
        const answer = res.response || 'Task executed with absolute precision.';
        const allTools = res.tool_calls || (res.tool_call ? [res.tool_call] : []);
        
        // Execute client-side action bridges if web URLs or app links were returned
        if (res.client_actions && Array.isArray(res.client_actions)) {
          for (const action of res.client_actions) {
            if (action.type === 'open_url' && action.url) {
              try {
                window.open(action.url, '_blank');
              } catch (e) {}
            }
          }
        }

        this.messages.push({
          sender: this.selectedAgent === 'clone' ? 'MY CLONE' : 'ABHI',
          role: 'assistant',
          content: answer,
          toolCall: res.tool_call,
          toolCalls: allTools
        });

        if (this.isUserScrolledUp) {
          this.unreadWhileScrolled++;
        } else {
          this.shouldScrollToBottom = true;
        }

        this.audioService.speak(answer);
        this.agentResponse.emit(res);
      },
      error: (err) => {
        this.audioService.avatarState$.next('idle');
        this.audioService.playSciFiTone('error');
        this.messages.push({
          sender: 'ABHI',
          role: 'assistant',
          content: `Controller anomaly: ${err.message || 'Check server connection'}`
        });
        if (this.isUserScrolledUp) {
          this.unreadWhileScrolled++;
        } else {
          this.shouldScrollToBottom = true;
        }
      }
    });
  }

  public openActionLink(tc: any): void {
    this.audioService.playSciFiTone('ack');
    const url = tc.result?.app_target || tc.result?.target || tc.result?.url || tc.result?.client_action?.url;
    if (url) {
      if (url.startsWith('whatsapp:') || url.startsWith('whatsapp://')) {
        window.location.href = url;
      } else {
        window.open(url, '_blank');
      }
    }
    // Also trigger backend OS controller for native focus
    if (tc.tool === 'open_whatsapp') {
      const recipient = tc.parameters?.phone || tc.result?.recipient;
      const message = tc.parameters?.message || tc.result?.message;
      this.apiService.sendWhatsApp(recipient, message).subscribe();
    }
  }

  public copyMessageText(text: string): void {
    if (!text) return;
    this.audioService.playSciFiTone('beep');
    navigator.clipboard.writeText(text).then(() => {
      this.sessionStatusMessage = '📋 Message copied to clipboard!';
      setTimeout(() => this.sessionStatusMessage = '', 3000);
    }).catch(() => {
      this.sessionStatusMessage = 'Could not copy to clipboard.';
      setTimeout(() => this.sessionStatusMessage = '', 3000);
    });
  }

  public addSystemMessage(sender: string, content: string, imagePreview: string | null = null): void {
    this.messages.push({
      sender,
      role: 'assistant',
      content,
      imagePreview
    });
    if (this.isUserScrolledUp) {
      this.unreadWhileScrolled++;
    } else {
      this.shouldScrollToBottom = true;
    }
  }
}
