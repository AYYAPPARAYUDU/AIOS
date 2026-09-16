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
  @Input() selectedAgent: string = 'supervisor';
  @Output() agentResponse = new EventEmitter<any>();
  @ViewChild('chatScroll') private chatScrollContainer!: ElementRef;

  public messages: ChatMessage[] = [
    {
      sender: 'JARVIS',
      role: 'assistant',
      content: 'Online and ready, sir. All systems, 50GB storage vault, and OS controllers are active.'
    }
  ];
  public inputText: string = '';
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

  sendMessage(): void {
    const text = this.inputText.trim();
    if (!text) return;

    this.audioService.playSciFiTone('beep');
    this.messages.push({ sender: 'User', role: 'user', content: text });
    this.inputText = '';
    this.audioService.avatarState$.next('thinking');

    this.apiService.chatWithJarvis(text, this.selectedAgent).subscribe({
      next: (res) => {
        this.audioService.avatarState$.next('idle');
        const answer = res.response || 'Task executed successfully, sir.';
        this.messages.push({
          sender: 'JARVIS',
          role: 'assistant',
          content: answer,
          toolCall: res.tool_call
        });
        this.audioService.speak(answer);
        this.agentResponse.emit(res);
      },
      error: (err) => {
        this.audioService.avatarState$.next('idle');
        this.audioService.playSciFiTone('error');
        this.messages.push({
          sender: 'JARVIS',
          role: 'assistant',
          content: `An anomaly occurred: ${err.message}`
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
