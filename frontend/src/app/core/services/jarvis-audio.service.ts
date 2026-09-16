import { Injectable } from '@angular/core';
import { Subject } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class JarvisAudioService {
  public speechEnabled = true;
  public isListening = false;
  
  public speechResult$ = new Subject<string>();
  public audioIntensity$ = new Subject<number>();
  public avatarState$ = new Subject<string>();

  private recognition: any = null;
  private audioCtx: AudioContext | null = null;

  constructor() {
    this.initAudioContext();
    this.initSpeechRecognition();
  }

  private initAudioContext() {
    try {
      const AudioCtx = (window as any).AudioContext || (window as any).webkitAudioContext;
      if (AudioCtx) {
        this.audioCtx = new AudioCtx();
      }
    } catch (e) {}
  }

  public playSciFiTone(type: 'beep' | 'boot' | 'ack' | 'error' = 'beep') {
    if (!this.audioCtx) return;
    try {
      if (this.audioCtx.state === 'suspended') {
        this.audioCtx.resume();
      }
      const osc = this.audioCtx.createOscillator();
      const gain = this.audioCtx.createGain();
      osc.connect(gain);
      gain.connect(this.audioCtx.destination);

      const now = this.audioCtx.currentTime;
      if (type === 'boot') {
        osc.type = 'sine';
        osc.frequency.setValueAtTime(440, now);
        osc.frequency.exponentialRampToValueAtTime(880, now + 0.2);
        gain.gain.setValueAtTime(0.15, now);
        gain.gain.linearRampToValueAtTime(0.01, now + 0.3);
        osc.start(now);
        osc.stop(now + 0.3);
      } else if (type === 'ack') {
        osc.type = 'sine';
        osc.frequency.setValueAtTime(600, now);
        osc.frequency.setValueAtTime(900, now + 0.08);
        gain.gain.setValueAtTime(0.1, now);
        gain.gain.linearRampToValueAtTime(0.01, now + 0.18);
        osc.start(now);
        osc.stop(now + 0.18);
      } else if (type === 'error') {
        osc.type = 'sawtooth';
        osc.frequency.setValueAtTime(250, now);
        osc.frequency.linearRampToValueAtTime(150, now + 0.2);
        gain.gain.setValueAtTime(0.12, now);
        gain.gain.linearRampToValueAtTime(0.01, now + 0.25);
        osc.start(now);
        osc.stop(now + 0.25);
      } else {
        osc.type = 'sine';
        osc.frequency.setValueAtTime(750, now);
        gain.gain.setValueAtTime(0.08, now);
        gain.gain.linearRampToValueAtTime(0.01, now + 0.08);
        osc.start(now);
        osc.stop(now + 0.08);
      }
    } catch (e) {}
  }

  private initSpeechRecognition() {
    const SpeechRecognition = (window as any).SpeechRecognition || (window as any).webkitSpeechRecognition;
    if (!SpeechRecognition) return;

    this.recognition = new SpeechRecognition();
    this.recognition.continuous = false;
    this.recognition.interimResults = false;
    this.recognition.lang = 'en-US';

    this.recognition.onstart = () => {
      this.isListening = true;
      this.playSciFiTone('ack');
      this.avatarState$.next('listening');
    };

    this.recognition.onresult = (event: any) => {
      const transcript = event.results[0][0].transcript;
      this.speechResult$.next(transcript);
    };

    this.recognition.onerror = () => {
      this.stopListening();
    };

    this.recognition.onend = () => {
      this.stopListening();
    };
  }

  public startListening() {
    if (this.recognition && !this.isListening) {
      try {
        this.recognition.start();
      } catch (e) {}
    }
  }

  public stopListening() {
    this.isListening = false;
    if (this.recognition) {
      try { this.recognition.stop(); } catch (e) {}
    }
    this.avatarState$.next('idle');
  }

  public speak(text: string) {
    if (!this.speechEnabled || !('speechSynthesis' in window)) return;

    window.speechSynthesis.cancel();

    const cleanText = text
      .replace(/```[\s\S]*?```/g, 'Code block omitted.')
      .replace(/`([^`]+)`/g, '$1')
      .replace(/[*_#]/g, '');

    const utterance = new SpeechSynthesisUtterance(cleanText);
    utterance.rate = 1.02;
    utterance.pitch = 0.94;

    const voices = window.speechSynthesis.getVoices();
    const jarvisVoice = voices.find(v => 
      v.name.includes('UK English Male') || 
      v.name.includes('George') || 
      v.name.includes('Oliver') ||
      v.name.includes('Daniel') ||
      v.name.includes('Arthur') ||
      v.lang.includes('en-GB') ||
      v.name.includes('David')
    );
    if (jarvisVoice) utterance.voice = jarvisVoice;

    let pulseInterval: any = null;

    utterance.onstart = () => {
      this.avatarState$.next('speaking');
      pulseInterval = setInterval(() => {
        const freq = 0.25 + Math.random() * 0.75;
        this.audioIntensity$.next(freq);
      }, 70);
    };

    utterance.onend = () => {
      if (pulseInterval) clearInterval(pulseInterval);
      this.audioIntensity$.next(0);
      this.avatarState$.next('idle');
    };

    utterance.onerror = () => {
      if (pulseInterval) clearInterval(pulseInterval);
      this.audioIntensity$.next(0);
      this.avatarState$.next('idle');
    };

    window.speechSynthesis.speak(utterance);
  }
}
