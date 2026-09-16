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
  private currentAudioElement: HTMLAudioElement | null = null;
  private pulseInterval: any = null;

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
    if (!this.speechEnabled) return;

    // Stop any currently playing audio
    if (this.currentAudioElement) {
      this.currentAudioElement.pause();
      this.currentAudioElement = null;
    }
    if ('speechSynthesis' in window) {
      window.speechSynthesis.cancel();
    }
    if (this.pulseInterval) {
      clearInterval(this.pulseInterval);
      this.pulseInterval = null;
    }

    const cleanText = text
      .replace(/```[\s\S]*?```/g, 'Code block omitted.')
      .replace(/`([^`]+)`/g, '$1')
      .replace(/[*_#]/g, '')
      .replace(/[\u{1F300}-\u{1F9FF}]/gu, '')
      .trim();

    if (!cleanText) return;

    // 1. Try Neural Lady Voice via backend streaming
    const ttsUrl = `/api/voice/tts?text=${encodeURIComponent(cleanText)}&voice=en-IN-NeerjaNeural`;
    const audio = new Audio(ttsUrl);
    this.currentAudioElement = audio;

    this.avatarState$.next('speaking');
    this.pulseInterval = setInterval(() => {
      const freq = 0.3 + Math.random() * 0.7;
      this.audioIntensity$.next(freq);
    }, 60);

    audio.onplay = () => {
      this.avatarState$.next('speaking');
    };

    audio.onended = () => {
      if (this.pulseInterval) clearInterval(this.pulseInterval);
      this.audioIntensity$.next(0);
      this.avatarState$.next('idle');
      this.currentAudioElement = null;
    };

    audio.onerror = () => {
      // Fallback to browser SpeechSynthesis with Lady voice
      this.fallbackBrowserSpeech(cleanText);
    };

    audio.play().catch(() => {
      this.fallbackBrowserSpeech(cleanText);
    });
  }

  private fallbackBrowserSpeech(cleanText: string) {
    if (!('speechSynthesis' in window)) {
      if (this.pulseInterval) clearInterval(this.pulseInterval);
      this.audioIntensity$.next(0);
      this.avatarState$.next('idle');
      return;
    }

    const utterance = new SpeechSynthesisUtterance(cleanText);
    utterance.rate = 1.0;
    utterance.pitch = 1.08; // Pleasant celestial lady tone

    const voices = window.speechSynthesis.getVoices();
    // Prioritize female / lady voices (Zira, Neerja, Aria, Jenny, Sonia, Samantha, Female)
    const ladyVoice = voices.find(v => 
      v.name.includes('Zira') ||
      v.name.includes('Neerja') ||
      v.name.includes('Aria') ||
      v.name.includes('Jenny') ||
      v.name.includes('Sonia') ||
      v.name.includes('Samantha') ||
      v.name.includes('Female') ||
      v.name.includes('Google UK English Female') ||
      v.name.includes('Google US English Female')
    );
    if (ladyVoice) utterance.voice = ladyVoice;

    utterance.onend = () => {
      if (this.pulseInterval) clearInterval(this.pulseInterval);
      this.audioIntensity$.next(0);
      this.avatarState$.next('idle');
    };

    utterance.onerror = () => {
      if (this.pulseInterval) clearInterval(this.pulseInterval);
      this.audioIntensity$.next(0);
      this.avatarState$.next('idle');
    };

    window.speechSynthesis.speak(utterance);
  }
}
