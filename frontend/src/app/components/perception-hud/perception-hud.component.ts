import { Component, OnInit, OnDestroy, ViewChild, ElementRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../../environments/environment';

@Component({
  selector: 'app-perception-hud',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './perception-hud.component.html',
  styleUrls: ['./perception-hud.component.scss']
})
export class PerceptionHudComponent implements OnInit, OnDestroy {
  @ViewChild('videoPlayer') videoPlayer!: ElementRef<HTMLVideoElement>;
  @ViewChild('canvasOverlay') canvasOverlay!: ElementRef<HTMLCanvasElement>;
  @ViewChild('micWaveform') micWaveform!: ElementRef<HTMLCanvasElement>;

  cameraActive = false;
  micActive = false;
  mediaStream: MediaStream | null = null;
  audioCtx: AudioContext | null = null;
  analyser: AnalyserNode | null = null;
  animationFrameId: number | null = null;

  perceptionState: any = {
    dominant_emotion: 'Deep Focus',
    emotion_scores: {
      'Deep Focus': 0.88,
      'Calm': 0.72,
      'Curiosity': 0.65,
      'Joy': 0.40,
      'Fatigue': 0.12,
      'Stress': 0.08
    },
    attention_level: 0.94,
    audio_decibel_avg: 42.5,
    speech_tone: 'Engaged / Analytical',
    clone_mirror_response: 'Mirroring high cognitive focus. Synthesizing streamlined responses with deep analytical precision.'
  };

  telemetryInterval: any = null;

  constructor(private http: HttpClient) {}

  ngOnInit() {
    this.startSensors();
    this.telemetryInterval = setInterval(() => this.sendTelemetry(), 4000);
  }

  ngOnDestroy() {
    this.stopSensors();
    if (this.telemetryInterval) clearInterval(this.telemetryInterval);
  }

  async startSensors() {
    try {
      if (navigator.mediaDevices && navigator.mediaDevices.getUserMedia) {
        this.mediaStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
        this.cameraActive = true;
        this.micActive = true;

        if (this.videoPlayer && this.videoPlayer.nativeElement) {
          this.videoPlayer.nativeElement.srcObject = this.mediaStream;
        }

        this.initAudioAnalyser(this.mediaStream);
        this.drawOverlayLoop();
      }
    } catch (err) {
      console.warn('Camera/Mic permission fallback:', err);
      this.cameraActive = false;
      this.micActive = false;
    }
  }

  stopSensors() {
    if (this.mediaStream) {
      this.mediaStream.getTracks().forEach(t => t.stop());
    }
    if (this.audioCtx) {
      this.audioCtx.close();
    }
    if (this.animationFrameId) {
      cancelAnimationFrame(this.animationFrameId);
    }
    this.cameraActive = false;
    this.micActive = false;
  }

  toggleCamera() {
    if (this.cameraActive) {
      this.stopSensors();
    } else {
      this.startSensors();
    }
  }

  initAudioAnalyser(stream: MediaStream) {
    try {
      this.audioCtx = new (window.AudioContext || (window as any).webkitAudioContext)();
      const source = this.audioCtx.createMediaStreamSource(stream);
      this.analyser = this.audioCtx.createAnalyser();
      this.analyser.fftSize = 64;
      source.connect(this.analyser);
    } catch (e) {
      console.error('Audio Context init failed', e);
    }
  }

  drawOverlayLoop() {
    const render = () => {
      this.drawFacialGrid();
      this.drawAudioWave();
      this.animationFrameId = requestAnimationFrame(render);
    };
    render();
  }

  drawFacialGrid() {
    if (!this.canvasOverlay) return;
    const canvas = this.canvasOverlay.nativeElement;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    ctx.clearRect(0, 0, canvas.width, canvas.height);

    if (this.cameraActive) {
      ctx.strokeStyle = '#00f0ff';
      ctx.lineWidth = 1.5;
      ctx.strokeRect(canvas.width * 0.25, canvas.height * 0.2, canvas.width * 0.5, canvas.height * 0.6);

      ctx.fillStyle = '#ff007f';
      ctx.fillRect(canvas.width * 0.25 - 4, canvas.height * 0.2 - 4, 8, 8);
      ctx.fillRect(canvas.width * 0.75 - 4, canvas.height * 0.2 - 4, 8, 8);
      ctx.fillRect(canvas.width * 0.25 - 4, canvas.height * 0.8 - 4, 8, 8);
      ctx.fillRect(canvas.width * 0.75 - 4, canvas.height * 0.8 - 4, 8, 8);

      ctx.beginPath();
      ctx.arc(canvas.width * 0.4, canvas.height * 0.4, 6, 0, Math.PI * 2);
      ctx.arc(canvas.width * 0.6, canvas.height * 0.4, 6, 0, Math.PI * 2);
      ctx.fillStyle = 'rgba(0, 240, 255, 0.7)';
      ctx.fill();

      ctx.font = '10px monospace';
      ctx.fillStyle = '#00ff66';
      ctx.fillText(`EMOTION: ${this.perceptionState.dominant_emotion.toUpperCase()}`, canvas.width * 0.26, canvas.height * 0.17);
    }
  }

  drawAudioWave() {
    if (!this.micWaveform || !this.analyser) return;
    const canvas = this.micWaveform.nativeElement;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const bufferLength = this.analyser.frequencyBinCount;
    const dataArray = new Uint8Array(bufferLength);
    this.analyser.getByteFrequencyData(dataArray);

    ctx.clearRect(0, 0, canvas.width, canvas.height);
    const barWidth = (canvas.width / bufferLength) * 1.5;
    let x = 0;

    for (let i = 0; i < bufferLength; i++) {
      const barHeight = (dataArray[i] / 255) * canvas.height;
      ctx.fillStyle = `hsl(${180 + i * 4}, 100%, 50%)`;
      ctx.fillRect(x, canvas.height - barHeight, barWidth, barHeight);
      x += barWidth + 2;
    }
  }

  sendTelemetry() {
    const payload = {
      visual_data: {
        face_detected: this.cameraActive,
        brightness: 0.7,
        motion_intensity: 0.25
      },
      audio_data: {
        volume: this.micActive ? 0.55 : 0.1,
        pitch: 0.5
      }
    };

    this.http.post<any>(`${environment.apiUrl}/clone/perceive`, payload).subscribe({
      next: (res) => {
        if (res && res.dominant_emotion) {
          this.perceptionState = res;
        }
      },
      error: () => {}
    });
  }

  getEmotionKeys(): string[] {
    return this.perceptionState?.emotion_scores ? Object.keys(this.perceptionState.emotion_scores) : [];
  }
}
