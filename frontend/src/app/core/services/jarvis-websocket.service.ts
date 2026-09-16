import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';
import { SystemTelemetry } from '../models/system.model';
import { AgentModel } from '../models/agent.model';

@Injectable({
  providedIn: 'root'
})
export class JarvisWebsocketService {
  private ws: WebSocket | null = null;
  private reconnectInterval = 3000;
  
  private telemetrySubject = new BehaviorSubject<SystemTelemetry | null>(null);
  public telemetry$: Observable<SystemTelemetry | null> = this.telemetrySubject.asObservable();

  private agentsSubject = new BehaviorSubject<AgentModel[]>([]);
  public agents$: Observable<AgentModel[]> = this.agentsSubject.asObservable();

  private isConnectedSubject = new BehaviorSubject<boolean>(false);
  public isConnected$: Observable<boolean> = this.isConnectedSubject.asObservable();

  constructor() {
    this.connect();
  }

  public connect(): void {
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = window.location.host || 'localhost:8000';
    const wsUrl = `${proto}//${host}/ws/telemetry`;

    try {
      this.ws = new WebSocket(wsUrl);

      this.ws.onopen = () => {
        console.log('[JARVIS-WS] Connected to live telemetry stream');
        this.isConnectedSubject.next(true);
      };

      this.ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.type === 'TELEMETRY_UPDATE') {
            if (data.telemetry) this.telemetrySubject.next(data.telemetry);
            if (data.agents) this.agentsSubject.next(data.agents);
          }
        } catch (e) {
          console.error('[JARVIS-WS] Parse error', e);
        }
      };

      this.ws.onclose = () => {
        this.isConnectedSubject.next(false);
        setTimeout(() => this.connect(), this.reconnectInterval);
      };

      this.ws.onerror = (err) => {
        this.isConnectedSubject.next(false);
      };
    } catch (e) {
      setTimeout(() => this.connect(), this.reconnectInterval);
    }
  }
}
