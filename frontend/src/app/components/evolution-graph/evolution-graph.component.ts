import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../../environments/environment';

export interface DailyRecord {
  date: string;
  day_index: number;
  scores: Record<string, number>;
  average_mastery: number;
  tokens_processed: number;
  neural_synapses: number;
  loss_convergence: number;
  learning_velocity_pct: number;
  status: string;
}

export interface SectorItem {
  id: string;
  name: string;
  icon: string;
  color: string;
  current_score: number;
  target_score: number;
}

export interface MonthlyRecord {
  month: string;
  overall_iq_equivalent: number;
  coding_mastery: number;
  empathy_index: number;
  perception_accuracy: number;
  actions_executed: number;
  memory_vectors_stored: number;
  uptime_reliability_pct: number;
}

@Component({
  selector: 'app-evolution-graph',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './evolution-graph.component.html',
  styleUrls: ['./evolution-graph.component.scss']
})
export class EvolutionGraphComponent implements OnInit, OnDestroy {
  activeView: 'daily' | 'monthly' | 'sectors' | 'algorithms' = 'daily';
  isLoading = true;
  evolutionData: {
    status: string;
    sectors: any[];
    radar_distribution: SectorItem[];
    daily_history: DailyRecord[];
    monthly_timeline: MonthlyRecord[];
    current_snapshot: any;
  } | null = null;
  
  trainingTelemetry: any = null;
  pollTimer: any = null;

  constructor(private http: HttpClient) {}

  ngOnInit() {
    this.fetchEvolutionData();
    this.fetchTrainingTelemetry();
    this.pollTimer = setInterval(() => {
      this.fetchEvolutionData();
      if (this.activeView === 'algorithms') {
        this.fetchTrainingTelemetry();
      }
    }, 8000);
  }

  ngOnDestroy() {
    if (this.pollTimer) clearInterval(this.pollTimer);
  }

  fetchEvolutionData() {
    this.http.get<any>(`${environment.apiUrl}/clone/evolution`).subscribe({
      next: (data) => {
        this.evolutionData = data;
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Failed to fetch evolution metrics', err);
        this.isLoading = false;
      }
    });
  }

  fetchTrainingTelemetry() {
    this.http.get<any>(`${environment.apiUrl}/clone/training-telemetry`).subscribe({
      next: (data) => {
        this.trainingTelemetry = data;
      },
      error: (err) => console.error('Failed to fetch training telemetry', err)
    });
  }

  setView(view: 'daily' | 'monthly' | 'sectors' | 'algorithms') {
    this.activeView = view;
    if (view === 'algorithms') {
      this.fetchTrainingTelemetry();
    }
  }

  getParamKeys(params: any): string[] {
    return params ? Object.keys(params) : [];
  }
}
