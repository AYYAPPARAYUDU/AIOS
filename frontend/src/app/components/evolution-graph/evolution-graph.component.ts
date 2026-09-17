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
  activeView: 'daily' | 'monthly' | 'sectors' = 'daily';
  isLoading = true;
  evolutionData: {
    status: string;
    sectors: any[];
    radar_distribution: SectorItem[];
    daily_history: DailyRecord[];
    monthly_timeline: MonthlyRecord[];
    current_snapshot: any;
  } | null = null;
  
  selectedSector: string = 'coding';
  pollTimer: any = null;

  constructor(private http: HttpClient) {}

  ngOnInit() {
    this.fetchEvolutionData();
    this.pollTimer = setInterval(() => this.fetchEvolutionData(), 10000);
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

  setView(view: 'daily' | 'monthly' | 'sectors') {
    this.activeView = view;
  }

  selectSector(id: string) {
    this.selectedSector = id;
  }

  getSectorScore(sectorId: string): number {
    if (!this.evolutionData?.radar_distribution) return 85;
    const s = this.evolutionData.radar_distribution.find((item: any) => item.id === sectorId);
    return s ? s.current_score : 85;
  }
}
