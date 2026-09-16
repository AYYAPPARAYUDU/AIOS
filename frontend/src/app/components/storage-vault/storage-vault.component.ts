import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { StorageFileItem, SearchResultItem } from '../../core/models/storage.model';
import { JarvisApiService } from '../../core/services/jarvis-api.service';
import { JarvisAudioService } from '../../core/services/jarvis-audio.service';

@Component({
  selector: 'app-storage-vault',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './storage-vault.component.html',
  styleUrls: ['./storage-vault.component.scss']
})
export class StorageVaultComponent implements OnInit {
  public files: StorageFileItem[] = [];
  public searchResults: SearchResultItem[] = [];
  public searchQuery: string = '';
  public indexDirPath: string = '';
  public statusMessage: string = '';

  constructor(
    private apiService: JarvisApiService,
    private audioService: JarvisAudioService
  ) {}

  ngOnInit(): void {
    this.loadFiles();
  }

  loadFiles(): void {
    this.apiService.listStorageFiles().subscribe(res => {
      this.files = res.files || [];
    });
  }

  onFileSelected(event: any): void {
    const file = event.target.files[0];
    if (file) {
      this.statusMessage = `Uploading ${file.name}...`;
      this.apiService.uploadStorageFile(file).subscribe({
        next: (res) => {
          this.audioService.playSciFiTone('ack');
          this.statusMessage = `Saved ${res.filename} (${res.size} bytes).`;
          this.loadFiles();
        },
        error: (err) => {
          this.audioService.playSciFiTone('error');
          this.statusMessage = `Upload error: ${err.message}`;
        }
      });
    }
  }

  searchKnowledge(): void {
    const q = this.searchQuery.trim();
    if (!q) return;

    this.audioService.playSciFiTone('beep');
    this.apiService.searchKnowledge(q).subscribe(res => {
      this.searchResults = res.results || [];
    });
  }

  indexDirectory(): void {
    const dir = this.indexDirPath.trim();
    if (!dir) return;

    this.audioService.playSciFiTone('ack');
    this.statusMessage = `Scanning and indexing ${dir}...`;
    this.apiService.indexDirectory(dir).subscribe({
      next: (res) => {
        this.statusMessage = `Indexed ${res.indexed_count} files into SQLite knowledge base.`;
        this.loadFiles();
      },
      error: (err) => {
        this.statusMessage = `Index error: ${err.message}`;
      }
    });
  }
}
