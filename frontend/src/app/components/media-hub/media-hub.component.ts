import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../../environments/environment';

export interface GalleryItem {
  filename: string;
  url: string;
  type: string;
}

@Component({
  selector: 'app-media-hub',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './media-hub.component.html',
  styleUrls: ['./media-hub.component.scss']
})
export class MediaHubComponent implements OnInit {
  promptText = '';
  mediaType: 'image' | 'video' = 'image';
  selectedStyle = 'Cyberpunk 4K Hologram';
  aspectRatio = '16:9';
  isGenerating = false;
  lastGeneratedMedia: any = null;
  gallery: { images: GalleryItem[]; videos: GalleryItem[]; total_items: number } = { images: [], videos: [], total_items: 0 };

  styles = [
    'Cyberpunk 4K Hologram',
    'Quantum Neural Core',
    'Futuristic HUD Interface',
    'Deep Space Nebula Matrix',
    'Hyper-Realistic Synthwave'
  ];

  constructor(private http: HttpClient) {}

  ngOnInit() {
    this.fetchGallery();
  }

  fetchGallery() {
    this.http.get<any>(`${environment.apiUrl}/media/gallery`).subscribe({
      next: (data) => {
        this.gallery = data || { images: [], videos: [], total_items: 0 };
      },
      error: (err) => console.error('Failed to fetch media gallery', err)
    });
  }

  generateMedia() {
    if (!this.promptText.trim() || this.isGenerating) return;

    this.isGenerating = true;
    if (this.mediaType === 'image') {
      const payload = {
        prompt: this.promptText,
        style: this.selectedStyle,
        aspect_ratio: this.aspectRatio
      };
      this.http.post<any>(`${environment.apiUrl}/media/generate-image`, payload).subscribe({
        next: (res) => {
          this.lastGeneratedMedia = res;
          this.isGenerating = false;
          this.fetchGallery();
        },
        error: (err) => {
          console.error('Image generation failed', err);
          this.isGenerating = false;
        }
      });
    } else {
      const payload = {
        prompt: this.promptText,
        duration_sec: 5,
        fps: 30
      };
      this.http.post<any>(`${environment.apiUrl}/media/generate-video`, payload).subscribe({
        next: (res) => {
          this.lastGeneratedMedia = res;
          this.isGenerating = false;
          this.fetchGallery();
        },
        error: (err) => {
          console.error('Video generation failed', err);
          this.isGenerating = false;
        }
      });
    }
  }

  getMediaUrl(path: string): string {
    return `${environment.apiUrl.replace('/api', '')}${path}`;
  }
}
