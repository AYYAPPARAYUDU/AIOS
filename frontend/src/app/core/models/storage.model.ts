export interface StorageFileItem {
  name: string;
  path: string;
  is_directory: boolean;
  size_bytes: number;
  size_formatted: string;
  modified: number;
  extension: string;
}

export interface SearchResultItem {
  id: number;
  file_path: string;
  file_name: string;
  file_size: number;
  extension: string;
  category: string;
  summary: string;
  keywords: string;
  indexed_at: number;
  last_modified: number;
}
