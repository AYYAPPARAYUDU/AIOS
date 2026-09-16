export interface UserEmotion {
  mood: 'focused' | 'happy' | 'stressed' | 'tired' | 'excited' | 'neutral' | string;
  stress_level: number;
  focus_score: number;
  sentiment: string;
  heart_rate_sim: number;
  energy_level: number;
  last_updated: number;
  recommendation: string;
}

export interface CpuMetric {
  percent: number;
  cores_logical: number;
  cores_physical: number;
  current_freq_mhz: number;
}

export interface MemoryMetric {
  total_gb: number;
  used_gb: number;
  free_gb: number;
  percent: number;
  swap_percent: number;
}

export interface BatteryMetric {
  percent: number;
  power_plugged: boolean;
  secsleft: number;
}

export interface DiskMetric {
  device: string;
  mountpoint: string;
  fstype: string;
  total_gb: number;
  used_gb: number;
  free_gb: number;
  percent: number;
}

export interface NetworkMetric {
  bytes_sent: number;
  bytes_recv: number;
  upload_speed_kbps: number;
  download_speed_kbps: number;
}

export interface OsInfo {
  system: string;
  hostname: string;
  local_ip: string;
  boot_time: number;
  uptime_seconds: number;
}

export interface StoragePoolSummary {
  pool_path: string;
  max_quota_gb: number;
  used_mb: number;
  used_gb: number;
  free_gb: number;
  used_percentage: number;
  total_files: number;
}

export interface SystemTelemetry {
  timestamp: number;
  os: OsInfo;
  cpu: CpuMetric;
  memory: MemoryMetric;
  battery: BatteryMetric | null;
  disks: DiskMetric[];
  storage_pool_50gb: StoragePoolSummary;
  network: NetworkMetric;
  emotion?: UserEmotion;
  gpu?: any[];
}

export interface ProcessItem {
  pid: number;
  name: string;
  cpu_percent: number;
  memory_percent: number;
  status: string;
  uptime: number;
}
