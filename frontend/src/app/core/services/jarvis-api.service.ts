import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { SystemTelemetry, ProcessItem } from '../models/system.model';
import { AgentModel, AgentLog } from '../models/agent.model';
import { StorageFileItem, SearchResultItem } from '../models/storage.model';

@Injectable({
  providedIn: 'root'
})
export class JarvisApiService {
  private baseUrl = '';

  constructor(private http: HttpClient) {}

  getHealth(): Observable<any> {
    return this.http.get<any>(`${this.baseUrl}/health`);
  }

  getTelemetry(): Observable<SystemTelemetry> {
    return this.http.get<SystemTelemetry>(`${this.baseUrl}/api/system/telemetry`);
  }

  getSystemState(): Observable<{ volume: number; muted: boolean; brightness: number }> {
    return this.http.get<{ volume: number; muted: boolean; brightness: number }>(`${this.baseUrl}/api/system/actions/state`);
  }

  getProcesses(limit = 25): Observable<{ processes: ProcessItem[] }> {
    return this.http.get<{ processes: ProcessItem[] }>(`${this.baseUrl}/api/system/processes?limit=${limit}`);
  }

  killProcess(pid: number): Observable<any> {
    return this.http.post<any>(`${this.baseUrl}/api/system/processes/kill`, { pid });
  }

  setVolume(level: number | null, mute: boolean | null = null): Observable<any> {
    const payload: any = {};
    if (level !== null) payload.level = level;
    if (mute !== null) payload.mute = mute;
    return this.http.post<any>(`${this.baseUrl}/api/system/actions/volume`, payload);
  }

  setBrightness(level: number): Observable<any> {
    return this.http.post<any>(`${this.baseUrl}/api/system/actions/brightness`, { level });
  }

  launchApp(appName: string, args: string | null = null): Observable<any> {
    return this.http.post<any>(`${this.baseUrl}/api/system/actions/app`, { app_name: appName, arguments: args });
  }

  powerAction(mode: string): Observable<any> {
    return this.http.post<any>(`${this.baseUrl}/api/system/actions/power`, { mode });
  }

  runMacro(macroName: string): Observable<any> {
    return this.http.post<any>(`${this.baseUrl}/api/system/macro`, { macro_name: macroName });
  }

  purgeRam(): Observable<any> {
    return this.http.post<any>(`${this.baseUrl}/api/system/purge-ram`, {});
  }

  takeScreenshot(): Observable<any> {
    return this.http.post<any>(`${this.baseUrl}/api/system/automation/screenshot`, {});
  }

  executeTerminal(command: string, shellType = 'powershell'): Observable<any> {
    return this.http.post<any>(`${this.baseUrl}/api/system/terminal/execute`, { command, shell_type: shellType });
  }

  // 50GB Storage Pool
  getStorageStats(): Observable<any> {
    return this.http.get<any>(`${this.baseUrl}/api/storage/stats`);
  }

  listStorageFiles(subfolder = ''): Observable<{ files: StorageFileItem[] }> {
    return this.http.get<{ files: StorageFileItem[] }>(`${this.baseUrl}/api/storage/files?subfolder=${encodeURIComponent(subfolder)}`);
  }

  uploadStorageFile(file: File, subfolder = 'documents'): Observable<any> {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('subfolder', subfolder);
    return this.http.post<any>(`${this.baseUrl}/api/storage/upload`, formData);
  }

  indexDirectory(directoryPath: string, maxFiles = 300): Observable<any> {
    return this.http.post<any>(`${this.baseUrl}/api/storage/index-directory`, { directory_path: directoryPath, max_files: maxFiles });
  }

  searchKnowledge(query: string): Observable<{ query: string; count: number; results: SearchResultItem[] }> {
    return this.http.post<{ query: string; count: number; results: SearchResultItem[] }>(`${this.baseUrl}/api/storage/search`, { query });
  }

  // Multi-Agent Swarm
  chatWithAbhi(message: string, targetAgent = 'abhi', conversationId = 'main_session'): Observable<any> {
    return this.http.post<any>(`${this.baseUrl}/api/agents/chat`, { message, target_agent: targetAgent, conversation_id: conversationId });
  }

  chatWithJarvis(message: string, targetAgent = 'abhi', conversationId = 'main_session'): Observable<any> {
    return this.chatWithAbhi(message, targetAgent, conversationId);
  }

  dispatchAgent(agentId: string, instruction: string, conversationId = 'main_session'): Observable<any> {
    return this.http.post<any>(`${this.baseUrl}/api/agents/dispatch`, { agent_id: agentId, instruction, conversation_id: conversationId });
  }

  getAgentStatus(): Observable<{ agents: AgentModel[]; recent_logs: AgentLog[]; llm_health: any }> {
    return this.http.get<{ agents: AgentModel[]; recent_logs: AgentLog[]; llm_health: any }>(`${this.baseUrl}/api/agents/status`);
  }

  // Contacts Directory & Memory

  listContacts(): Observable<{ contacts: any[] }> {
    return this.http.get<{ contacts: any[] }>(`${this.baseUrl}/api/agents/contacts`);
  }

  saveContact(name: string, phone?: string, email?: string, notes?: string): Observable<any> {
    return this.http.post<any>(`${this.baseUrl}/api/agents/contacts`, { name, phone, email, notes });
  }

  deleteContact(name: string): Observable<any> {
    return this.http.delete<any>(`${this.baseUrl}/api/agents/contacts/${encodeURIComponent(name)}`);
  }

  // Session Vault Management
  exportSession(conversationId = 'main_session'): Observable<any> {
    return this.http.post<any>(`${this.baseUrl}/api/agents/session/export`, { conversation_id: conversationId });
  }

  clearSession(conversationId = 'main_session'): Observable<any> {
    return this.http.delete<any>(`${this.baseUrl}/api/agents/session/clear?conversation_id=${encodeURIComponent(conversationId)}`);
  }
}

