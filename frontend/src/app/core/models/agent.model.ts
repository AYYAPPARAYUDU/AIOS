export interface AgentModel {
  id: string;
  name: string;
  status: 'idle' | 'thinking' | 'executing' | 'error' | string;
  role: string;
  avatar_color?: string;
  capabilities?: string[];
}

export interface AgentLog {
  timestamp: number;
  agent_id: string;
  agent_name: string;
  action: string;
  details: string;
  status: string;
}
