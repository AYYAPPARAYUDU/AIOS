export interface AgentModel {
  id: string;
  name: string;
  deity?: string;
  status: 'idle' | 'thinking' | 'executing' | 'error' | 'active' | string;
  role: string;
  avatar_color?: string;
  avatar_icon?: string;
  mantra?: string;
  capabilities?: string[];
}

export interface AgentLog {
  timestamp: number;
  agent_id: string;
  agent_name: string;
  deity?: string;
  action: string;
  details: string;
  status: string;
}
