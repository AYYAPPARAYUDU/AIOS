export interface ToolCallExecution {
  tool: string;
  parameters: any;
  result: any;
  executed_by?: string;
}

export interface ChatMessage {
  id?: number;
  sender: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  toolCall?: ToolCallExecution | null;
  toolCalls?: ToolCallExecution[] | null;
  imagePreview?: string | null;
  timestamp?: number;
}

