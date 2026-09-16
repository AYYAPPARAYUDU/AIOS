import sqlite3
import json
import time
from typing import Any, Optional
from pathlib import Path
from backend.app.config import settings

class Database:
    def __init__(self, db_path: Path = settings.SQLITE_DB_PATH):
        self.db_path = db_path
        self._init_db()

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Chat & Conversation History
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                id TEXT PRIMARY KEY,
                title TEXT,
                created_at REAL,
                updated_at REAL
            )""")

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT,
                role TEXT,
                sender_name TEXT,
                content TEXT,
                tool_calls TEXT,
                thoughts TEXT,
                created_at REAL,
                FOREIGN KEY (conversation_id) REFERENCES conversations(id)
            )""")

            # Contacts Directory for WhatsApp & Email Memory
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                phone TEXT,
                email TEXT,
                notes TEXT,
                created_at REAL,
                updated_at REAL
            )""")

            # Long-term Memory & Knowledge
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS memory (
                key TEXT PRIMARY KEY,
                value TEXT,
                category TEXT,
                created_at REAL,
                updated_at REAL
            )""")

            # 50GB Local Storage DB File Metadata & Index
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS indexed_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT UNIQUE,
                file_name TEXT,
                file_size INTEGER,
                extension TEXT,
                category TEXT,
                summary TEXT,
                keywords TEXT,
                embedding TEXT,
                indexed_at REAL,
                last_modified REAL
            )""")

            # Agent Task Queue and Logs
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS agent_tasks (
                id TEXT PRIMARY KEY,
                agent_name TEXT,
                task_description TEXT,
                status TEXT,
                plan TEXT,
                result TEXT,
                created_at REAL,
                completed_at REAL
            )""")

            # System Audit & Action Log
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action_type TEXT,
                command TEXT,
                executor TEXT,
                status TEXT,
                output TEXT,
                timestamp REAL
            )""")

            conn.commit()

    # Contacts Management (WhatsApp & Email Memory)
    def save_contact(self, name: str, phone: Optional[str] = None, email: Optional[str] = None, notes: Optional[str] = None) -> dict[str, Any]:
        clean_name = name.lower().strip()
        now = time.time()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            INSERT INTO contacts (name, phone, email, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET 
                phone = COALESCE(excluded.phone, contacts.phone),
                email = COALESCE(excluded.email, contacts.email),
                notes = COALESCE(excluded.notes, contacts.notes),
                updated_at = excluded.updated_at
            """, (clean_name, phone, email, notes, now, now))
            conn.commit()
            return {"status": "success", "contact": clean_name, "phone": phone, "email": email}

    def get_contact(self, name: str) -> Optional[dict[str, Any]]:
        clean_name = name.lower().strip()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM contacts WHERE name = ? OR name LIKE ?", (clean_name, f"%{clean_name}%"))
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None

    def list_contacts(self) -> list[dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM contacts ORDER BY name ASC")
            return [dict(row) for row in cursor.fetchall()]

    def delete_contact(self, name: str) -> bool:
        clean_name = name.lower().strip()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM contacts WHERE name = ?", (clean_name,))
            conn.commit()
            return cursor.rowcount > 0

    # Memory Operations
    def set_memory(self, key: str, value: Any, category: str = "general"):
        val_str = json.dumps(value) if not isinstance(value, str) else value
        now = time.time()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            INSERT INTO memory (key, value, category, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at, category=excluded.category
            """, (key, val_str, category, now, now))
            conn.commit()

    def get_memory(self, key: str) -> Optional[Any]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM memory WHERE key = ?", (key,))
            row = cursor.fetchone()
            if row:
                try:
                    return json.loads(row["value"])
                except Exception:
                    return row["value"]
            return None

    def list_memories(self, category: Optional[str] = None) -> list[dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if category:
                cursor.execute("SELECT * FROM memory WHERE category = ? ORDER BY updated_at DESC", (category,))
            else:
                cursor.execute("SELECT * FROM memory ORDER BY updated_at DESC")
            return [dict(row) for row in cursor.fetchall()]

    # Conversation & Session Operations
    def add_message(self, conversation_id: str, role: str, content: str, sender_name: str = "ABHI", tool_calls: Optional[list] = None, thoughts: Optional[str] = None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            now = time.time()
            cursor.execute("""
            INSERT INTO conversations (id, title, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at
            """, (conversation_id, content[:40] if content else "Session", now, now))
            
            cursor.execute("""
            INSERT INTO messages (conversation_id, role, sender_name, content, tool_calls, thoughts, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (conversation_id, role, sender_name, content, json.dumps(tool_calls) if tool_calls else None, thoughts, now))
            conn.commit()

    def get_messages(self, conversation_id: str, limit: int = 50) -> list[dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT * FROM messages WHERE conversation_id = ? ORDER BY id ASC LIMIT ?
            """, (conversation_id, limit))
            rows = cursor.fetchall()
            messages = []
            for row in rows:
                item = dict(row)
                if item.get("tool_calls"):
                    try:
                        item["tool_calls"] = json.loads(item["tool_calls"])
                    except Exception:
                        pass
                messages.append(item)
            return messages

    def clear_conversation(self, conversation_id: str) -> bool:
        """Deletes all messages for a specific session."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM messages WHERE conversation_id = ?", (conversation_id,))
            cursor.execute("DELETE FROM conversations WHERE id = ?", (conversation_id,))
            conn.commit()
            return True

    def export_session_to_vault(self, conversation_id: str) -> dict[str, Any]:
        """Saves session transcript to the 50GB local storage vault as a formatted markdown file."""
        messages = self.get_messages(conversation_id, limit=500)
        if not messages:
            return {"status": "empty", "message": "No messages to export"}
            
        timestamp_str = time.strftime("%Y%m%d_%H%M%S")
        filename = f"session_{conversation_id}_{timestamp_str}.md"
        file_path = settings.STORAGE_POOL_DIR / "documents" / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        md_content = f"# ABHI AIOS Session Archive — {conversation_id}\n*Exported on {time.ctime()}*\n\n---\n\n"
        for m in messages:
            sender = m.get("sender_name", m.get("role", "Unknown"))
            md_content += f"### **{sender.upper()}**:\n{m.get('content', '')}\n\n"
            if m.get("tool_calls"):
                md_content += f"> **Deity Tool Actions**: `{json.dumps(m['tool_calls'])}`\n\n"
                
        file_path.write_text(md_content, encoding="utf-8")
        
        # Index in 50GB local vault
        self.index_file(
            file_path=str(file_path),
            file_name=filename,
            file_size=len(md_content.encode("utf-8")),
            extension=".md",
            category="session_archives",
            summary=f"Saved conversation session transcript with {len(messages)} messages.",
            keywords=f"session,{conversation_id},transcript,archive",
            last_modified=time.time()
        )
        return {"status": "success", "file_name": filename, "file_path": str(file_path), "messages_count": len(messages)}

    # File Index Operations
    def index_file(self, file_path: str, file_name: str, file_size: int, extension: str, category: str, summary: str = "", keywords: str = "", embedding: Optional[list] = None, last_modified: float = 0):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            now = time.time()
            cursor.execute("""
            INSERT INTO indexed_files (file_path, file_name, file_size, extension, category, summary, keywords, embedding, indexed_at, last_modified)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(file_path) DO UPDATE SET
                file_size=excluded.file_size,
                summary=excluded.summary,
                keywords=excluded.keywords,
                embedding=excluded.embedding,
                indexed_at=excluded.indexed_at,
                last_modified=excluded.last_modified
            """, (file_path, file_name, file_size, extension.lower(), category, summary, keywords, json.dumps(embedding) if embedding else None, now, last_modified))
            conn.commit()

    def search_indexed_files(self, query: str, limit: int = 25) -> list[dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            like_q = f"%{query}%"
            cursor.execute("""
            SELECT id, file_path, file_name, file_size, extension, category, summary, keywords, indexed_at, last_modified
            FROM indexed_files
            WHERE file_name LIKE ? OR summary LIKE ? OR keywords LIKE ? OR file_path LIKE ?
            ORDER BY indexed_at DESC LIMIT ?
            """, (like_q, like_q, like_q, like_q, limit))
            return [dict(row) for row in cursor.fetchall()]

    def count_indexed_files(self) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as cnt FROM indexed_files")
            return cursor.fetchone()["cnt"]

    # Audit Logging
    def log_audit(self, action_type: str, command: str, executor: str, status: str, output: str = ""):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            INSERT INTO audit_logs (action_type, command, executor, status, output, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (action_type, command, executor, status, output[:1000] if output else "", time.time()))
            conn.commit()

    def get_audit_logs(self, limit: int = 50) -> list[dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM audit_logs ORDER BY id DESC LIMIT ?", (limit,))
            return [dict(row) for row in cursor.fetchall()]

db = Database()
