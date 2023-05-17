import sqlite3
from typing import Optional, Any
from datetime import datetime
import json
import config

class Database:
    def __init__(self):
        self.conn = sqlite3.connect('telegram_bot.db', 
                                    detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.cursor = self.conn.cursor()

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS user (
                id INTEGER PRIMARY KEY, 
                chat_id INTEGER,
                username TEXT, 
                first_name TEXT, 
                last_name TEXT, 
                last_interaction TIMESTAMP, 
                first_seen TIMESTAMP, 
                current_dialog_id INTEGER, 
                current_chat_mode TEXT, 
                current_model TEXT,
                n_generated_images INTEGER, 
                n_transcribed_seconds REAL
            );
        """)
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_chat_id on user(chat_id);")

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS dialog (
                id INTEGER PRIMARY KEY, 
                user_id INTEGER, 
                chat_mode TEXT, 
                start_time TIMESTAMP, 
                model TEXT
            );
        """)
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_dialog_user_id on dialog(user_id);")

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                id INTEGER PRIMARY KEY,
                user_id INTEGER, 
                model TEXT, 
                n_input_tokens INTEGER, 
                n_output_tokens INTEGER
            );
        """)
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_tokens_user_id on tokens(user_id);")

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY, 
                dialog_id INTEGER, 
                user TEXT,
                bot TEXT,
                date TIMESTAMP
            );
        """)
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_dialog_id_date on messages(dialog_id, date);")

        self.conn.commit()

    def check_if_user_exists(self, user_id: int, raise_exception: bool = False):
        self.cursor.execute("SELECT COUNT(*) FROM user WHERE id=?", (user_id,))
        if self.cursor.fetchone()[0] > 0:
            return True
        else:
            if raise_exception:
                raise ValueError(f"User {user_id} does not exist")
            else:
                return False

    def add_new_user(
        self,
        user_id: int,
        chat_id: int,
        username: str = "",
        first_name: str = "",
        last_name: str = "",
    ):
        now = datetime.now()
        current_model = config.models["available_text_models"][0]

        if not self.check_if_user_exists(user_id):
            self.cursor.execute("""
                INSERT INTO user (
                    id, chat_id, username, first_name, last_name, 
                    last_interaction, first_seen, current_chat_mode, 
                    current_model, n_generated_images, n_transcribed_seconds
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (user_id, chat_id, username, first_name, last_name, now, now, 'assistant', current_model, 0, 0.0))

        self.conn.commit()

    def start_new_dialog(self, user_id: int):
        self.check_if_user_exists(user_id, raise_exception=True)

        # dialog_id = str(uuid.uuid4())
        chat_mode = self.get_user_attribute(user_id, "current_chat_mode")
        model = self.get_user_attribute(user_id, "current_model")

        result = self.cursor.execute("""
            INSERT INTO dialog (user_id, chat_mode, start_time, model)
            VALUES (?, ?, ?, ?)
        """, (user_id, chat_mode, datetime.now(), model))
        dialog_id = result.lastrowid

        self.cursor.execute("""
        UPDATE user SET current_dialog_id = ? WHERE id = ?
                """, (dialog_id, user_id))
        self.conn.commit()
        return dialog_id

    def get_user_attribute(self, user_id: int, key: str):
        self.check_if_user_exists(user_id, raise_exception=True)
        if key == 'n_used_tokens':
            self.cursor.execute(f"SELECT * FROM tokens WHERE user_id = ?", (user_id,))
            rows = self.cursor.fetchall()
            result = []
            for r in rows:
                result.append({"id":r[0], "user_id":r[1], "model": r[2], "n_input_tokens": r[3], "n_output_tokens": r[4]})
            if result:
                return result
        else:
            self.cursor.execute(f"SELECT {key} FROM user WHERE id = ?", (user_id,))
            result = self.cursor.fetchone()
            if result:
                return result[0]
        return None

    def set_user_attribute(self, user_id: int, key: str, value: Any):
        self.check_if_user_exists(user_id, raise_exception=True)
        self.cursor.execute(f"UPDATE user SET {key} = ? WHERE id = ?", (value, user_id))
        self.conn.commit()

    def update_n_used_tokens(self, user_id: int, model: str, n_input_tokens: int, n_output_tokens: int):
        self.cursor.execute("""
            SELECT id, n_input_tokens, n_output_tokens 
            FROM tokens 
            WHERE user_id = ? AND model = ?
        """, (user_id, model))

        row = self.cursor.fetchone()
        if row:
            token_id, n_input_tokens_old, n_output_tokens_old = row
            self.cursor.execute("""
                UPDATE tokens 
                SET n_input_tokens = ?, n_output_tokens = ? 
                WHERE id = ? and model = ?
            """, (n_input_tokens_old + n_input_tokens, n_output_tokens_old + n_output_tokens, token_id, model))
        else:
            self.cursor.execute("""
                INSERT INTO tokens (user_id, model, n_input_tokens, n_output_tokens)
                VALUES (?, ?, ?, ?)
            """, (user_id, model, n_input_tokens, n_output_tokens))

        self.conn.commit()

    def get_dialog_messages(self, user_id: int, dialog_id: Optional[int] = None):
        self.check_if_user_exists(user_id, raise_exception=True)

        if dialog_id is None:
            dialog_id = self.get_user_attribute(user_id, "current_dialog_id")

        cur = self.cursor.execute("SELECT * FROM messages WHERE dialog_id = ? order by date ASC", (dialog_id,))
        rows = cur.fetchall()
        if rows is None:
            return []

        messages = []
        for r in rows:
            messages.append(self.__wrap_message(r, cur.description))

        return messages
    
    def __wrap_message(self, row: tuple, desc: list):
        m = {}
        for i, d in enumerate(desc):
            m[d[0]] = row[i]
        return m
    
    def get_last_dialog_message(self, user_id: int, dialog_id: Optional[int] = None):
        if dialog_id is None:
            dialog_id = self.get_user_attribute(user_id, "current_dialog_id")
        if dialog_id is None:
            raise ValueError(f"User {user_id} does not exist")
        cur = self.cursor.execute("SELECT * FROM messages WHERE dialog_id = ? order by date DESC limit 1", (dialog_id,))
        row = cur.fetchone()
        return self.__wrap_message(row, cur.description)

    def insert_dialog_message(self, user_id: int, user: str, bot: str, date: datetime, dialog_id: Optional[int] = None):
        self.check_if_user_exists(user_id, raise_exception=True)

        if dialog_id is None:
            dialog_id = self.get_user_attribute(user_id, "current_dialog_id")

        self.cursor.execute("INSERT INTO messages(dialog_id, user, bot, date) VALUES (?, ?, ?, ?) ", 
                            (dialog_id, user, bot, date))

        self.conn.commit()