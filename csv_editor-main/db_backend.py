# db_backend.py

import sqlite3
import pandas as pd
import tempfile
import os
import tkinter as tk
from tkinter import ttk

class SQLiteBackend:
    """SQLiteを使った高速データ処理（UI統合版）"""
    
    def __init__(self, app_instance):
        self.app = app_instance
        self.theme = app_instance.theme
        self.db_file = tempfile.mktemp(suffix='.db')
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
        self.table_name = 'csv_data'
        self.cancelled = False
        
    def import_csv_with_progress(self, filepath, encoding='utf-8', delimiter=','):
        """プログレスダイアログ付きでCSVをインポート"""
        progress_window = tk.Toplevel(self.app)
        progress_window.title("データベース構築中")
        progress_window.geometry("500x200")
        progress_window.transient(self.app)
        progress_window.grab_set()
        progress_window.protocol("WM_DELETE_WINDOW", lambda: None) # 閉じさせない

        progress_window.configure(bg=self.theme.BG_LEVEL_1)
        
        main_frame = ttk.Frame(progress_window, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        status_label = ttk.Label(main_frame, text="CSVファイルを読み込んでいます...")
        status_label.pack(pady=(0, 10))
        
        progress_bar = ttk.Progressbar(main_frame, length=450, mode='determinate')
        progress_bar.pack(pady=(0, 10))
        
        detail_label = ttk.Label(main_frame, text="準備中...", foreground=self.theme.TEXT_SECONDARY)
        detail_label.pack()
        
        cancel_button = ttk.Button(main_frame, text="キャンセル", command=lambda: setattr(self, 'cancelled', True))
        cancel_button.pack(pady=(10, 0))
        
        self.app.update_idletasks()
        
        try:
            total_rows = sum(1 for _ in open(filepath, 'r', encoding=encoding, errors='ignore')) - 1
            if total_rows <= 0:
                progress_window.destroy()
                return None, 0

            df_sample = pd.read_csv(filepath, nrows=0, encoding=encoding, sep=delimiter)
            columns = df_sample.columns.tolist()
            self._create_table(columns)
            
            chunk_size = 50000
            processed_rows = 0
            
            for chunk in pd.read_csv(filepath, chunksize=chunk_size, encoding=encoding, dtype=str, sep=delimiter, on_bad_lines='skip'):
                if self.cancelled:
                    break
                
                chunk.to_sql(self.table_name, self.conn, if_exists='append', index=False)
                processed_rows += len(chunk)
                
                progress = int(processed_rows / total_rows * 100)
                progress_bar['value'] = progress
                status_label['text'] = f"データベースにインポート中... {progress}%"
                detail_label['text'] = f"{processed_rows:,} / {total_rows:,} 行"
                self.app.update_idletasks()
            
            if not self.cancelled:
                status_label['text'] = "インデックスを構築中... (高速化処理)"
                progress_bar['mode'] = 'indeterminate'
                progress_bar.start()
                self.app.update_idletasks()
                
                self._create_indexes(columns)
                self.conn.commit()
                
                progress_window.destroy()
                return columns, processed_rows
            else:
                progress_window.destroy()
                self.close()
                return None, 0
                
        except Exception as e:
            progress_window.destroy()
            self.close()
            raise e
    
    def _create_table(self, columns):
        """テーブルを作成"""
        # "rowid"はSQLiteの内部的な行IDなので、主キーとして利用する
        column_defs = ", ".join([f'"{col}" TEXT' for col in columns])
        create_sql = f"CREATE TABLE {self.table_name} ({column_defs})"
        
        self.conn.execute(f"DROP TABLE IF EXISTS {self.table_name}")
        self.conn.execute(create_sql)
    
    def _create_indexes(self, columns):
        """検索用インデックスを作成"""
        # 最初の5列にインデックスを作成（一般的な検索を高速化）
        for col in columns[:5]:
            try:
                self.conn.execute(f'CREATE INDEX "idx_{col}" ON {self.table_name}("{col}")')
            except sqlite3.OperationalError as e:
                print(f"Could not create index on column '{col}': {e}")
                pass
    
    def search(self, search_term, columns=None):
        """高速検索を実行"""
        if not columns:
            cursor = self.conn.execute(f"PRAGMA table_info({self.table_name})")
            columns = [row[1] for row in cursor]
        
        where_clauses = [f'"{col}" LIKE ?' for col in columns]
        where_sql = " OR ".join(where_clauses)
        params = [f'%{search_term}%'] * len(columns)
        
        query = f"SELECT rowid - 1 FROM {self.table_name} WHERE {where_sql}"
        
        cursor = self.conn.execute(query, params)
        return [row[0] for row in cursor]
    
    def get_rows_by_ids(self, indices):
        """複数行のIDを指定して一括取得"""
        if not indices:
            return pd.DataFrame()
        
        placeholders = ','.join('?' * len(indices))
        # rowidは1から始まるので、0-basedのインデックスに合わせるために+1する
        params = [i + 1 for i in indices]
        
        # ▼▼▼ 修正箇所 ▼▼▼
        # SQLの予約語である 'index' をエイリアスとして使うために二重引用符で囲む
        query = f'SELECT rowid - 1 as "index", * FROM {self.table_name} WHERE rowid IN ({placeholders})'
        # ▲▲▲ 修正完了 ▲▲▲
        
        df = pd.read_sql_query(query, self.conn, params=params, index_col='index')
        return df

    def get_all_indices(self):
        """全行のインデックスリストを取得"""
        query = f"SELECT rowid - 1 FROM {self.table_name}"
        cursor = self.conn.execute(query)
        return [row[0] for row in cursor]

    def get_total_rows(self):
        """総行数を取得"""
        return self.conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()[0]

    def close(self):
        """データベース接続を閉じ、一時ファイルを削除する"""
        if self.conn:
            self.conn.close()
        if os.path.exists(self.db_file):
            try:
                os.remove(self.db_file)
            except OSError as e:
                print(f"Error removing temp db file {self.db_file}: {e}")