# db_backend.py

import sqlite3
import pandas as pd
import tempfile
import os
import csv
import time
import re
import traceback

from PySide6.QtWidgets import QDialog, QVBoxLayout, QLabel, QProgressBar, QPushButton, QApplication
from PySide6.QtCore import Qt

class SQLiteBackend:
    """SQLiteを使った高速データ処理（UI統合版）"""
    
    def __init__(self, app_instance):
        self.app = app_instance
        self.db_file = tempfile.mktemp(suffix='.db')
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
        self.table_name = 'csv_data'
        self.cancelled = False
        self.header = []
        self.sort_info = None

        # --- パフォーマンス向上のためのPRAGMA設定 ---
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-64000") # 64MB cache
        self.conn.execute("PRAGMA temp_store=MEMORY")

    def import_csv_with_progress(self, filepath, encoding='utf-8', delimiter=','):
        progress_window = QDialog(self.app)
        progress_window.setWindowTitle("データベース構築中")
        vbox = QVBoxLayout(progress_window)
        status_label = QLabel("ファイルを読み込んでいます...")
        vbox.addWidget(status_label)
        detail_label = QLabel("")
        vbox.addWidget(detail_label)
        progress_bar = QProgressBar()
        vbox.addWidget(progress_bar)
        cancel_button = QPushButton("キャンセル")
        vbox.addWidget(cancel_button)
        
        def cancel_import():
            self.cancelled = True
            
        cancel_button.clicked.connect(cancel_import)
        
        progress_window.setWindowModality(Qt.ApplicationModal)
        progress_window.show()

        try:
            total_rows = sum(1 for _ in open(filepath, 'r', encoding=encoding, errors='ignore')) - 1
            if total_rows <= 0:
                progress_window.accept()
                return None, 0

            df_sample = pd.read_csv(filepath, nrows=0, encoding=encoding, sep=delimiter)
            columns = df_sample.columns.tolist()
            self._create_table(columns)
            self.header = columns
            
            chunk_size = 50000
            processed_rows = 0
            
            progress_bar.setRange(0, total_rows)
            
            last_progress_update_time = time.time()
            
            for chunk in pd.read_csv(filepath, chunksize=chunk_size, encoding=encoding, dtype=str, sep=delimiter, on_bad_lines='skip'):
                if self.cancelled:
                    break
                chunk.to_sql(self.table_name, self.conn, if_exists='append', index=False)
                processed_rows += len(chunk)
                
                current_time = time.time()
                if current_time - last_progress_update_time > 0.1:
                    progress_bar.setValue(processed_rows)
                    status_label.setText(f"データベースにインポート中... {processed_rows / total_rows * 100:.1f}%")
                    detail_label.setText(f"{processed_rows:,} / {total_rows:,} 行")
                    if QApplication.instance(): QApplication.instance().processEvents()
                    last_progress_update_time = current_time
            
            if self.cancelled:
                progress_window.reject()
                self.close()
                return None, 0

            progress_bar.setValue(processed_rows)
            status_label.setText(f"データベースにインポート中... 100.0%")
            detail_label.setText(f"{processed_rows:,} / {total_rows:,} 行")
            if QApplication.instance(): QApplication.instance().processEvents()

            status_label.setText("インデックスを構築中... (高速化処理)")
            progress_bar.setRange(0, 0)
            if QApplication.instance(): QApplication.instance().processEvents()
            self._create_indexes(columns)
            self.conn.commit()
            progress_window.accept()
            return columns, processed_rows
        except Exception as e:
            progress_window.reject()
            self.close()
            raise e

    def _create_table(self, columns):
        column_defs = ", ".join([f'"{col}" TEXT' for col in columns])
        create_sql = f"CREATE TABLE {self.table_name} ({column_defs})"
        self.conn.execute(f"DROP TABLE IF EXISTS {self.table_name}")
        self.conn.execute(create_sql)
    
    def _create_indexes(self, columns):
        for col in columns:
            try:
                self.conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{col}" ON {self.table_name}("{col}")')
            except sqlite3.OperationalError as e:
                print(f"Could not create index on column '{col}': {e}")

    def set_sort_order(self, column_name, order):
        """UIからのソート指示を受け取り、状態を保存する"""
        if column_name is None:
            self.sort_info = None
        else:
            self.sort_info = {'column': column_name, 'order': order}

    def search(self, search_term, columns=None, case_sensitive=True):
        if not columns:
            cursor = self.conn.execute(f"PRAGMA table_info({self.table_name})")
            columns = [row[1] for row in cursor]
        
        search_results = []
        like_term = f'%{search_term}%'
        
        for col_idx, col_name in enumerate(columns):
            where_clause = f'"{col_name}" LIKE ?'
            params = [like_term]
            
            if not case_sensitive:
                where_clause = f'LOWER("{col_name}") LIKE ?'
                params = [like_term.lower()]

            query = f"SELECT rowid - 1 FROM {self.table_name} WHERE {where_clause}"
            
            try:
                cursor = self.conn.execute(query, params)
                for row in cursor:
                    search_results.append((row[0], col_idx))
            except sqlite3.OperationalError as e:
                print(f"Search error on column '{col_name}': {e}")
        
        return search_results

    def execute_replace_all_in_db(self, settings):
        """データベース内で直接、全件置換を実行する。"""
        search_term = settings["search_term"]
        replace_term = settings["replace_term"]
        target_columns = settings["target_columns"]
        is_regex = settings["is_regex"]
        is_case_sensitive = settings["is_case_sensitive"]
        
        if not search_term or not target_columns:
            return False, 0

        cursor = self.conn.cursor()
        total_updated_count = 0
        
        try:
            if is_regex:
                def regexp_replace(pattern_str, repl_str, string_val):
                    if string_val is None: return None
                    try:
                        flags = 0 if is_case_sensitive else re.IGNORECASE
                        pattern = re.compile(pattern_str, flags)
                        return pattern.sub(repl_str, string_val)
                    except re.error:
                        return string_val
                
                try:
                    self.conn.create_function("REGEXP_REPLACE", 3, regexp_replace)
                except sqlite3.NotSupportedError:
                     pass

            cursor.execute('BEGIN TRANSACTION')

            for col_name in target_columns:
                if is_regex:
                    sql = f'UPDATE "{self.table_name}" SET "{col_name}" = REGEXP_REPLACE(?, ?, "{col_name}") WHERE "{col_name}" IS NOT NULL'
                    params = (search_term, replace_term)
                else:
                    if is_case_sensitive:
                        sql = f'UPDATE "{self.table_name}" SET "{col_name}" = REPLACE("{col_name}", ?, ?) WHERE INSTR("{col_name}", ?) > 0'
                        params = (search_term, replace_term, search_term)
                    else:
                        sql = f'UPDATE "{self.table_name}" SET "{col_name}" = REPLACE("{col_name}", ?, ?) WHERE INSTR(LOWER("{col_name}"), LOWER(?)) > 0'
                        params = (search_term, replace_term, search_term)

                cursor.execute(sql, params)
                cursor.execute("SELECT changes()")
                count = cursor.fetchone()[0]
                total_updated_count += count
            
            self.conn.commit()
            return True, total_updated_count

        except Exception as e:
            self.conn.rollback()
            print(f"DB execute_replace_all_in_db failed: {e}")
            return False, 0
    
    def update_cells(self, changes: list):
        if not changes:
            return
            
        cursor = self.conn.cursor()
        try:
            cursor.execute('BEGIN TRANSACTION')
            for change in changes:
                row_idx = change['row_idx']
                col_name = change['col_name']
                new_value = change['new_value']
                
                sql = f'UPDATE "{self.table_name}" SET "{col_name}" = ? WHERE rowid = ?'
                cursor.execute(sql, (new_value, row_idx + 1))
            
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"DB update failed: {e}")
            raise

    def get_rows_by_ids(self, indices):
        if not indices: return pd.DataFrame(columns=self.header)
        unique_indices = sorted(list(set(indices)))
        placeholders = ','.join('?' * len(unique_indices))
        params = [i + 1 for i in unique_indices]
        query = f'SELECT rowid, * FROM {self.table_name} WHERE rowid IN ({placeholders})'
        
        df = pd.read_sql_query(query, self.conn, params=params)
        
        if df.empty:
            return pd.DataFrame(columns=self.header)
        
        df.set_index(df['rowid'] - 1, inplace=True)
        
        if set(self.header).issubset(df.columns):
            df = df[self.header]
        
        return df.reindex(indices)

    def get_all_indices(self):
        query = f"SELECT rowid - 1 FROM {self.table_name}"
        if self.sort_info and self.sort_info['column'] in self.header:
            order_str = "ASC" if self.sort_info['order'] == Qt.AscendingOrder else "DESC"
            query += f' ORDER BY "{self.sort_info["column"]}" {order_str}'
        else:
            query += " ORDER BY rowid"

        cursor = self.conn.execute(query)
        return [row[0] for row in cursor]

    def get_total_rows(self):
        return self.conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()[0]

    def insert_rows(self, row_pos, count, headers):
        cursor = self.conn.cursor()
        try:
            cursor.execute('BEGIN TRANSACTION')
            
            header_cols_quoted = [f'"{h}"' for h in headers]
            placeholders = ','.join(['?'] * len(headers))
            sql = f'INSERT INTO "{self.table_name}" ({",".join(header_cols_quoted)}) VALUES ({placeholders})'
            
            for _ in range(count):
                cursor.execute(sql, [""] * len(headers))
            
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"DB insert_rows failed: {e}")
            raise
    
    def remove_rows(self, row_indices):
        if not row_indices: return False
        cursor = self.conn.cursor()
        try:
            cursor.execute('BEGIN TRANSACTION')
            rowids_to_delete = [idx + 1 for idx in row_indices]
            placeholders = ','.join('?' * len(rowids_to_delete))
            sql = f'DELETE FROM "{self.table_name}" WHERE rowid IN ({placeholders})'
            cursor.execute(sql, rowids_to_delete)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"DB remove_rows failed: {e}")
            raise

    def recreate_table_with_new_columns(self, new_headers: list, old_headers_order: list, progress_callback=None):
        temp_table_name = "temp_csv_data_rebuild"
        
        cursor = self.conn.cursor()
        try:
            cursor.execute('BEGIN TRANSACTION')
            
            new_column_defs = ", ".join([f'"{col}" TEXT' for col in new_headers])
            create_temp_sql = f"CREATE TABLE {temp_table_name} ({new_column_defs})"
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            cursor.execute(create_temp_sql)

            select_columns = []
            for h in new_headers:
                if h in old_headers_order:
                    select_columns.append(f'"{h}"')
                else:
                    select_columns.append("'' AS " + f'"{h}"')
            
            total_rows = self.get_total_rows()
            chunk_size = 50000
            
            if total_rows > 0:
                select_from_old_table_sql = f"SELECT {', '.join(select_columns)} FROM {self.table_name}"
                
                processed_rows = 0
                last_progress_update_time = time.time()
                
                read_cursor = self.conn.cursor()
                read_cursor.execute(select_from_old_table_sql)
                
                insert_placeholders = ','.join(['?'] * len(new_headers))
                insert_sql = f'INSERT INTO "{temp_table_name}" VALUES ({insert_placeholders})'

                while True:
                    rows_chunk = read_cursor.fetchmany(chunk_size)
                    if not rows_chunk:
                        break
                    cursor.executemany(insert_sql, rows_chunk)
                    processed_rows += len(rows_chunk)
                    
                    current_time = time.time()
                    if current_time - last_progress_update_time > 0.1 and progress_callback:
                        progress_callback(processed_rows)
                        if QApplication.instance():
                            QApplication.instance().processEvents()
                        last_progress_update_time = current_time
                
                if progress_callback:
                    progress_callback(total_rows)
                    if QApplication.instance():
                        QApplication.instance().processEvents()

            cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            cursor.execute(f"ALTER TABLE {temp_table_name} RENAME TO {self.table_name}")
            self.header = new_headers
            self._create_indexes(new_headers)

            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"DB recreate_table_with_new_columns failed: {e}")
            raise

    def add_column_fast(self, column_name, default_value=''):
        """ALTER TABLEを使った高速な列追加"""
        try:
            self.conn.execute(
                f'ALTER TABLE {self.table_name} ADD COLUMN "{column_name}" TEXT DEFAULT ?',
                (default_value,)
            )
            self.header.append(column_name)
            self.conn.commit()
            return True
        except sqlite3.OperationalError as e:
            print(f"列追加エラー: {e}")
            self.conn.rollback()
            return False

    def insert_column(self, col_name, col_pos, new_full_headers):
        old_headers_order = list(self.header)
        return self.recreate_table_with_new_columns(new_full_headers, old_headers_order, 
                                                     progress_callback=lambda p: self.app.progress_bar_update_signal.emit(p))

    def delete_columns(self, col_names_to_delete: list, new_full_headers: list):
        old_headers_order = list(self.header)
        return self.recreate_table_with_new_columns(new_full_headers, old_headers_order,
                                                     progress_callback=lambda p: self.app.progress_bar_update_signal.emit(p))

    def execute_replace_from_file_in_db(self, params, progress_callback=None):
        lookup_filepath = params['lookup_filepath']
        lookup_encoding = params['lookup_file_encoding']
        target_col = params['target_col']
        lookup_key_col = params['lookup_key_col']
        replace_val_col = params['replace_val_col']

        cursor = self.conn.cursor()
        
        try:
            # 1. 参照ファイルを読み込み、前処理したキーを持つ辞書を作成
            lookup_dict = {}
            with open(lookup_filepath, 'r', encoding=lookup_encoding) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = row.get(lookup_key_col)
                    val = row.get(replace_val_col)
                    if key is not None and val is not None:
                        processed_key = key.strip().lower()
                        if processed_key not in lookup_dict: # 重複キーは最初の一つを優先
                            lookup_dict[key.strip().lower()] = val
            
            if not lookup_dict:
                return True, [], 0 # 参照ファイルが空

            # 2. 本体テーブルから更新対象の行を特定
            update_targets = []
            # rowid を使うことで高速に反復処理
            read_cursor = self.conn.cursor()
            query = f'SELECT rowid, "{target_col}" FROM "{self.table_name}"'
            read_cursor.execute(query)

            while True:
                rows_chunk = read_cursor.fetchmany(10000)
                if not rows_chunk:
                    break
                
                for rowid, cell_value in rows_chunk:
                    if cell_value is not None:
                        processed_cell = str(cell_value).strip().lower()
                        if processed_cell in lookup_dict:
                            new_value = lookup_dict[processed_cell]
                            # 既存の値と異なる場合のみ更新リストに追加
                            if str(cell_value) != new_value:
                                update_targets.append((new_value, rowid))
            
            if not update_targets:
                return True, [], 0 # 更新対象なし

            # 3. 特定した行を一括で更新
            cursor.execute('BEGIN TRANSACTION')
            update_sql = f'UPDATE "{self.table_name}" SET "{target_col}" = ? WHERE rowid = ?'
            cursor.executemany(update_sql, update_targets)
            self.conn.commit()

            return True, [], len(update_targets)

        except Exception as e:
            self.conn.rollback()
            print(f"DB replace_from_file failed: {e}\n{traceback.format_exc()}")
            return False, [], 0

    def close(self):
        if self.conn: self.conn.close()
        if os.path.exists(self.db_file):
            try: os.remove(self.db_file)
            except OSError as e: print(f"Error removing temp db file {self.db_file}: {e}")

    def export_to_csv(self, filepath, encoding='utf-8', quoting_style=csv.QUOTE_MINIMAL):
        """
        メモリ効率の良いストリーミング方式でCSVにエクスポートする。
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"PRAGMA table_info({self.table_name})")
            columns = [row[1] for row in cursor]

            with open(filepath, 'w', encoding=encoding, newline='') as f:
                writer = csv.writer(f, quoting=quoting_style)
                writer.writerow(columns)

                query = f"SELECT * FROM {self.table_name}"
                cursor.execute(query)

                chunk_size = 50000
                processed_rows = 0
                total_rows = self.get_total_rows()
                last_progress_update_time = time.time()
                
                while True:
                    rows_chunk = cursor.fetchmany(chunk_size)
                    if not rows_chunk:
                        break
                    writer.writerows(rows_chunk)
                    processed_rows += len(rows_chunk)

                    current_time = time.time()
                    if current_time - last_progress_update_time > 0.1:
                        if self.app and hasattr(self.app, 'progress_bar_update_signal'):
                            self.app.progress_bar_update_signal.emit(processed_rows)
                        if QApplication.instance():
                            QApplication.instance().processEvents()
                        last_progress_update_time = current_time
                
                if self.app and hasattr(self.app, 'progress_bar_update_signal'):
                    self.app.progress_bar_update_signal.emit(total_rows)

            return True
        except Exception as e:
            print(f"Error exporting to CSV: {e}")
            raise