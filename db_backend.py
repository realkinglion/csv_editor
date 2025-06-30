# db_backend.py

import sqlite3
import pandas as pd
import tempfile
import os
import csv
import time
import re
import traceback

# ▼▼▼ 変更点: UI関連のimportをすべて削除 ▼▼▼
# from PySide6.QtWidgets import QDialog, QVBoxLayout, QLabel, QProgressBar, QPushButton, QApplication
# from PySide6.QtCore import Qt
# ▲▲▲ 変更点 ▲▲▲


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

    # ▼▼▼ 変更点: UIコードを削除し、progress_callbackを受け取るように変更 ▼▼▼
    def import_csv_with_progress(self, filepath, encoding='utf-8', delimiter=',', progress_callback=None):
        self.cancelled = False

        try:
            # Step 1: 行数のカウント（これも時間がかかるため進捗を通知）
            if progress_callback:
                progress_callback("行数をカウント中...", 0, 1) # ステータス、現在値、最大値
            
            total_rows = 0
            with open(filepath, 'r', encoding=encoding, errors='ignore') as f:
                # 巨大ファイルの場合、readlineでのカウントも時間がかかるため、
                # ここでは簡略化のため、既存の方法を踏襲するが、
                # 理想はファイルサイズベースでの進捗管理
                total_rows = sum(1 for _ in f) -1

            if self.cancelled: return None, 0
            
            if total_rows <= 0:
                return None, 0

            # Step 2: CSVのインポート
            if progress_callback:
                progress_callback(f"データベースにインポート中... (0%)", 0, total_rows)

            df_sample = pd.read_csv(filepath, nrows=0, encoding=encoding, sep=delimiter)
            columns = df_sample.columns.tolist()
            self._create_table(columns)
            self.header = columns
            
            chunk_size = 50000
            processed_rows = 0
            
            reader = pd.read_csv(filepath, chunksize=chunk_size, encoding=encoding, dtype=str, sep=delimiter, on_bad_lines='skip')

            for chunk in reader:
                if self.cancelled:
                    break
                
                chunk.to_sql(self.table_name, self.conn, if_exists='append', index=False)
                processed_rows += len(chunk)
                
                if progress_callback:
                    percentage = (processed_rows / total_rows * 100) if total_rows > 0 else 0
                    status_text = f"データベースにインポート中... ({percentage:.1f}%)"
                    progress_callback(status_text, processed_rows, total_rows)
            
            if self.cancelled:
                self.close()
                return None, 0

            # Step 3: インデックスの作成
            if progress_callback:
                progress_callback("インデックスを構築中... (高速化処理)", 0, len(columns))

            for i, col in enumerate(columns):
                if self.cancelled: break
                try:
                    self.conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{col}" ON {self.table_name}("{col}")')
                except sqlite3.OperationalError as e:
                    print(f"Could not create index on column '{col}': {e}")
                if progress_callback:
                    progress_callback(f"インデックスを構築中... ({col})", i + 1, len(columns))

            if self.cancelled:
                self.close()
                return None, 0
                
            self.conn.commit()
            return columns, processed_rows
        except Exception as e:
            self.close()
            raise e
    # ▲▲▲ 変更点 ▲▲▲

    def _create_table(self, columns):
        column_defs = ", ".join([f'"{col}" TEXT' for col in columns])
        create_sql = f"CREATE TABLE {self.table_name} ({column_defs})"
        self.conn.execute(f"DROP TABLE IF EXISTS {self.table_name}")
        self.conn.execute(create_sql)
    
    def _create_indexes(self, columns):
        # import_csv_with_progress 内に移動したため、このメソッドは直接は使われない
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
        
        for col_idx, col_name in enumerate(self.header):
            # 検索対象列が指定されている場合、それ以外の列はスキップ
            if columns and col_name not in columns:
                continue

            where_clause = f'"{col_name}" LIKE ?'
            params = [like_term]
            
            if not case_sensitive:
                where_clause = f'LOWER("{col_name}") LIKE ?'
                params = [like_term.lower()]

            # rowidは1から始まるので、0ベースのインデックスにするために -1 する
            query = f"SELECT rowid - 1 FROM {self.table_name} WHERE {where_clause}"
            
            try:
                cursor = self.conn.execute(query, params)
                for row in cursor:
                    search_results.append((row[0], self.header.index(col_name)))
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
                    # この関数は接続ごとに登録が必要
                    self.conn.create_function("REGEXP_REPLACE", 3, regexp_replace)
                except sqlite3.NotSupportedError:
                     # 既に登録されている場合など
                     pass

            cursor.execute('BEGIN TRANSACTION')

            for col_name in target_columns:
                if is_regex:
                    sql = f'UPDATE "{self.table_name}" SET "{col_name}" = REGEXP_REPLACE(?, ?, "{col_name}") WHERE "{col_name}" IS NOT NULL'
                    params = (search_term, replace_term)
                else:
                    # SQLiteのREPLACEはデフォルトで大文字小文字を区別する
                    # 区別しない置換は少し複雑になるが、ここではINSTRで検索対象を絞ることで対応
                    if is_case_sensitive:
                        sql = f'UPDATE "{self.table_name}" SET "{col_name}" = REPLACE("{col_name}", ?, ?) WHERE INSTR("{col_name}", ?) > 0'
                        params = (search_term, replace_term, search_term)
                    else:
                        # SQLiteには標準で大文字小文字を区別しないREPLACEはないため、
                        # まずSELECTで対象行を見つけてからUPDATEするか、あるいは単純に全行に適用する
                        # ここでは簡潔さのため、LOWERを使って検索対象を絞る
                        sql = f'UPDATE "{self.table_name}" SET "{col_name}" = REPLACE("{col_name}", ?, ?) WHERE INSTR(LOWER("{col_name}"), LOWER(?)) > 0'
                        params = (search_term, replace_term, search_term)


                cursor.execute(sql, params)
                # 影響を受けた行数を取得
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
                
                # rowid は 1から始まるので、row_idxに+1する
                sql = f'UPDATE "{self.table_name}" SET "{col_name}" = ? WHERE rowid = ?'
                cursor.execute(sql, (new_value, row_idx + 1))
            
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"DB update failed: {e}")
            raise

    def get_rows_by_ids(self, indices):
        if not indices: return pd.DataFrame(columns=self.header)
        
        # indicesがソートされていない可能性も考慮
        unique_indices = sorted(list(set(indices)))
        
        # rowidは1から始まるので、+1する
        params = [i + 1 for i in unique_indices]
        placeholders = ','.join('?' * len(params))
        
        query = f'SELECT rowid, * FROM {self.table_name} WHERE rowid IN ({placeholders})'
        
        df = pd.read_sql_query(query, self.conn, params=params)
        
        if df.empty:
            return pd.DataFrame(columns=self.header)
        
        # rowidを0ベースのインデックスに変換
        df.set_index(df['rowid'] - 1, inplace=True)
        df.drop(columns=['rowid'], inplace=True)
        
        if set(self.header).issubset(df.columns):
            df = df[self.header]
        
        # 元のindicesの順序を維持して返す
        return df.reindex(indices)

    def get_all_indices(self):
        query = f"SELECT rowid - 1 FROM {self.table_name}"
        if self.sort_info and self.sort_info['column'] in self.header:
            from PySide6.QtCore import Qt # ここでインポート
            order_str = "ASC" if self.sort_info['order'] == Qt.AscendingOrder else "DESC"
            query += f' ORDER BY "{self.sort_info["column"]}" {order_str}'
        else:
            query += " ORDER BY rowid"

        cursor = self.conn.execute(query)
        return [row[0] for row in cursor]

    def get_total_rows(self):
        try:
            return self.conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()[0]
        except (sqlite3.OperationalError, IndexError):
            # テーブルが存在しない場合など
            return 0

    def insert_rows(self, row_pos, count, headers):
        # SQLiteでは特定の行位置への挿入は直接サポートされていない。
        # 全データを再構築する必要があり、非常に高コスト。
        # ここでは単純に末尾に追加する実装とするか、エラーとするのが現実的。
        # Undo/Redoを考慮すると、この操作はさらに複雑になる。
        # ここでは、簡略化のため末尾追加とする。
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

            # 新しいヘッダーリストに存在する列を古いテーブルから選択
            # 存在しない列は空文字列として選択
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
                
                # データを新しいテーブルに挿入
                insert_sql = f'INSERT INTO {temp_table_name} ({", ".join(f"{h}" for h in new_headers)}) {select_from_old_table_sql}'
                cursor.execute(insert_sql)

                # 進捗通知のロジックは、一括INSERTのためここでは簡略化
                if progress_callback:
                    progress_callback(total_rows, total_rows)

            cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            cursor.execute(f"ALTER TABLE {temp_table_name} RENAME TO {self.table_name}")
            self.header = new_headers

            # 新しいテーブルにインデックスを再作成
            if progress_callback:
                progress_callback(f"インデックスを再構築中...", 0, len(new_headers))
            for i, col in enumerate(new_headers):
                if self.cancelled: break
                try:
                    self.conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{col}" ON {self.table_name}("{col}")')
                except sqlite3.OperationalError as e:
                     print(f"Could not create index on column '{col}': {e}")
                if progress_callback:
                    progress_callback(f"インデックスを再構築中... ({col})", i + 1, len(new_headers))

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
        # recreate_table_with_new_columns がUIスレッドをブロックしないように修正が必要
        # ここでは一旦そのまま呼び出す
        return self.recreate_table_with_new_columns(new_full_headers, old_headers_order, 
                                                     progress_callback=None) # コールバックを渡す口が必要

    def delete_columns(self, col_names_to_delete: list, new_full_headers: list):
        old_headers_order = list(self.header)
        # SQLite 3.35.0+ なら DROP COLUMNが使える
        # if sqlite3.sqlite_version_info >= (3, 35, 0): ...
        return self.recreate_table_with_new_columns(new_full_headers, old_headers_order,
                                                     progress_callback=None)

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
            with open(lookup_filepath, 'r', encoding=lookup_encoding, errors='ignore') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = row.get(lookup_key_col)
                    val = row.get(replace_val_col)
                    if key is not None and val is not None:
                        processed_key = key.strip().lower()
                        if processed_key not in lookup_dict: # 重複キーは最初の一つを優先
                            lookup_dict[processed_key] = val
            
            if not lookup_dict:
                return True, [], 0 # 参照ファイルが空

            # 2. 本体テーブルから更新対象の行を特定
            update_targets = []
            # rowid を使うことで高速に反復処理
            read_cursor = self.conn.cursor()
            query = f'SELECT rowid, "{target_col}" FROM "{self.table_name}"'
            read_cursor.execute(query)

            total_rows = self.get_total_rows()
            processed_rows = 0
            if progress_callback:
                progress_callback("更新対象を検索中...", 0, total_rows)

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
                
                processed_rows += len(rows_chunk)
                if progress_callback:
                    progress_callback("更新対象を検索中...", processed_rows, total_rows)
            
            if not update_targets:
                return True, [], 0 # 更新対象なし

            # 3. 特定した行を一括で更新
            if progress_callback:
                progress_callback("データベースを更新中...", 0, len(update_targets))

            cursor.execute('BEGIN TRANSACTION')
            update_sql = f'UPDATE "{self.table_name}" SET "{target_col}" = ? WHERE rowid = ?'
            cursor.executemany(update_sql, update_targets)
            self.conn.commit()
            
            if progress_callback:
                progress_callback("データベースを更新中...", len(update_targets), len(update_targets))


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

    # ▼▼▼ 変更点: UIスレッドをブロックしないようにprogress_callbackを追加 ▼▼▼
    def export_to_csv(self, filepath, encoding='utf-8', quoting_style=csv.QUOTE_MINIMAL, progress_callback=None, line_terminator='\r\n'):
        """
        メモリ効率の良いストリーミング方式でCSVにエクスポートする。
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"PRAGMA table_info({self.table_name})")
            columns = [row[1] for row in cursor]

            total_rows = self.get_total_rows()
            if progress_callback:
                progress_callback(0, total_rows)

            with open(filepath, 'w', encoding=encoding, newline='') as f:
                # 🔥 改行コードの修正: line_terminator → lineterminator
                writer = csv.writer(f, quoting=quoting_style, lineterminator=line_terminator)
                writer.writerow(columns)

                query = f"SELECT * FROM {self.table_name}"
                cursor.execute(query)

                chunk_size = 50000
                processed_rows = 0
                
                while True:
                    rows_chunk = cursor.fetchmany(chunk_size)
                    if not rows_chunk:
                        break
                    writer.writerows(rows_chunk)
                    processed_rows += len(rows_chunk)

                    if progress_callback:
                        progress_callback(processed_rows, total_rows)
                
                if progress_callback:
                    progress_callback(total_rows, total_rows)

            return True
        except Exception as e:
            print(f"Error exporting to CSV: {e}")
            raise
    # ▲▲▲ 変更点 ▲▲▲