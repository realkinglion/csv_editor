# db_backend.py

import sqlite3
import pandas as pd
import tempfile
import os
import csv
import time
import re
import traceback
import subprocess
import platform


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
        self.encoding = 'utf-8'

        # パフォーマンス向上のためのPRAGMA設定（大幅強化）
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-256000")  # 256MB cache（4倍増強）
        self.conn.execute("PRAGMA mmap_size=536870912")  # 512MB memory mapping
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA optimize")  # 自動最適化

    def import_csv_with_progress(self, filepath, encoding='utf-8', delimiter=',', progress_callback=None):
        self.cancelled = False
        self.encoding = encoding

        try:
            # Step 1: 行数のカウント
            if progress_callback:
                progress_callback("ファイルサイズを確認中...", 0, 100)
            
            total_rows = self._fast_line_count(filepath)
            
            if self.cancelled:
                return None, 0
            
            if total_rows <= 0:
                return None, 0

            # Step 2: CSVのインポート
            if progress_callback:
                progress_callback(f"データベース準備中... (全{total_rows:,}行)", 5, 100)
            
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
                    percentage = (processed_rows / total_rows * 90) if total_rows > 0 else 0
                    status_text = f"データベースにインポート中... ({percentage:.1f}%)"
                    progress_callback(status_text, 5 + int(percentage * 0.95), 100)

            if self.cancelled:
                self.close()
                return None, 0

            # Step 3: インデックスの作成
            if progress_callback:
                progress_callback("インデックスを構築中... (高速化処理)", 95, 100)
            
            for i, col in enumerate(columns):
                if self.cancelled:
                    break
                try:
                    # エスケープ処理をf-string外で実行
                    escaped_col = col.replace('"', '""')
                    index_name = f'idx_{i}'  # インデックス名を簡素化
                    self.conn.execute(f'CREATE INDEX IF NOT EXISTS "{index_name}" ON {self.table_name}("{escaped_col}")')
                except sqlite3.OperationalError as e:
                    print(f"Could not create index on column '{col}': {e}")
                
                if progress_callback:
                    col_percentage = ((i + 1) / len(columns)) * 5
                    progress_callback(f"インデックスを構築中... ({col})", 95 + int(col_percentage), 100)

            if self.cancelled:
                self.close()
                return None, 0

            self.conn.commit()
            return columns, processed_rows

        except Exception as e:
            self.close()
            raise e

    def _fast_line_count(self, filepath):
        """OSネイティブコマンドを使った高速行数カウント"""
        try:
            if platform.system() == 'Windows':
                result = subprocess.run(
                    ['powershell', '-Command', f'(Get-Content -LiteralPath "{filepath}" | Measure-Object -Line).Lines'],
                    capture_output=True, text=True, check=True,
                    creationflags=subprocess.CREATE_NO_WINDOW
                )
                return int(result.stdout.strip()) - 1 if int(result.stdout.strip()) > 0 else 0
            else:
                # Unix系はwcコマンド
                result = subprocess.run(
                    ['wc', '-l', filepath],
                    capture_output=True, text=True, check=True
                )
                return int(result.stdout.split()[0]) - 1
        except (subprocess.CalledProcessError, FileNotFoundError, ValueError) as e:
            print(f"WARNING: Fast line count failed using OS command: {e}. Falling back to Python.")
            try:
                with open(filepath, 'r', encoding=self.encoding, errors='ignore') as f:
                    count = -1
                    buf_size = 1024 * 1024
                    while True:
                        data = f.read(buf_size)
                        if not data:
                            break
                        count += data.count('\n')
                    return count if count >= 0 else 0
            except Exception as e_fallback:
                print(f"ERROR: Fallback line count also failed: {e_fallback}")
                return 0

    def _create_table(self, columns):
        """テーブル作成時の列名エスケープをより堅牢に"""
        # f-string外でエスケープ処理
        sanitized_column_defs = []
        for col in columns:
            sanitized_col_name = col.replace('"', '""')
            sanitized_column_defs.append(f'"{sanitized_col_name}" TEXT')
        
        column_defs_str = ", ".join(sanitized_column_defs)
        create_sql = f"CREATE TABLE {self.table_name} ({column_defs_str})"
        self.conn.execute(f"DROP TABLE IF EXISTS {self.table_name}")
        self.conn.execute(create_sql)

    def _create_indexes(self, columns):
        for col in columns:
            try:
                escaped_col = col.replace('"', '""')
                self.conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{escaped_col}" ON {self.table_name}("{escaped_col}")')
            except sqlite3.OperationalError as e:
                print(f"Could not create index on column '{col}': {e}")

    def set_sort_order(self, column_name, order):
        """UIからのソート指示を受け取り、状態を保存する"""
        if column_name is None:
            self.sort_info = None
        else:
            self.sort_info = {'column': column_name, 'order': order}

    def search(self, search_term, columns=None, case_sensitive=True, is_regex=False):
        """最適化された複数列検索"""
        print(f"DEBUG: SQLite search - term: '{search_term}', columns: {columns}, case_sensitive: {case_sensitive}, is_regex: {is_regex}")
        
        # デバッグ用データ検証（一時的に有効化して確認後、コメントアウトまたは削除推奨）
        # self.debug_data_verification() 
        
        if not columns:
            columns = self.header
            print(f"DEBUG: 検索対象列数: {len(columns)}")
        
        # 列数による処理方法の最適化
        if len(columns) > 20:
            chunk_size = 10000  # 大量列の場合はチャンクサイズを調整
        else:
            chunk_size = 50000
        
        if is_regex:
            return self._search_regex_optimized(search_term, columns, case_sensitive, chunk_size)
        else:
            return self._search_like_optimized(search_term, columns, case_sensitive)

    def _search_like_optimized(self, search_term, columns, case_sensitive):
        """LIKE検索の最適化（UNION ALL使用）"""
        search_results = []
        like_term = f'%{search_term}%'
        
        # 複数列をUNION ALLで効率的に検索
        union_queries = []
        params = []
        
        for col_name in columns:
            if col_name not in self.header:
                continue
            
            col_idx = self.header.index(col_name)
            escaped_col_name = col_name.replace('"', '""')
            
            if case_sensitive:
                condition = f'"{escaped_col_name}" LIKE ?'
            else:
                condition = f'LOWER("{escaped_col_name}") LIKE LOWER(?)'
            
            union_queries.append(f"""
                SELECT rowid - 1 as row_idx, {col_idx} as col_idx
                FROM {self.table_name}
                WHERE {condition}
            """)
            params.append(like_term)
        
        if union_queries:
            full_query = " UNION ALL ".join(union_queries)
            try:
                cursor = self.conn.execute(full_query, params)
                search_results = [(row[0], row[1]) for row in cursor]
            except sqlite3.OperationalError as e:
                print(f"ERROR: 複数列検索エラー: {e}")
                # フォールバック処理
                return self._search_like_fallback(search_term, columns, case_sensitive)
        
        return search_results

    def _search_like_fallback(self, search_term, columns, case_sensitive):
        """UNION ALLが失敗した場合のフォールバック処理（既存の単一列検索をループ）"""
        search_results = []
        like_term = f'%{search_term}%'
        
        for col_name in columns:
            if col_name not in self.header:
                continue
            
            escaped_col_name = col_name.replace('"', '""')
            
            if case_sensitive:
                query = f'SELECT rowid - 1 FROM {self.table_name} WHERE "{escaped_col_name}" LIKE ?'
                params = [like_term]
            else:
                query = f'SELECT rowid - 1 FROM {self.table_name} WHERE LOWER("{escaped_col_name}") LIKE LOWER(?)'
                params = [like_term]
            
            try:
                cursor = self.conn.execute(query, params)
                col_idx = self.header.index(col_name) if col_name in self.header else 0 # 列名から列インデックスを取得
                for row in cursor:
                    search_results.append((row[0], col_idx)) # (row_index, column_index)形式で追加
            except sqlite3.OperationalError as e:
                print(f"ERROR: 列 '{col_name}' の検索エラー (フォールバック): {e}")
        return search_results

    def _search_regex_optimized(self, search_term, columns, case_sensitive, chunk_size):
        """正規表現検索の最適化（Pandasチャンク処理）"""
        search_results = []
        total_rows = self.get_total_rows()

        import re
        try:
            flags = 0
            if not case_sensitive:
                flags |= re.IGNORECASE
            if '^' in search_term or '$' in search_term:
                flags |= re.MULTILINE
            pattern = re.compile(search_term, flags)
        except re.error as e:
            print(f"正規表現エラー: {e}")
            return []
        
        valid_target_columns = [col for col in columns if col in self.header]
        if not valid_target_columns:
            print("WARNING: 検索対象の有効な列がありません。")
            return []

        # チャンクごとにデータを読み込み、Pandasで正規表現検索
        for offset in range(0, total_rows, chunk_size):
            if hasattr(self, 'cancelled') and self.cancelled:
                break
            
            limit = min(chunk_size, total_rows - offset)
            
            # 検索対象列とrowidのみを読み込むクエリを生成
            select_cols_quoted = []
            for col in valid_target_columns:
                escaped_col = col.replace('"', '""')
                select_cols_quoted.append(f'"{escaped_col}"')
            
            # SQLクエリ
            query = f'''
                SELECT rowid, {", ".join(select_cols_quoted)}
                FROM {self.table_name}
                LIMIT {limit} OFFSET {offset}
            '''
            
            chunk_df = pd.read_sql_query(query, self.conn)
            if chunk_df.empty:
                continue
            
            # Pandasのstr.containsで高速正規表現マッチング
            for col_name in valid_target_columns:
                if col_name in chunk_df.columns:
                    matched_mask = chunk_df[col_name].astype(str).str.contains(pattern, na=False, regex=True)
                    
                    if matched_mask.any():
                        for idx in chunk_df[matched_mask].index:
                            rowid = chunk_df.loc[idx, 'rowid']
                            # 列名から列インデックスを正確に取得
                            col_idx = self.header.index(col_name) if col_name in self.header else 0
                            search_results.append((rowid - 1, col_idx)) # rowidは1から始まるため-1する
            
            # 進捗通知
            if hasattr(self, 'app') and hasattr(self.app, 'async_manager'):
                progress_value = min(100, int(((offset + limit) / total_rows) * 100))
                status = f"正規表現検索中... ({offset + limit:,}/{total_rows:,}行)"
                try:
                    self.app.async_manager.task_progress.emit(status, progress_value, 100)
                except:
                    pass
                
        print(f"DEBUG: 検索完了 - 合計 {len(search_results)} 件")
        return search_results

    def execute_replace_all_in_db(self, settings):
        """チャンク処理による高速置換（最適化版）"""
        import pandas as pd
        import re
        
        search_term = settings["search_term"]
        replace_term = settings["replace_term"]
        target_columns = settings["target_columns"]
        is_regex = settings["is_regex"]
        is_case_sensitive = settings["is_case_sensitive"]
        
        print(f"DEBUG: execute_replace_all_in_db called with settings: {settings}")
        
        if not search_term or not target_columns:
            return False, 0, []
        
        # 正規表現パターンの事前コンパイル（重要な最適化）
        try:
            if is_regex:
                flags = 0
                if not is_case_sensitive:
                    flags |= re.IGNORECASE
                if '^' in search_term or '$' in search_term:
                    flags |= re.MULTILINE
                pattern = re.compile(search_term, flags)
            else:
                pattern = re.compile(re.escape(search_term), 
                                     0 if is_case_sensitive else re.IGNORECASE)
        except re.error as e:
            print(f"正規表現エラー: {e}")
            return False, 0, []
        
        total_rows = self.get_total_rows()
        total_updated_count = 0
        chunk_size = 50000
        
        cursor = self.conn.cursor()
        
        # 🔥 追加: Undo用の変更履歴を収集
        all_changes_for_undo = []

        try:
            cursor.execute('BEGIN TRANSACTION')
            
            # チャンクごに処理
            for offset in range(0, total_rows, chunk_size):
                # キャンセル処理
                if hasattr(self, 'cancelled') and self.cancelled:
                    break
                
                # チャンクを効率的に読み込み
                limit = min(chunk_size, total_rows - offset)
                
                escaped_select_cols = []
                for col in target_columns:
                    escaped_col = col.replace('"', '""')
                    escaped_select_cols.append(f'"{escaped_col}"')
                
                select_cols = ['rowid'] + escaped_select_cols
                
                query = f'''
                    SELECT {", ".join(select_cols)}
                    FROM {self.table_name}
                    LIMIT {limit} OFFSET {offset}
                '''
                
                chunk_df = pd.read_sql_query(query, self.conn)
                if chunk_df.empty:
                    continue
                
                # 🔥 重要: rowidを一意のインデックスとして設定
                chunk_df.set_index('rowid', inplace=True)
                
                # Pandasで超高速処理
                changes_in_chunk = []
                
                for col in target_columns:
                    if col in chunk_df.columns:
                        # ベクトル化された置換処理
                        old_values = chunk_df[col].astype(str).fillna('')
                        new_values = old_values.str.replace(pattern, replace_term, regex=True)
                        
                        # 変更があった行のみを特定
                        changed_mask = old_values != new_values
                        
                        if changed_mask.any():
                            # 🔥 修正: インデックス（rowid）を直接使用
                            for rowid in chunk_df[changed_mask].index:
                                new_value = str(new_values.loc[rowid])
                                old_value = str(old_values.loc[rowid]) # old_values Seriesはrowidをインデックスとして持っているはず
                                
                                # 🔥 追加: Undo用データの収集
                                all_changes_for_undo.append({
                                    'item': str(rowid - 1),  # SQLiteのrowidは1から始まるため-1する
                                    'column': col,
                                    'old': str(old_value),
                                    'new': str(new_value)
                                })

                                changes_in_chunk.append((new_value, rowid, col))
                                print(f"DEBUG: 置換検出 - rowid: {rowid} (type: {type(rowid)}), "
                                      f"col: {col}, old: '{old_value}', new: '{new_value}'")
                
                # バッチで効率的に更新
                if changes_in_chunk:
                    # 列ごとにグループ化して一括更新
                    by_column = {}
                    for new_value_item, rowid_item, col_item in changes_in_chunk:
                        if col_item not in by_column:
                            by_column[col_item] = []
                        safe_new_value = str(new_value_item)
                        safe_rowid = int(rowid_item)
                        by_column[col_item].append((safe_new_value, safe_rowid))
                    
                    # executemanyで高速バッチ更新
                    for col_to_update, updates_list in by_column.items():
                        escaped_col = col_to_update.replace('"', '""')
                        sql = f'UPDATE {self.table_name} SET "{escaped_col}" = ? WHERE rowid = ?'
                        
                        print(f"DEBUG: SQL実行準備 - 列: {col_to_update}, 更新件数: {len(updates_list)}")
                        
                        try:
                            cursor.executemany(sql, updates_list)
                            print(f"DEBUG: SQL実行成功 - 列: {col_to_update}")
                        except Exception as sql_error:
                            print(f"ERROR: SQL実行失敗 - 列: {col_to_update}, エラー: {sql_error}")
                            raise
                    
                    total_updated_count += len(changes_in_chunk)
                
                # プログレス更新
                if hasattr(self, 'app'):
                    progress = min(100, int(((offset + limit) / total_rows) * 100))
                    status = f"高速処理中... ({offset + limit:,}/{total_rows:,}行)"
                    try:
                        self.app.async_manager.task_progress.emit(status, progress, 100)
                    except:
                        pass
            
            self.conn.commit()
            print(f"DEBUG: 置換完了 - 合計 {total_updated_count} 件を更新")
            
            # 🔥 修正: 変更履歴も返す
            return True, total_updated_count, all_changes_for_undo
            
        except Exception as e:
            self.conn.rollback()
            print(f"チャンク処理エラー: {e}")
            import traceback
            traceback.print_exc()
            # 🔥 修正: 変更履歴も返すように変更
            return False, 0, []

    def update_cells(self, changes: list):
        """バッチ更新による高速化（セキュリティ強化版）"""
        if not changes:
            return
        
        cursor = self.conn.cursor()
        try:
            cursor.execute('BEGIN TRANSACTION')
            
            # 列ごとにグループ化してexecutemanyで一括更新
            updates_by_column = {}
            for change in changes:
                col_name = change['col_name']
                # セキュリティ強化: 列名が正当かチェック
                if col_name not in self.header:
                    print(f"WARNING: 不正な列名 '{col_name}' が検出されました。スキップします。")
                    continue
                
                if col_name not in updates_by_column:
                    updates_by_column[col_name] = []
                updates_by_column[col_name].append((change['new_value'], change['row_idx'] + 1))

            for col_name, params_list in updates_by_column.items():
                # SQLインジェクション対策：列名エスケープ + プレースホルダー
                escaped_col_name = col_name.replace('"', '""')
                sql = f'UPDATE "{self.table_name}" SET "{escaped_col_name}" = ? WHERE rowid = ?'
                cursor.executemany(sql, params_list)

            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"DB update failed: {e}")
            raise

    def get_rows_by_ids(self, indices):
        if not indices:
            return pd.DataFrame(columns=self.header)

        unique_indices = sorted(list(set(indices)))
        params = [i + 1 for i in unique_indices]
        placeholders = ','.join('?' * len(params))

        # f-string外でエスケープ処理
        select_cols = []
        for h in self.header:
            escaped_h = h.replace('"', '""')
            select_cols.append(f'"{escaped_h}"')

        select_cols_str = ", ".join(select_cols)
        query = f'SELECT rowid, {select_cols_str} FROM {self.table_name} WHERE rowid IN ({placeholders})'

        df = pd.read_sql_query(query, self.conn, params=params)

        if df.empty:
            return pd.DataFrame(columns=self.header)

        df.set_index(df['rowid'] - 1, inplace=True)
        df.drop(columns=['rowid'], inplace=True)

        if set(self.header).issubset(df.columns):
            df = df[self.header]

        return df.reindex(indices)

    def get_all_indices(self):
        query = f"SELECT rowid - 1 FROM {self.table_name}"
        if self.sort_info and self.sort_info['column'] in self.header:
            from PySide6.QtCore import Qt
            escaped_col = self.sort_info['column'].replace('"', '""')
            order_str = "ASC" if self.sort_info['order'] == Qt.AscendingOrder else "DESC"
            query += f' ORDER BY "{escaped_col}" {order_str}'
        else:
            query += " ORDER BY rowid" # ORDER BY BY -> ORDER BY に修正

        cursor = self.conn.execute(query)
        return [row[0] for row in cursor]

    def get_total_rows(self):
        try:
            return self.conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()[0]
        except (sqlite3.OperationalError, IndexError):
            return 0

    def insert_rows(self, row_pos, count, headers):
        cursor = self.conn.cursor()
        try:
            cursor.execute('BEGIN TRANSACTION')

            # f-string外でエscape処理
            header_cols_quoted = []
            for h in headers:
                escaped_h = h.replace('"', '""')
                header_cols_quoted.append(f'"{escaped_h}"')

            placeholders = ','.join(['?'] * len(headers))
            header_cols_str = ",".join(header_cols_quoted)
            sql = f'INSERT INTO "{self.table_name}" ({header_cols_str}) VALUES ({placeholders})'

            for _ in range(count):
                cursor.execute(sql, [""] * len(headers))

            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"DB insert_rows failed: {e}")
            raise

    def remove_rows(self, row_indices):
        if not row_indices:
            return False
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
        """テーブル構造を再作成（列の追加・削除用）"""
        temp_table_name = "temp_csv_data_rebuild"

        cursor = self.conn.cursor()
        try:
            cursor.execute('BEGIN TRANSACTION')

            # 新しいテーブルの列定義
            new_column_defs = []
            for col in new_headers:
                escaped_col = col.replace('"', '""')
                new_column_defs.append(f'"{escaped_col}" TEXT')

            column_defs_str = ", ".join(new_column_defs)
            create_temp_sql = f"CREATE TABLE {temp_table_name} ({column_defs_str})"
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            cursor.execute(create_temp_sql)

            # SELECT文の列リスト作成
            select_columns = []
            for h in new_headers:
                escaped_h = h.replace('"', '""')
                if h in old_headers_order:
                    select_columns.append(f'"{escaped_h}"')
                else:
                    # format()メソッドを使用してエスケープの問題を回避
                    select_columns.append("'' AS \"{}\"".format(escaped_h))

            total_rows = self.get_total_rows()

            if total_rows > 0:
                select_columns_str = ", ".join(select_columns)
                select_from_old_table_sql = f"SELECT {select_columns_str} FROM {self.table_name}"

                # INSERT文の列リスト
                insert_columns = []
                for h in new_headers:
                    escaped_h = h.replace('"', '""')
                    insert_columns.append(f'"{escaped_h}"')

                insert_columns_str = ", ".join(insert_columns)
                insert_sql = f'INSERT INTO {temp_table_name} ({insert_columns_str}) {select_from_old_table_sql}'
                cursor.execute(insert_sql)

                if progress_callback:
                    progress_callback(f"テーブルを再構築中...", 90, 100)

            cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            cursor.execute(f"ALTER TABLE {temp_table_name} RENAME TO {self.table_name}")
            self.header = new_headers

            # 新しいテーブルにインデックスを再作成
            if progress_callback:
                progress_callback(f"インデックスを再構築中...", 95, 100)

            for i, col in enumerate(new_headers):
                if self.cancelled:
                    break
                try:
                    escaped_col = col.replace('"', '""')
                    index_name = f'idx_{i}'  # 簡素化されたインデックス名
                    self.conn.execute(f'CREATE INDEX IF NOT EXISTS "{index_name}" ON {self.table_name}("{escaped_col}")')
                except sqlite3.OperationalError as e:
                    print(f"Could not create index on column '{col}': {e}")

                if progress_callback:
                    col_percentage = ((i + 1) / len(new_headers)) * 5
                    progress_callback(f"インデックスを再構築中... ({col})", 95 + int(col_percentage), 100)

            self.conn.commit()
            return True

        except Exception as e:
            self.conn.rollback()
            print(f"DB recreate_table_with_new_columns failed: {e}")
            raise

    def add_column_fast(self, column_name, default_value=''):
        """ALTER TABLEを使った高速な列追加"""
        try:
            escaped_col_name = column_name.replace('"', '""')
            self.conn.execute(
                f'ALTER TABLE {self.table_name} ADD COLUMN "{escaped_col_name}" TEXT DEFAULT ? ',
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
        return self.recreate_table_with_new_columns(new_full_headers, old_headers_order, progress_callback=None)

    def delete_columns(self, col_names_to_delete: list, new_full_headers: list):
        old_headers_order = list(self.header)
        return self.recreate_table_with_new_columns(new_full_headers, old_headers_order, progress_callback=None)

    def execute_replace_from_file_in_db(self, params, progress_callback=None):
        """データベース内で直接、ファイル参照置換を実行する。"""
        
        lookup_filepath = params['lookup_filepath']
        lookup_encoding = params['lookup_file_encoding']
        target_col = params['target_col']
        lookup_key_col = params['lookup_key_col']
        replace_val_col = params['replace_val_col']

        cursor = self.conn.cursor()
        try:
            # 1. 参照ファイルを読み込み
            lookup_dict = {}
            with open(lookup_filepath, 'r', encoding=lookup_encoding, errors='ignore') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = row.get(lookup_key_col)
                    val = row.get(replace_val_col)
                    if key is not None and val is not None:
                        processed_key = key.strip().lower()
                        if processed_key not in lookup_dict:
                            lookup_dict[processed_key] = val

            if not lookup_dict:
                return True, [], 0

            # 2. 本体テーブルから更新対象の行を特定
            update_targets = []
            read_cursor = self.conn.cursor()
            escaped_target_col = target_col.replace('"', '""')
            query = f'SELECT rowid, "{escaped_target_col}" FROM "{self.table_name}"'
            read_cursor.execute(query)

            total_rows = self.get_total_rows()
            processed_rows = 0

            while True:
                rows_chunk = read_cursor.fetchmany(10000)
                if not rows_chunk:
                    break

                for rowid, cell_value in rows_chunk:
                    if cell_value is not None:
                        processed_cell = str(cell_value).strip().lower()
                        if processed_cell in lookup_dict:
                            new_value = lookup_dict[processed_cell]
                            if str(cell_value) != new_value:
                                update_targets.append((new_value, rowid))

                processed_rows += len(rows_chunk)
                if progress_callback:
                    progress_callback("更新対象を検索中...", processed_rows, total_rows)

            if not update_targets:
                return True, [], 0

            # 3. 特定した行を一括で更新
            if progress_callback:
                progress_callback("データベースを更新中...", 0, len(update_targets))

            cursor.execute('BEGIN TRANSACTION')
            
            # 完全なSQL文を構築
            update_sql = f'UPDATE "{self.table_name}" SET "{escaped_target_col}" = ? WHERE rowid = ?'
            
            total_updated_count = 0
            for i, (new_value, rowid) in enumerate(update_targets):
                cursor.execute(update_sql, (new_value, rowid))
                total_updated_count += 1
                
                if i % 1000 == 0 and progress_callback:
                    progress_callback("データベースを更新中...", i, len(update_targets))

            self.conn.commit()
            return True, [], total_updated_count

        except Exception as e:
            self.conn.rollback()
            print(f"DB execute_replace_from_file_in_db failed: {e}")
            return False, 0
            
    # 完全削除：以下の関数は削除してください
    # def regexp_match(pattern_str, string):
    #     if string is None:
    #         return False
    #     try:
    #         if len(string) > 10000:
    #             return False
    #         return bool(re.search(pattern_str, string, flags))
    #     except Exception as e:
    #         print(f"WARNING: 正規表現マッチエラー: {e}")
    #         return False
    # self.conn.create_function("REGEXP_MATCH", 2, regexp_match) # この行も削除

    def close(self):
        """リソースのクリーンアップ"""
        if self.conn:
            self.conn.close()
            self.conn = None
        if os.path.exists(self.db_file):
            try:
                os.remove(self.db_file)
            except OSError as e:
                print(f"Error removing temporary database file {self.db_file}: {e}")

    def __del__(self):
        self.close()

    def debug_data_verification(self): # 新規追加
        """データベースの内容を確認（デバッグ用）""" # 新規追加
        try: # 新規追加
            # テーブルの行数確認 # 新規追加
            count_result = self.conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()[0] # 新規追加
            print(f"DEBUG: SQLiteテーブル総行数: {count_result}") # 新規追加
            
            # 最初の5行を表示 # 新規追加
            sample_result = self.conn.execute(f"SELECT * FROM {self.table_name} LIMIT 5").fetchall() # 新規追加
            print(f"DEBUG: サンプルデータ（最初の5行）: {sample_result}") # 新規追加
            
            # 特定の検索対象データの確認 # 新規追加
            search_result = self.conn.execute(f'SELECT rowid, * FROM {self.table_name} WHERE "商品番号" LIKE "%00-012%"').fetchall() # 新規追加
            print(f"DEBUG: '00-012'を含む行: {search_result}") # 新規追加
            
        except Exception as e: # 新規追加
            print(f"DEBUG: データベース確認エラー: {e}") # 新規追加

    def debug_verify_data(self, search_term): # 新規追加
        """デバッグ用：データベース内の特定データを確認""" # 新規追加
        try: # 新規追加
            result = self.conn.execute( # 新規追加
                f'SELECT rowid, "商品番号" FROM {self.table_name} WHERE "商品番号" LIKE ?', # 新規追加
                [f'%{search_term}%'] # 新規追加
            ).fetchall() # 新規追加
            print(f"DEBUG: データベース内の'{search_term}'を含む行: {result}") # 新規追加
            return result # 新規追加
        except Exception as e: # 新規追加
            print(f"DEBUG: データベース確認エラー: {e}") # 新規追加
            return [] # 新規追加