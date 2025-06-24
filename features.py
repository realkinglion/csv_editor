# features.py

import csv
import pandas as pd
from PySide6.QtCore import QObject, Signal, QRunnable, Slot, QCoreApplication
from PySide6.QtWidgets import QApplication
from concurrent.futures import ThreadPoolExecutor
import time
import re
import traceback

#==============================================================================
# 1. 非同期処理管理クラス
#==============================================================================
class Worker(QRunnable):
    """実行可能なワーカースレッド"""
    def __init__(self, fn, *args, **kwargs):
        super(Worker, self).__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    @Slot()
    def run(self):
        # ワーカースレッド内ではGUI関連の操作を行わない
        self.fn(*self.args, **self.kwargs)

class AsyncDataManager(QObject):
    """データ処理をバックグラウンドで実行し、UIの応答性を維持する"""
    data_ready = Signal(pd.DataFrame)
    progress_update = Signal(int)
    search_results_ready = Signal(list)
    analysis_results_ready = Signal(str)
    replace_from_file_completed = Signal(list, str)
    
    def __init__(self, app_instance):
        super().__init__()
        self.app = app_instance
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.current_load_mode = 'normal'
        self.backend_instance = None

    def load_full_dataframe_async(self, filepath, encoding, load_mode):
        self.current_load_mode = load_mode
        worker = Worker(self._do_load_full_df, filepath, encoding, load_mode)
        self.executor.submit(worker.run)

    def _do_load_full_df(self, filepath, encoding, load_mode):
        from db_backend import SQLiteBackend
        from lazy_loader import LazyCSVLoader
        
        df = None
        try:
            if load_mode == 'sqlite':
                self.backend_instance = SQLiteBackend(self.app)
                columns, _ = self.backend_instance.import_csv_with_progress(filepath, encoding)
                if columns is not None:
                    self.backend_instance.header = columns
            elif load_mode == 'lazy':
                self.backend_instance = LazyCSVLoader(filepath, encoding)
            else: # normal
                df = pd.read_csv(filepath, encoding=encoding, dtype=str).fillna('')
            
            self.data_ready.emit(df if df is not None else pd.DataFrame())
        except Exception as e:
            error_message = f"ファイル読み込みエラー: {e}"
            print(error_message)
            if QApplication.instance():
                QCoreApplication.instance().callLater(self.app.show_operation_status, error_message, 5000, True)
            self.data_ready.emit(pd.DataFrame())

    def search_data_async(self, settings: dict, current_load_mode: str, parent_child_data: dict, selected_rows: set):
        worker = Worker(self._do_search, settings, current_load_mode, parent_child_data, selected_rows)
        self.executor.submit(worker.run)

    def _do_search(self, settings: dict, current_load_mode: str, parent_child_data: dict, selected_rows: set):
        """ワーカースレッドで実行される検索処理。GUIアクセスは行わない。"""
        search_term = settings["search_term"]
        target_columns = settings["target_columns"]
        is_case_sensitive = settings["is_case_sensitive"]
        is_regex = settings["is_regex"]
        in_selection_only = settings["in_selection_only"]
        
        results = []
        
        try:
            if current_load_mode == 'sqlite':
                if self.backend_instance:
                    results = self.backend_instance.search(search_term, target_columns, is_case_sensitive)
            
            elif current_load_mode == 'lazy':
                if self.backend_instance:
                    results = self.backend_instance.search_in_file(
                        search_term, target_columns, is_case_sensitive, is_regex,
                        progress_callback=lambda p: self.progress_update.emit(p)
                    )
            
            else: # normal mode (Pandas DataFrame)
                df = self.app.table_model._dataframe
                if df is None or df.empty:
                    self.search_results_ready.emit([])
                    return

                pattern = re.compile(search_term if is_regex else re.escape(search_term),
                                     0 if is_case_sensitive else re.IGNORECASE)

                # 対象行の絞り込み (GUIへのアクセスなし)
                target_rows = set(range(df.shape[0]))
                if in_selection_only:
                    target_rows.intersection_update(selected_rows)

                headers = self.app.table_model._headers
                target_col_indices = {headers.index(name) for name in target_columns if name in headers}

                # 実際の検索処理
                for row_idx in sorted(list(target_rows)):
                    for col_idx in target_col_indices:
                        if col_idx < len(df.columns):
                            cell_value = df.iat[row_idx, col_idx]
                            if cell_value is not None and pattern.search(str(cell_value)):
                                results.append((row_idx, col_idx))

        except re.error as e:
            if QApplication.instance():
                QCoreApplication.instance().callLater(self.app.show_operation_status, f"正規表現エラー: {e}", 5000, True)
            self.search_results_ready.emit([])
            return
        except Exception as e:
            print(f"Error during search: {traceback.format_exc()}")
            if QApplication.instance():
                QCoreApplication.instance().callLater(self.app.show_operation_status, f"検索中にエラーが発生しました: {e}", 5000, True)
            self.search_results_ready.emit([])
            return

        self.search_results_ready.emit(results)

    def analyze_parent_child_async(self, db_backend_instance, column_name, mode):
        worker = Worker(self._do_analyze_parent_child_in_db, db_backend_instance, column_name, mode)
        self.executor.submit(worker.run)

    def _do_analyze_parent_child_in_db(self, db_backend_instance, column_name, mode):
        success, message, total_rows = self.app.parent_child_manager.analyze_relationships_in_db(
            db_backend_instance, column_name, mode,
            progress_callback=lambda p: self.progress_update.emit(p)
        )
        if success:
            self.analysis_results_ready.emit(self.app.parent_child_manager.get_groups_summary())
        else:
            self.analysis_results_ready.emit(f"分析エラー: {message}")
    
    def replace_from_file_async(self, db_backend_instance, current_dataframe, params):
        worker = Worker(self._do_replace_from_file, db_backend_instance, current_dataframe, params)
        self.executor.submit(worker.run)

    def _do_replace_from_file(self, db_backend_instance, current_dataframe, params):
        changes = []
        status_message = ""
        
        try:
            lookup_df = pd.read_csv(params['lookup_filepath'], encoding=params['lookup_file_encoding'], dtype=str, on_bad_lines='skip').fillna('')
            
            if db_backend_instance:
                success, temp_changes, updated_count = db_backend_instance.execute_replace_from_file_in_db(params)
                if success:
                    status_message = f"ファイル参照置換完了: {updated_count}件のセルを置換しました。"
                    self.replace_from_file_completed.emit([], status_message) 
                else:
                    status_message = "ファイル参照置換に失敗しました (データベースエラー)。"
                    self.replace_from_file_completed.emit([], status_message)
                return

            else: # ノーマルモード
                df_current_memory_temp = current_dataframe.copy()
                df_current_memory_temp['_merge_key'] = df_current_memory_temp[params['target_col']].astype(str).str.strip().str.lower()
                
                lookup_cols_for_merge = lookup_df[[params['lookup_key_col'], params['replace_val_col']]].copy()
                lookup_cols_for_merge['_merge_key'] = lookup_cols_for_merge[params['lookup_key_col']].astype(str).str.strip().str.lower()
                
                lookup_cols_for_merge.drop_duplicates(subset=['_merge_key'], inplace=True)

                new_value_col_name_in_merged_df = "temp_replaced_value_col"
                lookup_cols_for_merge.rename(columns={params['replace_val_col']: new_value_col_name_in_merged_df}, inplace=True)

                merged_df = df_current_memory_temp.merge(
                    lookup_cols_for_merge,
                    on='_merge_key',
                    how='left'
                )
                
                current_target_values = current_dataframe[params['target_col']].astype(str).fillna('')
                new_lookup_values = merged_df[new_value_col_name_in_merged_df].astype(str).fillna('')
                
                changed_mask = merged_df[new_value_col_name_in_merged_df].notna() & \
                               (current_target_values != new_lookup_values)
                
                changed_indices = current_dataframe.index[changed_mask]
                
                if changed_indices.empty:
                    status_message = "置換対象となるデータが見つかりませんでした。"
                    self.replace_from_file_completed.emit([], status_message)
                    return
                
                for row_idx in changed_indices:
                    old_value = current_dataframe.at[row_idx, params['target_col']]
                    new_value = merged_df.at[row_idx, new_value_col_name_in_merged_df]
                    changes.append({
                        'item': str(row_idx),
                        'column': params['target_col'],
                        'old': str(old_value),
                        'new': str(new_value)
                    })
                
                status_message = f"{len(changed_indices)}件のセルを参照置換しました"
                self.replace_from_file_completed.emit(changes, status_message)

        except Exception as e:
            status_message = f"ファイル参照置換中に予期せぬエラーが発生しました。\n{traceback.format_exc()}"
            self.replace_from_file_completed.emit([], status_message)


    def get_backend_instance(self):
        return self.backend_instance

    def shutdown(self):
        self.executor.shutdown(wait=True)
        if self.backend_instance and hasattr(self.backend_instance, 'close'):
            self.backend_instance.close()

#==============================================================================
# 2. その他の機能管理クラス
#==============================================================================
class UndoRedoManager:
    """操作履歴を管理し、アンドゥ/リドゥ機能を提供するクラス"""
    def __init__(self, app, max_history=50):
        self.app = app
        self.history = []
        self.current_index = -1
        self.max_history = max_history

    def add_action(self, action):
        if self.current_index < len(self.history) - 1:
            self.history = self.history[:self.current_index + 1]
        
        self.history.append(action)
        
        if len(self.history) > self.max_history:
            self.history.pop(0)
        
        self.current_index = len(self.history) - 1
        self.app.update_menu_states()

    def undo(self):
        if not self.can_undo(): return
        action = self.history[self.current_index]
        self.app.apply_action(action, is_undo=True)
        self.current_index -= 1
        self.app.update_menu_states()

    def redo(self):
        if not self.can_redo(): return
        self.current_index += 1
        action = self.history[self.current_index]
        self.app.apply_action(action, is_undo=False)
        self.app.update_menu_states()

    def can_undo(self):
        return self.current_index >= 0

    def can_redo(self):
        return self.current_index < len(self.history) - 1

    def clear(self):
        self.history.clear()
        self.current_index = -1
        if hasattr(self.app, 'update_menu_states'):
            self.app.update_menu_states()

class CSVFormatManager:
    """CSV形式の判定と管理を行うクラス (現在は主にプレースホルダー)"""
    def __init__(self, app):
        self.app = app

class ClipboardManager:
    """クリップボード操作を管理するクラス"""
    @staticmethod
    def copy_cells_to_clipboard(app, cells_data):
        pass

    @staticmethod
    def get_paste_data_from_clipboard(app, start_row_idx, start_col_idx):
        return []

class CellMergeManager:
    """セル連結機能を管理するクラス"""
    def __init__(self, app):
        self.app = app
    
    def concatenate_cells_right(self, target_cell):
        return False, "未実装"

    def concatenate_cells_left(self, target_cell):
        return False, "未実装"

class ColumnMergeManager:
    """列連結機能を管理するクラス"""
    def __init__(self, app):
        self.app = app

class ParentChildManager(QObject):
    """
    列の値に基づく親子関係を管理するクラス (PySide6版)
    """
    analysis_completed = Signal(str)
    analysis_error = Signal(str)

    def __init__(self):
        super().__init__()
        self.parent_child_data = {}
        self.current_group_column = None
        self.df = None
        self.db_backend = None

    def analyze_relationships(self, dataframe, column_name, mode='consecutive'):
        """親子関係分析のディスパッチャー（メモリ内）"""
        if mode == 'global':
            return self._analyze_global(dataframe, column_name)
        else:
            return self._analyze_consecutive(dataframe, column_name)

    def analyze_relationships_in_db(self, db_backend_instance, column_name, mode='consecutive', progress_callback=None):
        """親子関係分析のディスパッチャー（データベース）"""
        if mode == 'global':
            return self._analyze_global_in_db(db_backend_instance, column_name, progress_callback)
        else:
            return self._analyze_consecutive_in_db(db_backend_instance, column_name, progress_callback)

    def _analyze_consecutive(self, dataframe, column_name):
        """連続する同じ値をグループとみなして親子関係を分析"""
        if dataframe is None or dataframe.empty or column_name not in dataframe.columns:
            msg = "データがないか、列名が不正です。"
            self.analysis_error.emit(msg)
            return False, msg

        self.df = dataframe
        self.current_group_column = column_name
        self.parent_child_data.clear()

        is_new_group = self.df[column_name] != self.df[column_name].shift()
        group_ids = is_new_group.cumsum()
        group_row_numbers = self.df.groupby(group_ids).cumcount()

        for i in range(len(self.df)):
            row_idx = self.df.index[i]
            self.parent_child_data[row_idx] = {
                'group_id': group_ids.iloc[i],
                'is_parent': group_row_numbers.iloc[i] == 0,
                'group_value': str(self.df.at[row_idx, column_name]).strip(),
            }

        summary_msg = f"列「{column_name}」で{group_ids.max()}個の連続グループを識別しました"
        self.analysis_completed.emit(self.get_groups_summary())
        return True, summary_msg

    def _analyze_global(self, dataframe, column_name):
        """ファイル全体で同じ値を持つものを一つのグループとして親子関係を分析"""
        if dataframe is None or dataframe.empty or column_name not in dataframe.columns:
            msg = "データがないか、列名が不正です。"
            self.analysis_error.emit(msg)
            return False, msg

        self.df = dataframe
        self.current_group_column = column_name
        self.parent_child_data.clear()

        is_child_flags = dataframe[column_name].duplicated(keep='first')
        
        # グループ識別のためにユニークな値にIDを割り振る
        unique_values = dataframe[column_name].unique()
        value_to_group_id = {val: i+1 for i, val in enumerate(unique_values)}

        for i in range(len(dataframe)):
            row_idx = dataframe.index[i]
            value = str(dataframe.at[row_idx, column_name]).strip()
            self.parent_child_data[row_idx] = {
                'group_id': value_to_group_id.get(value),
                'is_parent': not is_child_flags.iloc[i],
                'group_value': value,
            }
        
        summary_msg = f"列「{column_name}」で{len(unique_values)}個のグローバルグループを識別しました"
        self.analysis_completed.emit(self.get_groups_summary())
        return True, summary_msg

    def _analyze_consecutive_in_db(self, db_backend_instance, column_name, progress_callback=None):
        """DB内で連続する同じ値をグループとして親子関係を分析"""
        if not db_backend_instance or not hasattr(db_backend_instance, 'conn'):
            # ...
            return False, "DBエラー", 0
        
        self.db_backend = db_backend_instance
        self.current_group_column = column_name
        self.parent_child_data.clear()

        try:
            query = f"""
            WITH RowMapping AS (
                SELECT
                    ROW_NUMBER() OVER (ORDER BY rowid) - 1 AS row_idx,
                    "{column_name}"
                FROM "{db_backend_instance.table_name}"
            ),
            GroupFlags AS (
                SELECT
                    row_idx,
                    "{column_name}",
                    CASE
                        WHEN "{column_name}" != LAG("{column_name}", 1, '') OVER (ORDER BY row_idx) THEN 1
                        ELSE 0
                    END AS is_new_group_flag
                FROM RowMapping
            ),
            GroupIDs AS (
                SELECT
                    row_idx,
                    "{column_name}",
                    SUM(is_new_group_flag) OVER (ORDER BY row_idx) AS group_id
                FROM
                    GroupFlags
            )
            SELECT
                row_idx,
                "{column_name}",
                group_id,
                ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY row_idx) AS group_row_number
            FROM
                GroupIDs
            ORDER BY row_idx;
            """
            cursor = self.db_backend.conn.cursor()
            cursor.execute(query)
            
            # ... (ループとプログレスバー処理) ...
            return True, "連続グループ分析完了", 0 # 仮
        except Exception as e:
            # ...
            return False, f"DBエラー: {e}", 0

    def _analyze_global_in_db(self, db_backend_instance, column_name, progress_callback=None):
        """DB内でファイル全体で同じ値を持つものを一つのグループとして親子関係を分析"""
        if not db_backend_instance or not hasattr(db_backend_instance, 'conn'):
            return False, "DBエラー", 0

        self.db_backend = db_backend_instance
        self.current_group_column = column_name
        self.parent_child_data.clear()
        
        try:
            # 各値が最初に出現するrowidを取得
            parent_query = f'SELECT "{column_name}", MIN(rowid) FROM "{db_backend_instance.table_name}" GROUP BY "{column_name}"'
            cursor = self.db_backend.conn.cursor()
            cursor.execute(parent_query)
            parent_lookup = {row[0]: row[1] for row in cursor.fetchall()}
            
            # 全データをスキャンして親子を判定
            query = f'SELECT ROW_NUMBER() OVER (ORDER BY rowid) - 1 AS row_idx, "{column_name}", rowid FROM "{db_backend_instance.table_name}"'
            cursor.execute(query)

            rows_chunk = cursor.fetchall()
            for row_data in rows_chunk:
                row_idx, value, current_rowid = row_data
                is_parent = (parent_lookup.get(value) == current_rowid)
                self.parent_child_data[row_idx] = {
                    'group_id': parent_lookup.get(value), # 親のrowidをグループIDとして使用
                    'is_parent': is_parent,
                    'group_value': str(value).strip(),
                }

            summary_msg = f"列「{column_name}」で{len(parent_lookup)}個のグローバルグループを識別しました"
            self.analysis_completed.emit(self.get_groups_summary())
            return True, summary_msg, len(self.parent_child_data)
        except Exception as e:
            # ...
            return False, f"DBエラー: {e}", 0

    def get_parent_rows_indices(self):
        """親行のインデックスリストを取得"""
        if not self.parent_child_data: return []
        return [idx for idx, data in self.parent_child_data.items() if data['is_parent']]
    
    def get_child_rows_indices(self):
        """子行のインデックスリストを取得"""
        if not self.parent_child_data: return []
        return [idx for idx, data in self.parent_child_data.items() if not data['is_parent']]
    
    def get_groups_summary(self):
        """グループの概要を取得"""
        if not self.parent_child_data:
            return "親子関係が分析されていません"
        
        group_counts = {}
        for data in self.parent_child_data.values():
            group_id = data['group_id']
            if group_id not in group_counts:
                group_counts[group_id] = {'value': data['group_value'], 'count': 0}
            group_counts[group_id]['count'] += 1
        
        summary = f"グループ分析結果（基準列：{self.current_group_column}）\n\n"
        # group_idが数値でない可能性もあるため、ソートキーを調整
        for group_id, info in sorted(group_counts.items(), key=lambda item: str(item[0])):
            child_count = info['count'] - 1
            summary += f"グループ{group_id}: 「{info['value']}」 (親1行, 子{child_count}行, 計{info['count']}行)\n"
        
        total_parents = len(self.get_parent_rows_indices())
        total_children = len(self.get_child_rows_indices())
        summary += f"\n---\n全体: 親 {total_parents}行, 子 {total_children}行"
        
        return summary