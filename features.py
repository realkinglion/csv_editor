# features.py

import csv
import pandas as pd
import os
import traceback
from PySide6.QtCore import QObject, Signal, QRunnable, Slot, QCoreApplication, QThread, QTimer
from PySide6.QtWidgets import QApplication
from concurrent.futures import ThreadPoolExecutor
import time
import re
import math
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP


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
        # self.signals = kwargs.get('signals') # signalsは使用されていないので削除可


    @Slot()
    def run(self):
        try:
            self.fn(*self.args, **self.kwargs)
        except Exception as e:
            error_info = traceback.format_exc()
            print(f"Worker thread error:\n{error_info}")
            # Workerクラス自体からエラーシグナルを発行することも可能だが、
            # AsyncDataManagerのエラーハンドリングに任せる
            # if self.signals and hasattr(self.signals, 'error_occurred'):
            #     self.signals.error_occurred.emit(f"バックグラウンド処理でエラーが発生しました:\n{e}")


class AsyncDataManager(QObject):
    """データ処理をバックグラウンドで実行し、UIの応答性を維持する"""
    data_ready = Signal(pd.DataFrame)
    task_progress = Signal(str, int, int) # main_qt._update_progress_dialogに接続
    search_results_ready = Signal(list)
    analysis_results_ready = Signal(str)
    replace_from_file_completed = Signal(list, str)
    product_discount_completed = Signal(list, str)
    bulk_extract_completed = Signal(object, str) 

    # UIへの安全な通知シグナル
    close_progress_requested = Signal()
    status_message_requested = Signal(str, int, bool)
    show_welcome_requested = Signal()
    cleanup_backend_requested = Signal() 

    # ファイル読み込み用の新しいプログレスシグナル
    # main_qtに直接接続する（AsyncDataManagerがemitし、main_qtがLoadingOverlayを制御）
    file_loading_started = Signal()
    file_loading_progress = Signal(str, int, int)
    file_loading_finished = Signal()
    
    def __init__(self, app_instance):
        super().__init__()
        self.app = app_instance
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.current_load_mode = 'normal'
        self.backend_instance = None
        self.is_cancelled = False
        self.current_task = None

        self.close_progress_requested.connect(self.app._close_progress_dialog)
        self.status_message_requested.connect(self.app.show_operation_status)
        self.show_welcome_requested.connect(self.app.view_controller.show_welcome_screen)
        self.cleanup_backend_requested.connect(self.app._cleanup_backend) 

        self.file_loading_started.connect(self.app.file_loading_started)
        self.file_loading_progress.connect(self.app.file_loading_progress)
        self.file_loading_finished.connect(self.app.file_loading_finished)
        
        # タイムアウト保護
        self.timeout_timer = QTimer()
        self.timeout_timer.setSingleShot(True)
        self.timeout_timer.timeout.connect(self._handle_timeout)
        
    def cancel_current_task(self):
        """現在の非同期タスクにキャンセルを要求する（スレッドセーフ版）"""
        self.is_cancelled = True
        if self.backend_instance:
            self.backend_instance.cancelled = True
        if self.current_task and isinstance(self.current_task, (QThread, ProductDiscountTask)):
            if hasattr(self.current_task, 'cancelled'):
                self.current_task.cancelled = True
        
        if self.timeout_timer.isActive():
            from PySide6.QtCore import QTimer
            QTimer.singleShot(0, self.timeout_timer.stop)

    def load_full_dataframe_async(self, filepath, encoding, load_mode):
        self.is_cancelled = False
        self.current_load_mode = load_mode 

        self.file_loading_started.emit()

        # タイムアウトタイマーを開始（30秒）
        self.timeout_timer.start(30000)
        
        self.current_filepath = filepath
        self.current_encoding = encoding

        worker = Worker(self._do_load_full_df, filepath, encoding, load_mode)
        self.executor.submit(worker.run)
    
    def _handle_timeout(self):
        """読み込みタイムアウト時の処理"""
        print("WARNING: ファイル読み込みがタイムアウトしました")
        self.cancel_current_task() 
        self.file_loading_finished.emit() 
        self.status_message_requested.emit(
            "ファイル読み込みがタイムアウトしました。より大きなファイルモードで再試行してください。",
            5000, True
        )
        self.cleanup_backend_requested.emit() 
        self.show_welcome_requested.emit()

    def _do_load_full_df(self, filepath, encoding, load_mode, **kwargs):
        from db_backend import SQLiteBackend
        from lazy_loader import LazyCSVLoader
        import config 

        df = None
        try:
            # タイムアウトタイマーを停止
            if self.timeout_timer.isActive():
                self.timeout_timer.stop()

            self.file_loading_progress.emit(
                "ファイルを読み込み中...", 0, 100
            )

            if load_mode == 'sqlite':
                self.backend_instance = SQLiteBackend(self.app)
                self.app.db_backend = self.backend_instance
                self.backend_instance.cancelled = self.is_cancelled

                def progress_callback(status, current, total):
                    if self.is_cancelled:
                        self.backend_instance.cancelled = True
                        return False 
                    self.file_loading_progress.emit(status, current, total)
                    return True 

                columns, total_rows = self.backend_instance.import_csv_with_progress(
                    filepath, encoding, progress_callback=progress_callback
                )

                self.file_loading_finished.emit()

                if self.is_cancelled or columns is None:
                    self.backend_instance.close()
                    self.backend_instance = None
                    self.status_message_requested.emit("読み込みをキャンセルしました。", 3000, False)
                    self.cleanup_backend_requested.emit() 
                    self.show_welcome_requested.emit()
                    return 

                if columns is not None:
                    self.backend_instance.header = columns
                    self.backend_instance.total_rows = total_rows
                    if hasattr(self.app, 'file_controller'): 
                        self.app.file_controller.file_loaded.emit(self.backend_instance, filepath, encoding)
                    else:
                        from PySide6.QtCore import QTimer
                        QTimer.singleShot(0, lambda: self.app._on_file_loaded(self.backend_instance, filepath, encoding))
                    return 

            elif load_mode == 'lazy':
                self.backend_instance = LazyCSVLoader(filepath, encoding)
                self.file_loading_finished.emit()
                
                if hasattr(self.app, 'file_controller'): 
                    self.app.file_controller.file_loaded.emit(self.backend_instance, filepath, encoding)
                else:
                    from PySide6.QtCore import QTimer
                    QTimer.singleShot(0, lambda: self.app._on_file_loaded(self.backend_instance, filepath, encoding))
                return 

            else: 
                self.file_loading_progress.emit("ファイルをメモリに読み込み中...", 0, 100)
                
                chunks = []
                chunk_size = 10000 
                
                try:
                    with open(filepath, 'r', encoding=encoding, errors='ignore') as f: 
                        total_lines = sum(1 for _ in f) 
                        if total_lines > 0: 
                            total_data_lines = total_lines - 1
                        else:
                            total_data_lines = 0

                    # 🔥 修正前（エラーが発生）
                    # read_options = self.app.file_controller.config.CSV_READ_OPTIONS.copy() 
                    
                    # 🔥 修正後（直接configモジュールを参照）
                    read_options = config.CSV_READ_OPTIONS.copy()
                    read_options['encoding'] = encoding

                    try:
                        with open(filepath, 'r', encoding=encoding) as f_peek:
                            first_line = f_peek.readline()
                            if first_line.count(',') > 100:
                                if read_options.get('engine') != 'python':
                                    read_options['low_memory'] = False
                    except Exception as e_peek:
                        print(f"WARNING: ファイルの先頭行読み込み中にエラー (AsyncDataManager): {e_peek}")
                        pass
                        
                    reader = pd.read_csv(filepath, encoding=encoding, dtype=str,
                                        chunksize=chunk_size, on_bad_lines='skip', **read_options) 
                    
                    rows_read = 0
                    for i, chunk in enumerate(reader):
                        if self.is_cancelled:
                            break
                            
                        chunks.append(chunk.fillna('')) 
                        rows_read += len(chunk)
                        
                        if total_data_lines > 0:
                            progress = min(int((rows_read / total_data_lines) * 100), 99) 
                        else:
                            progress = 100 
                        self.file_loading_progress.emit(
                            f"データをメモリに読み込み中... ({rows_read:,}/{total_data_lines:,}行)", 
                            progress, 100
                        )
                    
                    if not self.is_cancelled:
                        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=self.app.table_model._headers) 
                        self.file_loading_progress.emit("読み込み完了", 100, 100)
                    
                except Exception as e_chunk:
                    print(f"チャンク読み込みエラー、通常読み込みに切り替え (AsyncDataManager): {e_chunk}")
                    df = pd.read_csv(filepath, encoding=encoding, dtype=str, on_bad_lines='skip').fillna('') 
                    self.file_loading_progress.emit("読み込み完了", 100, 100)
                
                self.file_loading_finished.emit()

                if not self.is_cancelled:
                    self.data_ready.emit(df if df is not None else pd.DataFrame())
                else: 
                    self.status_message_requested.emit("読み込みをキャンセルしました。", 3000, False)
                    self.cleanup_backend_requested.emit() 
                    self.show_welcome_requested.emit()

        except Exception as e:
            error_message = f"ファイル読み込みエラー: {e}"
            print(f"ERROR in _do_load_full_df: {error_message}")
            traceback.print_exc()
            
            self.file_loading_finished.emit()
            
            self.task_progress.emit(f"エラー: {e}", 1, 1) 
            self.status_message_requested.emit(error_message, 5000, True)
            self.cleanup_backend_requested.emit() 
            self.show_welcome_requested.emit()
            self.data_ready.emit(pd.DataFrame()) 

    def search_data_async(self, settings: dict, current_load_mode: str, parent_child_data: dict, selected_rows: set):
        self.is_cancelled = False
        worker = Worker(self._do_search, settings, current_load_mode, parent_child_data, selected_rows)
        self.executor.submit(worker.run)

    def _do_search(self, settings: dict, current_load_mode: str, parent_child_data: dict, selected_rows: set, **kwargs):
        """ワーカースレッドで実行される検索処理。GUIアクセスは行わない。"""
        search_term = settings["search_term"]
        target_columns = settings["target_columns"]
        is_case_sensitive = settings["is_case_sensitive"]
        is_regex = settings["is_regex"]
        in_selection_only = settings["in_selection_only"]
        
        results = [] 
        
        try:
            self.task_progress.emit("検索中...", 0, 0)

            if current_load_mode == 'sqlite':
                db_backend = self.app.db_backend if hasattr(self.app, 'db_backend') and self.app.db_backend else self.backend_instance
                
                if db_backend and hasattr(db_backend, 'search'):
                    print(f"DEBUG: SQLite検索開始 - backend: {db_backend}")
                    
                    raw_results_from_db = db_backend.search( 
                        search_term, 
                        target_columns, 
                        is_case_sensitive, 
                        is_regex
                    )
                    print(f"DEBUG: SQLite検索結果: {len(raw_results_from_db)}件")
                    
                    results.extend(raw_results_from_db) 
                else:
                    print("ERROR: SQLiteバックエンドが見つかりません")
                    self.status_message_requested.emit("エラー: データベースが初期化されていません", 5000, True)
                    self.search_results_ready.emit([])
                    self.task_progress.emit("検索エラー", 1, 1)
                    return 

            elif current_load_mode == 'lazy':
                if self.backend_instance:
                    total_rows = self.backend_instance.get_total_rows()
                    def progress_callback(current):
                        if self.is_cancelled:
                            self.backend_instance.cancelled = True
                        self.task_progress.emit("ファイル内を検索中...", current, total_rows)
                    
                    lazy_results = self.backend_instance.search_in_file( 
                        search_term, target_columns, is_case_sensitive, is_regex,
                        progress_callback=progress_callback
                    )
                    results.extend(lazy_results) 
            
            else: 
                df = self.app.table_model._dataframe
                if df is None or df.empty:
                    self.search_results_ready.emit([])
                    self.task_progress.emit("検索完了", 1, 1)
                    return

                pattern = re.compile(
                    search_term if is_regex else re.escape(search_term),
                    0 if is_case_sensitive else re.IGNORECASE
                )
                
                target_rows = list(range(df.shape[0]))
                
                if in_selection_only:
                    selected_row_indices = {idx.row() for idx in self.app.table_view.selectionModel().selectedIndexes()}
                    target_rows = sorted(list(selected_row_indices.intersection(target_rows)))
                
                headers = self.app.table_model._headers
                target_col_indices = {headers.index(name) for name in target_columns if name in headers}
                
                total_search_cells = len(target_rows) * len(target_col_indices)
                processed_cells = 0
                
                for row_idx in target_rows:
                    if self.is_cancelled:
                        self.task_progress.emit("検索がキャンセルされました", 1, 1)
                        self.search_results_ready.emit([])
                        return
                    
                    for col_idx in target_col_indices:
                        if col_idx < len(df.columns):
                            cell_value = df.iat[row_idx, col_idx]
                            if cell_value is not None and pattern.search(str(cell_value)):
                                results.append((row_idx, col_idx)) 
                        
                        processed_cells += 1
                        if processed_cells % 1000 == 0:
                            self.task_progress.emit(
                                "データ内を検索中...", 
                                processed_cells, 
                                total_search_cells
                            )
            
            self.task_progress.emit("検索完了", 1, 1)
            
        except re.error as e:
            if QApplication.instance():
                self.status_message_requested.emit(f"正規表現エラー: {e}", 5000, True)
            self.search_results_ready.emit([])
            return
        except Exception as e:
            print(f"Error during search: {traceback.format_exc()}")
            if QApplication.instance():
                self.status_message_requested.emit(f"検索中にエラーが発生しました: {e}", 5000, True)
            self.search_results_ready.emit([])
            return
        
        self.search_results_ready.emit(results) 

    def analyze_parent_child_async(self, db_backend_instance, column_name, mode):
        self.is_cancelled = False
        worker = Worker(self._do_analyze_parent_child_in_db, db_backend_instance, column_name, mode)
        self.executor.submit(worker.run)

    def _do_analyze_parent_child_in_db(self, db_backend_instance, column_name, mode, **kwargs):
        def progress_callback(status, current, total):
            if self.is_cancelled:
                db_backend_instance.cancelled = True
            self.task_progress.emit(status, current, total)
            
        success, message, total_rows = self.app.parent_child_manager.analyze_relationships_in_db(
            db_backend_instance, column_name, mode,
            progress_callback=progress_callback
        )
        if success:
            self.analysis_results_ready.emit(self.app.parent_child_manager.get_groups_summary())
        else:
            self.analysis_results_ready.emit(f"分析エラー: {message}")
    
    def replace_from_file_async(self, db_backend_instance, current_dataframe, params):
        self.is_cancelled = False
        worker = Worker(self._do_replace_from_file, db_backend_instance, current_dataframe, params)
        self.executor.submit(worker.run)

    def _do_replace_from_file(self, db_backend_instance, current_dataframe, params, **kwargs):
        changes = []
        status_message = ""
        
        try:
            required_params = ['lookup_filepath', 'lookup_file_encoding', 
                               'target_col', 'lookup_key_col', 'replace_val_col']
            missing_params = [p for p in required_params if p not in params]
            if missing_params:
                raise KeyError(f"必須パラメータが不足: {missing_params}")

            self.task_progress.emit("参照ファイルを読み込み中...", 0, 1)
            lookup_df = pd.read_csv(params['lookup_filepath'], encoding=params['lookup_file_encoding'], dtype=str, on_bad_lines='warn').fillna('')
            self.task_progress.emit("参照ファイルを読み込み完了", 1, 1)
            
            if db_backend_instance:
                def progress_callback(status, current, total):
                    self.task_progress.emit(status, current, total)

                success, temp_changes, updated_count = db_backend_instance.execute_replace_from_file_in_db(
                    params, 
                    progress_callback=progress_callback
                )
                if success:
                    status_message = f"ファイル参照置換完了: {updated_count}件のセルを置換しました。"
                    self.replace_from_file_completed.emit([], status_message)
                else:
                    status_message = "ファイル参照置換に失敗しました (データベースエラー)。"
                    self.replace_from_file_completed.emit([], status_message)
                return

            else:
                self.task_progress.emit("データをマージ中...", 0, 1)
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
                self.task_progress.emit("データをマージ完了", 1, 1)
                
                current_target_values = current_dataframe[params['target_col']].astype(str).fillna('')
                new_lookup_values = merged_df[new_value_col_name_in_merged_df].astype(str).fillna('')
                
                changed_mask = merged_df[new_value_col_name_in_merged_df].notna() & \
                               (current_target_values != new_lookup_values)
                
                changed_indices = current_dataframe.index[changed_mask]
                
                if changed_indices.empty:
                    status_message = "置換対象となるデータが見つかりませんでした。"
                    self.replace_from_file_completed.emit([], status_message)
                    return
                
                total_changes = len(changed_indices)
                self.task_progress.emit("変更リストを作成中...", 0, total_changes)
                for i, row_idx in enumerate(changed_indices):
                    old_value = current_dataframe.at[row_idx, params['target_col']]
                    new_value = merged_df.at[row_idx, new_value_col_name_in_merged_df]
                    changes.append({
                        'item': str(row_idx),
                        'column': params['target_col'],
                        'old': str(old_value),
                        'new': str(new_value)
                    })
                    if i % 1000 == 0:
                        self.task_progress.emit("変更リストを作成中...", i, total_changes)
                
                status_message = f"{len(changed_indices)}件のセルを参照置換しました"
                self.replace_from_file_completed.emit(changes, status_message)

        except Exception as e:
            error_info = traceback.format_exc()
            status_message = f"ファイル参照置換中に予期せぬエラーが発生しました。\n{error_info}"
            self.replace_from_file_completed.emit([], status_message)

    def product_discount_async(self, db_backend, table_model, params):
        """商品別割引適用の非同期処理を開始する"""
        if self.current_task and self.current_task.isRunning():
            self.cancel_current_task()
            time.sleep(0.1)
        
        self.is_cancelled = False

        self.current_task = ProductDiscountTask(db_backend, table_model, params)
        self.current_task.discount_completed.connect(self.product_discount_completed.emit)
        self.current_task.task_progress.connect(self.task_progress.emit)
        self.current_task.start()

    def bulk_extract_async(self, data_source, settings, load_mode): 
        """商品リスト一括抽出の非同期処理""" 
        self.is_cancelled = False 
        worker = Worker(self._do_bulk_extract, data_source, settings, load_mode) 
        self.executor.submit(worker.run) 

    def _do_bulk_extract(self, data_source, settings, load_mode, **kwargs): 
        """商品リスト一括抽出/除外の実際の処理"""
        try: 
            target_column = settings['bulk_extract_column'] 
            product_list = settings['product_list'] 
            case_sensitive = settings['case_sensitive'] 
            exact_match = settings['exact_match'] 
            trim_whitespace = settings['trim_whitespace'] 
            
            # 🔥 新規追加：モード取得
            bulk_mode = settings.get('bulk_mode', 'extract')  # デフォルトは抽出モード
            
            if trim_whitespace: 
                product_list = [item.strip() for item in product_list] 
            
            unique_products = list(set(product_list)) 
            
            if not case_sensitive: 
                search_dict = {item.lower(): item for item in unique_products} 
                search_keys = set(search_dict.keys()) 
            else: 
                search_keys = set(unique_products) 
            
            self.task_progress.emit("商品リストを解析中...", 10, 100) 
            
            matched_rows_indices = [] 
            
            # 各モードでマッチング処理を実行
            if load_mode == 'sqlite' and hasattr(data_source, 'conn'): 
                matched_rows_indices = self._bulk_extract_from_sqlite( 
                    data_source, target_column, search_keys, case_sensitive, exact_match 
                ) 
            elif load_mode == 'lazy' and hasattr(data_source, 'filepath'): 
                matched_rows_indices = self._bulk_extract_from_lazy_loader( 
                    data_source, target_column, search_keys, case_sensitive, exact_match 
                ) 
            else: 
                if hasattr(data_source, 'get_dataframe'): 
                    df = data_source.get_dataframe() 
                else: 
                    df = data_source 
                
                matched_rows_indices = self._bulk_extract_from_dataframe( 
                    df, target_column, search_keys, case_sensitive, exact_match 
                ) 
            
            # 🔥 重要：除外モードの場合、マッチしなかった行を取得
            if bulk_mode == 'exclude':
                # 全行のインデックスを取得
                if load_mode == 'sqlite' or load_mode == 'lazy':
                    total_rows = data_source.get_total_rows()
                    all_indices = list(range(total_rows))
                else:
                    all_indices = list(range(len(df)))
                
                # マッチした行を除外
                matched_set = set(matched_rows_indices)
                excluded_rows_indices = [idx for idx in all_indices if idx not in matched_set]
                
                # 結果を入れ替え
                matched_rows_indices = excluded_rows_indices
            
            # 結果の処理
            if matched_rows_indices:
                if load_mode == 'sqlite' or load_mode == 'lazy':
                    result_df = data_source.get_rows_by_ids(matched_rows_indices)
                    if hasattr(data_source, 'header') and not result_df.empty:
                        result_df = result_df[data_source.header]
                else:
                    result_df = df.iloc[matched_rows_indices].copy().reset_index(drop=True)
                
                if bulk_mode == 'extract':
                    status_message = f"商品リスト抽出完了: {len(matched_rows_indices)}件の商品が見つかりました（検索対象: {len(unique_products)}件）"
                else:
                    status_message = f"商品リスト除外完了: {len(matched_rows_indices)}件の商品が残りました（除外対象: {len(unique_products)}件）"
            else:
                result_df = pd.DataFrame(columns=self.app.table_model._headers)
                if bulk_mode == 'extract':
                    status_message = f"該当する商品が見つかりませんでした（検索対象: {len(unique_products)}件）"
                else:
                    status_message = f"すべての商品が除外されました（除外対象: {len(unique_products)}件）"
            
            self.task_progress.emit("処理完了", 100, 100)
            self.bulk_extract_completed.emit(result_df, status_message)
            
        except Exception as e:
            error_message = f"商品リスト処理中にエラーが発生しました: {str(e)}"
            print(f"ERROR in _do_bulk_extract: {error_message}")
            traceback.print_exc()
            self.bulk_extract_completed.emit(pd.DataFrame(), error_message)

    def _bulk_extract_from_sqlite(self, db_backend, target_column, search_keys, case_sensitive, exact_match): 
        """SQLiteバックエンドからの商品リスト抽出（一時テーブル+JOIN最適化）""" 
        matched_rows_indices = [] 
        
        try: 
            escaped_col = target_column.replace('"', '""') 
            cursor = db_backend.conn.cursor() 
            
            cursor.execute("CREATE TEMPORARY TABLE temp_lookup (value TEXT PRIMARY KEY)") 
            
            search_list = list(search_keys) 
            if len(search_list) > 10000: 
                for i in range(0, len(search_list), 10000): 
                    if self.is_cancelled: return [] 
                    chunk = search_list[i:i+10000] 
                    cursor.executemany("INSERT OR IGNORE INTO temp_lookup (value) VALUES (?)", 
                                      [(item,) for item in chunk]) 
                    self.task_progress.emit(f"検索リストをDBにロード中... ({i + len(chunk)}/{len(search_list)})", 20 + int((i + len(chunk)) / len(search_list) * 20), 100) 
            else: 
                cursor.executemany("INSERT INTO temp_lookup (value) VALUES (?)", 
                                  [(item,) for item in search_list]) 
            db_backend.conn.commit() 
            
            if exact_match: 
                if case_sensitive: 
                    query = f'''
                    SELECT T1.rowid - 1 FROM "{db_backend.table_name}" AS T1
                    JOIN temp_lookup AS T2 ON T1."{escaped_col}" = T2.value
                    ''' 
                else: 
                    query = f'''
                    SELECT T1.rowid - 1 FROM "{db_backend.table_name}" AS T1
                    JOIN temp_lookup AS T2 ON LOWER(T1."{escaped_col}") = LOWER(T2.value)
                    ''' 
            else: 
                if case_sensitive: 
                    query = f'''
                    SELECT T1.rowid - 1 FROM "{db_backend.table_name}" AS T1
                    JOIN temp_lookup AS T2 ON T1."{escaped_col}" LIKE '%' || T2.value || '%'
                    ''' 
                else: 
                    query = f'''
                    SELECT T1.rowid - 1 FROM "{db_backend.table_name}" AS T1
                    JOIN temp_lookup AS T2 ON LOWER(T1."{escaped_col}") LIKE '%' || LOWER(T2.value) || '%'
                    ''' 
            
            cursor.execute(query) 
            
            chunk_size = 50000 
            total_processed_rows = 0 
            
            while True: 
                if self.is_cancelled: 
                    matched_rows_indices = [] 
                    break 
                
                rows_chunk = cursor.fetchmany(chunk_size) 
                if not rows_chunk: 
                    break 
                
                matched_rows_indices.extend([row[0] for row in rows_chunk]) 
                total_processed_rows += len(rows_chunk) 
                self.task_progress.emit(f"商品を検索中... {total_processed_rows}件発見", 40 + int(total_processed_rows / db_backend.get_total_rows() * 40), 100) 
                
            cursor.execute("DROP TABLE IF EXISTS temp_lookup") 
            
            self.task_progress.emit(f"商品を検索中... {len(matched_rows_indices)}件発見", 90, 100) 
            
        except Exception as e: 
            print(f"ERROR in _bulk_extract_from_sqlite: {e}") 
            try: 
                cursor.execute("DROP TABLE IF EXISTS temp_lookup") 
            except: 
                pass 
            raise 
        
        return matched_rows_indices 

    def _bulk_extract_from_dataframe(self, df, target_column, search_keys, case_sensitive, exact_match): 
        """DataFrameからの商品リスト抽出""" 
        matched_rows_indices = [] 
        
        try: 
            if target_column not in df.columns: 
                return matched_rows_indices 
            
            target_series = df[target_column].astype(str).fillna('') 
            
            total_rows = len(df) 
            processed_rows = 0 
            
            if exact_match: 
                if case_sensitive: 
                    mask = target_series.isin(search_keys) 
                else: 
                    mask = target_series.str.lower().isin(search_keys) 
            else: 
                if case_sensitive: 
                    pattern_str = '|'.join(re.escape(item) for item in search_keys) 
                    if len(pattern_str) > 10000: 
                        chunk_size = 500 
                        masks = [] 
                        for i in range(0, len(search_keys), chunk_size): 
                            if self.is_cancelled: return [] 
                            sub_pattern_str = '|'.join(re.escape(item) for item in list(search_keys)[i:i+chunk_size]) 
                            masks.append(target_series.str.contains(sub_pattern_str, regex=True, na=False)) 
                            self.task_progress.emit(f"部分一致検索中... ({i + chunk_size}/{len(search_keys)}キー)", 40 + int((i + chunk_size) / len(search_keys) * 10), 100) 
                        mask = masks[0] 
                        for m in masks[1:]: 
                            mask |= m 
                    else: 
                        mask = target_series.str.contains(pattern_str, regex=True, na=False) 
                else: 
                    pattern_str = '|'.join(re.escape(item) for item in search_keys) 
                    if len(pattern_str) > 10000: 
                        chunk_size = 500 
                        masks = [] 
                        for i in range(0, len(search_keys), chunk_size): 
                            if self.is_cancelled: return [] 
                            sub_pattern_str = '|'.join(re.escape(item) for item in list(search_keys)[i:i+chunk_size]) 
                            masks.append(target_series.str.contains(sub_pattern_str, case=False, regex=True, na=False)) 
                            self.task_progress.emit(f"部分一致検索中... ({i + chunk_size}/{len(search_keys)}キー)", 40 + int((i + chunk_size) / len(search_keys) * 10), 100) 
                        mask = masks[0] 
                        for m in masks[1:]: 
                            mask |= m 
                    else: 
                        mask = target_series.str.contains(pattern_str, case=False, regex=True, na=False) 
            
            matched_rows_indices = df[mask].index.tolist() 
            
            self.task_progress.emit(f"商品を検索中... {len(matched_rows_indices)}件発見", 90, 100) 
            
        except Exception as e: 
            print(f"ERROR in _bulk_extract_from_dataframe: {e}") 
            raise 
        
        return matched_rows_indices 

    def _bulk_extract_from_lazy_loader(self, lazy_loader, target_column, search_keys, case_sensitive, exact_match): 
        """LazyCSVLoaderからの商品リスト抽出""" 
        matched_rows_indices = [] 
        col_idx = lazy_loader.header.index(target_column) 
        
        if exact_match: 
            if case_sensitive: 
                match_func = lambda cell_val: cell_val in search_keys 
            else: 
                search_keys_lower = {k.lower() for k in search_keys} 
                match_func = lambda cell_val: cell_val.lower() in search_keys_lower 
        else: 
            if case_sensitive: 
                patterns = [re.compile(re.escape(key)) for key in search_keys] 
                match_func = lambda cell_val: any(p.search(cell_val) for p in patterns) 
            else: 
                patterns = [re.compile(re.escape(key), re.IGNORECASE) for key in search_keys] 
                match_func = lambda cell_val: any(p.search(cell_val) for p in patterns) 

        total_rows = lazy_loader.get_total_rows() 
        
        try: 
            with lazy_loader._file_lock: 
                if lazy_loader._file_handle is None: 
                    lazy_loader._file_handle = open(lazy_loader.filepath, 'r', 
                                                    encoding=lazy_loader.encoding, 
                                                    errors='ignore', newline='') 
                    lazy_loader._file_handle.readline() 
                
                lazy_loader._file_handle.seek(lazy_loader._row_index[0] if lazy_loader._row_index else 0) 
                
                for row_idx in range(total_rows): 
                    if self.is_cancelled: return [] 
                    
                    line = lazy_loader._file_handle.readline() 
                    if not line: break 
                    
                    parsed_row = lazy_loader._parse_csv_line(line) 
                    
                    if col_idx < len(parsed_row): 
                        cell_value = parsed_row[col_idx] 
                        if match_func(cell_value): 
                            matched_rows_indices.append(row_idx) 
                    
                    if row_idx % 1000 == 0: 
                        self.task_progress.emit(f"Lazyロードで検索中... ({row_idx}/{total_rows})", 40 + int(row_idx / total_rows * 40), 100) 
            
        except Exception as e: 
            print(f"ERROR in _bulk_extract_from_lazy_loader: {e}") 
            raise 
        
        return matched_rows_indices 

class ProductDiscountTask(QThread):
    """商品別割引適用をバックグラウンドで実行するQThreadベースのタスク"""
    discount_completed = Signal(list, str)
    task_progress = Signal(str, int, int)
    
    def __init__(self, backend, table_model, params):
        super().__init__()
        self.backend = backend
        self.table_model = table_model
        self.params = params
        self.cancelled = False
        
    def run(self):
        try:
            changes, message = self._execute_discount_calculation()
            if self.cancelled:
                self.discount_completed.emit([], "商品別割引適用がキャンセルされました。")
            else:
                self.discount_completed.emit(changes, message)
        except Exception as e:
            error_info = traceback.format_exc()
            error_msg = f"商品別割引適用中にエラーが発生しました。\n{str(e)}\n{error_info}"
            print(f"ProductDiscountTask error:\n{error_msg}")
            self.discount_completed.emit([], error_msg)
            
    def _execute_discount_calculation(self):
        changes = []
        status_message = ""
        
        try:
            self.task_progress.emit("参照ファイルを読み込み中...", 0, 100)
            
            discount_file_encoding = self.params.get('discount_file_encoding', 'utf-8') 
            
            discount_df = pd.read_csv(
                self.params['discount_filepath'],
                encoding=discount_file_encoding,
                dtype=str,
                na_filter=False,
                keep_default_na=False
            )
            self.task_progress.emit("参照ファイルを読み込み完了", 10, 100)

            if self.cancelled: return [], "キャンセル"
            
            if self.params['ref_product_col'] not in discount_df.columns:
                return [], f"エラー: 参照ファイルに商品番号列'{self.params['ref_product_col']}'が見つかりません。"
            
            if self.params['ref_discount_col'] not in discount_df.columns:
                return [], f"エラー: 参照ファイルに割引率列'{self.params['ref_discount_col']}'が見つかりません。"
            
            self.task_progress.emit("割引率を解析中...", 20, 100)
            
            discount_lookup = {}
            total_discount_rows = len(discount_df)
            for i, row in discount_df.iterrows():
                if self.cancelled: return [], "キャンセル"
                
                product_id = str(row[self.params['ref_product_col']]).strip()
                discount_str = str(row[self.params['ref_discount_col']]).strip()
                
                discount_rate = self._parse_discount_rate(discount_str)
                if discount_rate is not None:
                    discount_lookup[product_id] = discount_rate
                
                if i % 1000 == 0:
                    self.task_progress.emit(f"割引率を解析中... ({i}/{total_discount_rows})", 20 + int(i/total_discount_rows * 20), 100)
            
            if not discount_lookup:
                return [], "エラー: 有効な割引率データが見つかりませんでした。"
            self.task_progress.emit("割引率解析完了", 40, 100)
            
            self.task_progress.emit("金額を計算中...", 50, 100)
            
            if self.backend:
                changes = self._process_with_backend(discount_lookup)
            else:
                changes = self._process_with_dataframe(discount_lookup)
            
            status_message = f"商品別割引適用完了: {len(changes)}件のセルを更新しました。"
            self.task_progress.emit("完了", 100, 100)
            
            return changes, status_message
            
        except Exception as e:
            error_info = traceback.format_exc()
            error_msg = f"計算処理中にエラーが発生しました。\n{str(e)}\n{error_info}"
            return [], error_msg
            
    def _parse_discount_rate(self, discount_str):
        try:
            cleaned = discount_str.replace('%', '').replace('％', '').strip()
            
            if not cleaned:
                return None
            
            rate = Decimal(cleaned)
            
            if rate > 1:
                rate = rate / Decimal('100')
            
            if Decimal('0') <= rate <= Decimal('1'):
                return float(rate)
            else:
                print(f"WARNING: 割引率が範囲外です: '{discount_str}' -> {rate}")
                return None
                
        except Exception:
            print(f"WARNING: 割引率の解析に失敗: '{discount_str}'")
            return None
            
    def _process_with_dataframe(self, discount_lookup):
        changes = []
        df = self.table_model._dataframe
        
        if df is None or df.empty:
            return []
            
        product_col = self.params['current_product_col']
        price_col = self.params['current_price_col']
        
        if product_col not in df.columns or price_col not in df.columns:
            return []
            
        total_rows = len(df)
        for idx, row_series in df.iterrows():
            if self.cancelled: return []
            
            product_id = str(row_series.get(product_col, '')).strip()
            
            if product_id in discount_lookup:
                try:
                    current_price_str = str(row_series.get(price_col, '')).strip()
                    current_price = self._parse_price(current_price_str)
                    
                    if current_price is None:
                        continue
                        
                    discount_rate = Decimal(str(discount_lookup[product_id]))
                    discounted_price_decimal = Decimal('1.0') - discount_rate 
                    final_price_decimal = Decimal(str(current_price)) * discounted_price_decimal
                    
                    final_price = self._apply_rounding(float(final_price_decimal), self.params['round_mode'])
                    final_price_str = str(int(final_price))
                    
                    if current_price_str != final_price_str:
                        changes.append({
                            'item': str(idx),
                            'column': price_col,
                            'old': current_price_str,
                            'new': final_price_str
                        })
                        
                except Exception as e:
                    print(f"WARNING: 行{idx}の処理中にエラー: {e}")
                    continue
            
            if idx % 1000 == 0:
                self.task_progress.emit(f"金額を計算中... ({idx}/{total_rows})", 50 + int(idx/total_rows * 40), 100)

        return changes
        
    def _process_with_backend(self, discount_lookup):
        changes = []
        if not self.backend:
            return []

        total_rows = self.backend.get_total_rows()
        self.task_progress.emit("DBデータを処理中...", 50, 100)
        
        try:
            df_from_backend = self.backend.get_all_data()
            
            product_col = self.params['current_product_col']
            price_col = self.params['current_price_col']

            if product_col not in df_from_backend.columns or price_col not in df_from_backend.columns:
                print("WARNING: DBバックエンド処理で列が見つかりません。")
                return []
            
            for idx, row_series in df_from_backend.iterrows():
                if self.cancelled: return []
                
                product_id = str(row_series.get(product_col, '')).strip()
                
                if product_id in discount_lookup:
                    try:
                        current_price_str = str(row_series.get(price_col, '')).strip()
                        current_price = self._parse_price(current_price_str)
                        
                        if current_price is None:
                            continue
                            
                        discount_rate = Decimal(str(discount_lookup[product_id]))
                        discounted_price_decimal = Decimal('1.0') - discount_rate 
                        final_price_decimal = Decimal(str(current_price)) * discounted_price_decimal
                        
                        final_price = self._apply_rounding(float(final_price_decimal), self.params['round_mode'])
                        final_price_str = str(int(final_price))
                        
                        if current_price_str != final_price_str:
                            changes.append({
                                'row_idx': idx,
                                'col_name': price_col,
                                'new_value': final_price_str,
                                'old_value': current_price_str 
                            })
                            
                    except Exception as e:
                        print(f"WARNING: DB処理中の行{idx}でエラー: {e}")
                        continue
                
                if idx % 1000 == 0:
                    self.task_progress.emit(f"DBデータを処理中... ({idx}/{total_rows})", 50 + int(idx/total_rows * 40), 100)

            if changes:
                self.backend.update_cells(changes)
                
        except Exception as e:
            print(f"ERROR: _process_with_backend failed: {e}")
            traceback.print_exc()
            return []

        return changes

    def _parse_price(self, price_str):
        try:
            cleaned = re.sub(r'[^\d.]', '', price_str)
            if not cleaned:
                return None
            return float(cleaned)
        except (ValueError, TypeError):
            return None
            
    def _apply_rounding(self, price, round_mode):
        decimal_price = Decimal(str(price))
        
        if round_mode == 'truncate':
            return float(decimal_price.quantize(Decimal('1'), rounding=ROUND_DOWN))
        elif round_mode == 'round':
            return float(decimal_price.quantize(Decimal('1'), rounding=ROUND_HALF_UP))
        elif round_mode == 'ceil':
            return float(decimal_price.quantize(Decimal('1'), rounding=ROUND_UP))
        else:
            return float(decimal_price.quantize(Decimal('1'), rounding=ROUND_DOWN))


    def get_backend_instance(self):
        return self.backend_instance

    def shutdown(self):
        self.executor.shutdown(wait=True)
        if self.backend_instance and hasattr(self.backend_instance, 'close'):
            self.backend_instance.close()

class UndoRedoManager(QObject):
    """操作履歴を管理し、アンドゥ/リドゥ機能を提供するクラス"""
    def __init__(self, app, max_history=50):
        super().__init__()
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
        """やり直し可能かどうかを判定"""
        return self.current_index < len(self.history) - 1
    
    def clear(self):
        """履歴をクリア"""
        self.history = []
        self.current_index = -1


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

    def __init__(self, ):
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
            return False, msg, 0
        
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
        return True, summary_msg, len(dataframe)

    def _analyze_global(self, dataframe, column_name):
        """ファイル全体で同じ値を持つものを一つのグループとして親子関係を分析"""
        if dataframe is None or dataframe.empty or column_name not in dataframe.columns:
            msg = "データがないか、列名が不正です。"
            self.analysis_error.emit(msg)
            return False, msg, 0

        self.df = dataframe
        self.current_group_column = column_name
        self.parent_child_data.clear()

        is_child_flags = dataframe[column_name].duplicated(keep='first')
        
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
        return True, summary_msg, len(dataframe)

    def _analyze_consecutive_in_db(self, db_backend_instance, column_name, progress_callback=None):
        """DB内で連続する同じ値をグループとして親子関係を分析"""
        if not db_backend_instance or not hasattr(db_backend_instance, 'conn'):
            return False, "DBエラー", 0
        
        self.db_backend = db_backend_instance
        self.current_group_column = column_name
        self.parent_child_data.clear()

        try:
            if progress_callback:
                progress_callback("連続グループを分析中...", 0, 1)

            query = f'SELECT ROW_NUMBER() OVER (ORDER BY rowid) - 1 AS row_idx, "{column_name}" FROM "{db_backend_instance.table_name}"'
            df_from_db = pd.read_sql_query(query, db_backend_instance.conn)
            
            self._analyze_consecutive(df_from_db, column_name)
            
            if progress_callback:
                progress_callback("分析完了", 1, 1)

            return True, "連続グループ分析完了", len(df_from_db)
        except Exception as e:
            return False, f"DBエラー: {e}", 0

    def _analyze_global_in_db(self, db_backend_instance, column_name, progress_callback=None):
        """DB内でファイル全体で同じ値を持つものを一つのグループとして親子関係を分析"""
        if not db_backend_instance or not hasattr(db_backend_instance, 'conn'):
            return False, "DBエラー", 0

        self.db_backend = db_backend_instance
        self.current_group_column = column_name
        self.parent_child_data.clear()
        
        try:
            if progress_callback: progress_callback("親レコードを特定中...", 0, 1)
            parent_query = f'SELECT "{column_name}", MIN(rowid) FROM "{db_backend_instance.table_name}" GROUP BY "{column_name}"'
            cursor = self.db_backend.conn.cursor()
            cursor.execute(parent_query)
            parent_lookup = {row[0]: row[1] for row in cursor.fetchall()}
            if progress_callback: progress_callback("親レコードを特定完了", 1, 1)

            total_rows = db_backend_instance.get_total_rows()
            if progress_callback: progress_callback("全レコードを分類中...", 0, total_rows)
            query = f'SELECT ROW_NUMBER() OVER (ORDER BY rowid) - 1 AS row_idx, "{column_name}", rowid FROM "{db_backend_instance.table_name}"'
            cursor.execute(query)

            processed_rows = 0
            while True:
                rows_chunk = cursor.fetchmany(10000)
                if not rows_chunk:
                    break
                
                for row_data in rows_chunk:
                    row_idx, value, current_rowid = row_data
                    is_parent = (parent_lookup.get(value) == current_rowid)
                    self.parent_child_data[row_idx] = {
                        'group_id': parent_lookup.get(value),
                        'is_parent': is_parent,
                        'group_value': str(value).strip() if value is not None else '',
                    }
                
                processed_rows += len(rows_chunk)
                if progress_callback:
                    progress_callback("全レコードを分類中...", processed_rows, total_rows)

            summary_msg = f"列「{column_name}」で{len(parent_lookup)}個のグローバルグループを識別しました"
            self.analysis_completed.emit(self.get_groups_summary())
            return True, summary_msg, len(self.parent_child_data)
        except Exception as e:
            return False, f"DBエラー: {e}", 0

    def get_parent_rows_indices(self):
        if not self.parent_child_data: return []
        return [idx for idx, data in self.parent_child_data.items() if data['is_parent']]
    
    def get_child_rows_indices(self):
        if not self.parent_child_data: return []
        return [idx for idx, data in self.parent_child_data.items() if not data['is_parent']]
    
    def get_groups_summary(self):
        if not self.parent_child_data:
            return "親子関係が分析されていません"
        
        group_counts = {}
        for data in self.parent_child_data.values():
            group_id = data['group_id']
            if group_id not in group_counts:
                group_counts[group_id] = {'value': data['group_value'], 'count': 0}
            group_counts[group_id]['count'] += 1
        
        summary = f"グループ分析結果（基準列：{self.current_group_column}）\n\n"
        for group_id, info in sorted(group_counts.items(), key=lambda item: str(item[0])):
            child_count = info['count'] - 1
            summary += f"グループ{group_id}: 「{info['value']}」 (親1行, 子{child_count}行, 計{info['count']}行)\n"
        
        total_parents = len(self.get_parent_rows_indices())
        total_children = len(self.get_child_rows_indices())
        summary += f"\n---\n全体: 親 {total_parents}行, 子 {total_children}行"
        
        return summary