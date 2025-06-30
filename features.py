# features.py

import csv
import pandas as pd
from PySide6.QtCore import QObject, Signal, QRunnable, Slot, QCoreApplication, QThread
from PySide6.QtWidgets import QApplication
from concurrent.futures import ThreadPoolExecutor
import time
import re
import traceback
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
        self.signals = kwargs.get('signals')

    @Slot()
    def run(self):
        try:
            self.fn(*self.args, **self.kwargs)
        except Exception as e:
            error_info = traceback.format_exc()
            print(f"Worker thread error:\n{error_info}")
            if self.signals and hasattr(self.signals, 'error_occurred'):
                self.signals.error_occurred.emit(f"バックグラウンド処理でエラーが発生しました:\n{e}")


class AsyncDataManager(QObject):
    """データ処理をバックグラウンドで実行し、UIの応答性を維持する"""
    data_ready = Signal(pd.DataFrame)
    task_progress = Signal(str, int, int)
    search_results_ready = Signal(list)
    analysis_results_ready = Signal(str)
    replace_from_file_completed = Signal(list, str)
    product_discount_completed = Signal(list, str)

    # 新しいシグナルを追加
    close_progress_requested = Signal()
    status_message_requested = Signal(str, int, bool)
    show_welcome_requested = Signal()
    
    def __init__(self, app_instance):
        super().__init__()
        self.app = app_instance
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.current_load_mode = 'normal'
        self.backend_instance = None
        self.is_cancelled = False
        self.current_task = None

        # シグナルを接続
        self.close_progress_requested.connect(app_instance._close_progress_dialog)
        self.status_message_requested.connect(app_instance.show_operation_status)
        self.show_welcome_requested.connect(app_instance.view_controller.show_welcome_screen)

    def cancel_current_task(self):
        """現在の非同期タスクにキャンセルを要求する"""
        self.is_cancelled = True
        if self.backend_instance:
            self.backend_instance.cancelled = True
        if self.current_task and isinstance(self.current_task, (QThread, ProductDiscountTask)):
            if hasattr(self.current_task, 'cancelled'):
                self.current_task.cancelled = True

    def load_full_dataframe_async(self, filepath, encoding, load_mode):
        self.is_cancelled = False
        # プログレスダイアログを表示
        self.app._show_progress_dialog(
            f"「{os.path.basename(filepath)}」を読み込み中...",
            self.cancel_current_task # キャンセルコールバックとして自身のメソッドを渡す
        )
        worker = Worker(self._do_load_full_df, filepath, encoding, load_mode)
        self.executor.submit(worker.run)

    def _do_load_full_df(self, filepath, encoding, load_mode, **kwargs):
        from db_backend import SQLiteBackend
        from lazy_loader import LazyCSVLoader
        
        df = None
        try:
            if load_mode == 'sqlite':
                self.backend_instance = SQLiteBackend(self.app)
                self.backend_instance.cancelled = self.is_cancelled

                def progress_callback(status, current, total):
                    if self.is_cancelled:
                        self.backend_instance.cancelled = True
                        return False # キャンセルを伝える
                    self.task_progress.emit(status, current, total)
                    # self.app._update_progress_dialog(status, current, total) # 🔥 main_windowのプログレスを更新
                    # main_qt.pyの_update_progress_dialogはAsyncDataManager.task_progressに接続されているので、ここで直接呼ぶ必要はない。
                    # ここでの直接呼び出しは、クロススレッドアクセスの問題は起こさないものの、冗長かつ誤解を招くため削除。
                    return True # 続行

                columns, total_rows = self.backend_instance.import_csv_with_progress( # total_rowsも取得
                    filepath, 
                    encoding,
                    progress_callback=progress_callback
                )
                
                # self.app._close_progress_dialog() # 🔥 プログレスを閉じる
                self.close_progress_requested.emit() # ← 安全

                if self.is_cancelled or columns is None:
                    self.backend_instance.close()
                    self.backend_instance = None
                    # self.app.show_operation_status("読み込みをキャンセルしました。", 3000) # 🔥 状態通知
                    self.status_message_requested.emit("読み込みをキャンセルしました。", 3000, False) # ← 安全
                    # self.app.view_controller.show_welcome_screen() # 🔥 ウェルカム画面に戻す
                    self.show_welcome_requested.emit() # ← 安全
                    return # ここで終了

                if columns is not None:
                    self.backend_instance.header = columns
                    self.backend_instance.total_rows = total_rows # 総行数を設定
                    # 🔥 SQLiteモードの場合は、AsyncDataManagerから直接FileIOControllerのシグナルをemit
                    # main_qt.pyがこれを受けてbackendを設定する
                    self.app.file_io_controller.file_loaded.emit(self.backend_instance, filepath, encoding)
                    return # ここで終了
            
            elif load_mode == 'lazy':
                self.backend_instance = LazyCSVLoader(filepath, encoding)
                # self.app._close_progress_dialog() # 🔥 プログレスを閉じる
                self.close_progress_requested.emit() # ← 安全
                # 🔥 Lazyモードも同様に、AsyncDataManagerから直接FileIOControllerのシグナルをemit
                self.app.file_io_controller.file_loaded.emit(self.backend_instance, filepath, encoding)
                return # ここで終了
            
            else: # normal mode
                self.task_progress.emit("ファイルをメモリに読み込み中...", 0, 0)
                df = pd.read_csv(filepath, encoding=encoding, dtype=str).fillna('')
                self.task_progress.emit("読み込み完了", 1, 1)
                # self.app._close_progress_dialog() # 🔥 プログレスを閉じる
                self.close_progress_requested.emit() # ← 安全

            # normalモードの場合のみdata_readyシグナルを送信
            if not self.is_cancelled and load_mode == 'normal':
                self.data_ready.emit(df if df is not None else pd.DataFrame()) # data_readyはDataFrameを期待
            elif self.is_cancelled: # normalモードでキャンセルされた場合
                 # self.app.show_operation_status("読み込みをキャンセルしました。", 3000)
                 self.status_message_requested.emit("読み込みをキャンセルしました。", 3000, False)
                 # self.app.view_controller.show_welcome_screen()
                 self.show_welcome_requested.emit()

        except Exception as e:
            error_message = f"ファイル読み込みエラー: {e}"
            print(error_message)
            traceback.print_exc()
            # self.app._close_progress_dialog() # 🔥 プログレスを閉じる
            self.close_progress_requested.emit() # ← 安全
            if QApplication.instance():
                self.task_progress.emit(f"エラー: {e}", 1, 1)
                # self.app.show_operation_status(f"ファイル読み込みエラー: {e}", 5000, True) # 🔥 エラー通知
                self.status_message_requested.emit(f"ファイル読み込みエラー: {e}", 5000, True)
                # self.app.view_controller.show_welcome_screen() # 🔥 ウェルカム画面に戻す
                self.show_welcome_requested.emit()
            # エラー時は空のDataFrameを送信 (normalモードの場合のみ考慮されるが、一応残す)
            # ただし、error_occurredシグナルでUIに通知されるべき
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
                if self.backend_instance:
                    # SQLiteBackend.search は rowid と col_name を返す想定
                    # features.pyの_do_search_in_fileと同様の形式に変換
                    # 修正: headersの取得方法を修正
                    headers = self.app.table_model._headers # main_windowを通じてヘッダーを取得
                    col_name_to_idx = {name: idx for idx, name in enumerate(headers)}
                    raw_results = self.backend_instance.search(search_term, target_columns, is_case_sensitive, is_regex)
                    for row_idx, col_name in raw_results:
                        if col_name in col_name_to_idx:
                            results.append((row_idx, col_name_to_idx[col_name]))

            elif current_load_mode == 'lazy':
                if self.backend_instance:
                    total_rows = self.backend_instance.get_total_rows()
                    def progress_callback(current):
                        if self.is_cancelled:
                            self.backend_instance.cancelled = True
                        self.task_progress.emit("ファイル内を検索中...", current, total_rows)
                    
                    results = self.backend_instance.search_in_file(
                        search_term, target_columns, is_case_sensitive, is_regex,
                        progress_callback=progress_callback
                    )
            
            else: # normal mode (DataFrame in memory)
                df = self.app.table_model._dataframe
                if df is None or df.empty:
                    self.search_results_ready.emit([])
                    self.task_progress.emit("検索完了", 1, 1)
                    return

                pattern = re.compile(search_term if is_regex else re.escape(search_term),
                                     0 if is_case_sensitive else re.IGNORECASE)

                target_rows = list(range(df.shape[0]))
                if in_selection_only:
                    target_rows = sorted(list(selected_rows.intersection(target_rows)))

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
                            self.task_progress.emit("データ内を検索中...", processed_cells, total_search_cells)

            self.task_progress.emit("検索完了", 1, 1)
        except re.error as e:
            if QApplication.instance():
                # QCoreApplication.instance().callLater(self.app.show_operation_status, f"正規表現エラー: {e}", 5000, True)
                self.status_message_requested.emit(f"正規表現エラー: {e}", 5000, True)
            self.search_results_ready.emit([])
            return
        except Exception as e:
            print(f"Error during search: {traceback.format_exc()}")
            if QApplication.instance():
                # QCoreApplication.instance().callLater(self.app.show_operation_status, f"検索中にエラーが発生しました: {e}", 5000, True)
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
            # (📋 統合改善案 - ここから追加)
            # パラメータの検証
            required_params = ['lookup_filepath', 'lookup_file_encoding', 
                               'target_col', 'lookup_key_col', 'replace_val_col']
            missing_params = [p for p in required_params if p not in params]
            if missing_params:
                raise KeyError(f"必須パラメータが不足: {missing_params}")
            # (📋 統合改善案 - ここまで追加)

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
            
            # 使用するエンコーディングはparamsから取得 (SearchWidgetで既に検出されていることを想定)
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
                    discounted_price_decimal = Decimal(str(current_price)) * (Decimal('1.0') - discount_rate)
                    
                    final_price = self._apply_rounding(float(discounted_price_decimal), self.params['round_mode'])
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
                        discounted_price_decimal = Decimal(str(current_price)) * (Decimal('1.0') - discount_rate)
                        
                        final_price = self._apply_rounding(float(discounted_price_decimal), self.params['round_mode'])
                        final_price_str = str(int(final_price))
                        
                        if current_price_str != final_price_str:
                            changes.append({
                                'row_idx': idx,
                                'col_name': price_col,
                                'new_value': final_price_str
                            })
                            
                    except Exception as e:
                        print(f"WARNING: DB処理中の行{idx}でエラー: {e}")
                        continue
                
                if idx % 1000 == 0:
                    self.task_progress.emit(f"DBデータを処理中... ({idx}/{total_rows})", 50 + int(idx/total_rows * 40), 100)

            if changes:
                # この changes は {row_idx, col_name, new_value} 形式。
                # Undo履歴に追加するために {item, column, old, new} 形式に変換する必要がある。
                # しかし、ここではDBの更新のみを行い、Undo履歴への追加は main_qt.py で行うのが適切。
                # main_qt.py (_on_product_discount_completed) で changes を受け取り、Undo Manager に追加するようにする。
                self.backend.update_cells(changes)
                # layoutChanged.emit() は main_qt.py で_on_product_discount_completed の後に呼ばれるため、ここでは不要。
                # self.table_model.layoutChanged.emit() 
                
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