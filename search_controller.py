# search_controller.py

import re
import pandas as pd
from PySide6.QtCore import QObject, Signal, Qt, QModelIndex, QItemSelectionModel
from PySide6.QtWidgets import QApplication, QMessageBox, QAbstractItemView

class SearchController(QObject):
    """検索・置換・抽出機能を管理するコントローラー"""
    
    # シグナル定義
    search_completed = Signal(list)
    replace_completed = Signal(int)
    extract_completed = Signal(object)
    
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.search_results = []
        self.current_search_index = -1
        self._last_search_settings = None
        self._pending_operations = {
            'replace_all': False,
            'replace_current': False,
            'extract': False
        }
        self._pending_replace_current_settings = None
        self._pending_replace_settings = None
        self._pending_extract_settings = None

    def find_next(self, settings):
        """次を検索"""
        if not settings["search_term"]:
            self.main_window.show_operation_status("検索条件を入力してください。", is_error=True)
            return
        
        settings_changed = self._last_search_settings != settings
        if not self.search_results or settings_changed:
            self._last_search_settings = settings.copy()
            self.main_window.show_operation_status("検索中です...", duration=0)
            self._call_async_search(settings)
            return
        
        if len(self.search_results) > 0:
            self.current_search_index = (self.current_search_index + 1) % len(self.search_results)
            self._highlight_current_search_result()
            self.main_window.show_operation_status(
                f"検索結果 {self.current_search_index + 1}/{len(self.search_results)}件"
            )
        else:
            self.main_window.show_operation_status("検索結果がありません。", is_error=True)
    
    def find_prev(self, settings):
        """前を検索"""
        if not settings["search_term"]:
            self.main_window.show_operation_status("検索条件を入力してください。", is_error=True)
            return
        
        settings_changed = self._last_search_settings != settings
        if not self.search_results or settings_changed:
            self._last_search_settings = settings.copy()
            self.main_window.show_operation_status("検索中です...", duration=0)
            self._call_async_search(settings)
            return
        
        if len(self.search_results) > 0:
            self.current_search_index = (self.current_search_index - 1 + len(self.search_results)) % len(self.search_results)
            self._highlight_current_search_result()
            self.main_window.show_operation_status(
                f"検索結果 {self.current_search_index + 1}/{len(self.search_results)}件"
            )
        else:
            self.main_window.show_operation_status("検索結果がありません。", is_error=True)
    
    def replace_current(self, settings):
        """現在の検索結果を置換"""
        if self.main_window.is_readonly_mode(for_edit=True):
            self.main_window.show_operation_status("このモードでは置換できません。", 3000, is_error=True)
            return
        
        if not settings["search_term"]:
            self.main_window.show_operation_status("検索条件を入力してください。", is_error=True)
            return
        
        settings_changed = self._last_search_settings != settings
        if not self.search_results or settings_changed or self.current_search_index == -1:
            self.main_window.show_operation_status("置換対象を検索中です...", duration=0)
            self._pending_operations['replace_current'] = True
            self._pending_replace_current_settings = settings.copy()
            self._last_search_settings = settings.copy()
            self._call_async_search(settings)
            return
        
        self._execute_current_replace(settings)
    
    def replace_all(self, settings):
        """すべて置換"""
        if self.main_window.is_readonly_mode(for_edit=True):
            self.main_window.show_operation_status("このモードではすべて置換できません。", 3000, is_error=True)
            return
        
        self._last_search_settings = settings.copy()
        self._pending_operations['replace_all'] = True
        self._pending_replace_settings = settings.copy()
        self.main_window.show_operation_status("置換対象を検索中です...", duration=0)
        self._call_async_search(settings)
    
    def execute_extract(self, settings):
        """抽出実行"""
        print(f"DEBUG: execute_extract 開始 - 設定: {settings}") # デバッグログ追加
        
        if not settings["search_term"]:
            self.main_window.show_operation_status("検索条件を入力してください。", is_error=True)
            return

        settings_changed = self._last_search_settings != settings
        if not self.search_results or settings_changed:
            print("DEBUG: 新しい検索が必要 - 検索を実行中") # デバッグログ追加
            self._last_search_settings = settings.copy()
            self._pending_operations['extract'] = True
            self._pending_extract_settings = settings.copy()
            self.main_window.show_operation_status("抽出対象を検索中です...", duration=0)
            self._call_async_search(settings)
            return

        print(f"DEBUG: 既存の検索結果を使用 - {len(self.search_results)}件") # デバッグログ追加
        self._execute_extract_with_results(self.search_results)
    
    def handle_search_results_ready(self, results):
        """検索結果受信処理（AsyncDataManagerから呼ばれる）"""
        print(f"DEBUG: handle_search_results_ready - 受信した検索結果: {len(results)}件")
        print(f"DEBUG: 検索結果詳細（最初の3件）: {results[:3]}")
        
        self.main_window._close_progress_dialog()
        self.main_window.progress_bar.hide()
        
        # 親子関係モードでのフィルタリング
        if self._last_search_settings:
            results = self._filter_results_by_parent_child_mode(results, self._last_search_settings)
        
        self.search_results = sorted(list(set(results)))
        print(f"DEBUG: フィルタリング後の検索結果: {len(self.search_results)}件")
        self.current_search_index = -1 # 検索結果が新しくなったのでリセット
        
        # ハイライト設定
        highlight_indexes = [] # より安全なインデックス作成
        for row, col in self.search_results:
            if 0 <= row < self.main_window.table_model.rowCount() and 0 <= col < self.main_window.table_model.columnCount():
                idx = self.main_window.table_model.index(row, col)
                if idx.isValid():
                    highlight_indexes.append(idx)
                else:
                    print(f"DEBUG: 無効なインデックス作成失敗: row={row}, col={col}")
            else:
                print(f"DEBUG: 範囲外のインデックス: row={row}, col={col}, max_row={self.main_window.table_model.rowCount()}, max_col={self.main_window.table_model.columnCount()}")
        
        print(f"DEBUG: ハイライト用インデックス作成: {len(highlight_indexes)}個")
        valid_indexes = [idx for idx in highlight_indexes if idx.isValid()]
        print(f"DEBUG: 有効なインデックス: {len(valid_indexes)}個")
        
        self.main_window.table_model.set_search_highlight_indexes(highlight_indexes)
        
        # ペンディング操作の処理
        if self._pending_operations['replace_current']:
            self._pending_operations['replace_current'] = False
            if self.search_results:
                self.current_search_index = 0
                self._highlight_current_search_result()
                self._execute_current_replace(self._pending_replace_current_settings)
            else:
                self.main_window.show_operation_status("置換対象が見つかりませんでした。", 3000)
            self._pending_replace_current_settings = None
            return
        
        if self._pending_operations['replace_all']:
            self._pending_operations['replace_all'] = False
            # 🔥 修正: execute_replace_all_in_db の戻り値が変わったため、受け取り方を修正
            # self._execute_replace_all_with_results(self._pending_replace_settings, self.search_results) # 修正前
            
            # _execute_replace_all_with_results は db_backend の結果を受け取る必要がないので、そのまま渡す
            # ただし、Undo履歴の追加は search_controller 側で行う
            self._execute_replace_all_with_results(self._pending_replace_settings, self.search_results)
            
            self._pending_replace_settings = None
            return
        
        # 🔥 追加: 抽出のペンディング処理
        if self._pending_operations['extract']:
            print("DEBUG: extract のペンディング処理を実行") # デバッグログ追加
            self._pending_operations['extract'] = False
            self._execute_extract_with_results(self.search_results)
            self._pending_extract_settings = None
            return
        
        # 通常の検索結果表示
        if not self.search_results:
            self.main_window.show_operation_status("検索: 一致する項目は見つかりませんでした。", 3000)
            return
        
        if len(self.search_results) > 0:
            self.current_search_index = 0
            print(f"DEBUG: 最初の検索結果をハイライト: {self.search_results[0]}")
            self._highlight_current_search_result()
            self.main_window.show_operation_status(f"検索: {len(self.search_results)}件見つかりました。")
        
        self.search_completed.emit(self.search_results)
    
    def clear_search_highlight(self):
        """検索ハイライトをクリア"""
        print("DEBUG: 検索ハイライトをクリア中") # デバッグログ追加
        
        # ハイライトインデックスをクリア
        self.main_window.table_model.set_search_highlight_indexes([])
        
        # 現在の検索インデックスをクリア
        self.main_window.table_model.set_current_search_index(QModelIndex())
        
        # 内部状態をリセット
        self.search_results = []
        self.current_search_index = -1
        
        print("DEBUG: ハイライトクリア完了") # デバッグログ追加
    
    def _call_async_search(self, settings):
        """非同期検索を呼び出す"""
        self.main_window._show_progress_dialog("検索中...", None)
        
        parent_child_data = self.main_window.parent_child_manager.parent_child_data
        selected_rows = set()
        if settings.get("in_selection_only"):
            selected_rows = {idx.row() for idx in self.main_window.table_view.selectionModel().selectedIndexes()}
        
        self.main_window.async_manager.search_data_async(
            settings,
            self.main_window.async_manager.current_load_mode,
            parent_child_data,
            selected_rows
        )
    
    def _highlight_current_search_result(self):
        """現在の検索結果をハイライト"""
        print(f"DEBUG: _highlight_current_search_result 開始")
        print(f"DEBUG: search_results数: {len(self.search_results)}, current_index: {self.current_search_index}")
        
        if not self.search_results or self.current_search_index == -1:
            self.main_window.table_model.set_current_search_index(QModelIndex())
            print("DEBUG: 有効な検索結果またはインデックスがありません")
            return
        
        row, col = self.search_results[self.current_search_index]
        print(f"DEBUG: ハイライト対象セル: row={row}, col={col}")
        
        index = self.main_window.table_model.index(row, col)
        print(f"DEBUG: QModelIndex作成: valid={index.isValid()}, row={index.row()}, col={index.column()}")

        if index.isValid():
            print("DEBUG: テーブルビューにスクロール要求")
            self.main_window.table_view.scrollTo(index, QAbstractItemView.PositionAtCenter)
            
            print("DEBUG: 選択状態をクリア")
            self.main_window.table_view.selectionModel().clearSelection()
            
            print("DEBUG: 現在のインデックスを設定")
            self.main_window.table_view.selectionModel().setCurrentIndex(
                index, 
                QItemSelectionModel.ClearAndSelect
            )
            
            print("DEBUG: テーブルモデルにハイライト要求")
            self.main_window.table_model.set_current_search_index(index)
            
            print(f"DEBUG: ハイライト処理完了 - セル({row},{col})")
            self.main_window.table_view.viewport().update() # 強制再描画
        else:
            self.main_window.table_model.set_current_search_index(QModelIndex())
            print(f"DEBUG: 無効なインデックス: row={row}, col={col}")
    
    def _execute_current_replace(self, settings):
        """現在の検索結果を置換"""
        if self.main_window.is_readonly_mode(for_edit=True):
            self.main_window.show_operation_status("このモードでは置換できません。", 3000, is_error=True)
            return
        
        if not settings["search_term"]:
            self.main_window.show_operation_status("検索条件を入力してください。", is_error=True)
            return
        
        row, col = self.search_results[self.current_search_index]
        index = self.main_window.table_model.index(row, col)
        old_value = self.main_window.table_model.data(index, Qt.EditRole)
        
        try:
            # 正規表現のコンパイルにMULTILINEフラグを考慮
            if settings["is_regex"]:
                flags = 0
                if not settings["is_case_sensitive"]:
                    flags |= re.IGNORECASE
                if '^' in settings["search_term"] or '$' in settings["search_term"]:
                    flags |= re.MULTILINE
                pattern = re.compile(settings["search_term"], flags)
            else:
                pattern = re.compile(
                    re.escape(settings["search_term"]),
                    0 if settings["is_case_sensitive"] else re.IGNORECASE
                )
            
            new_value = pattern.sub(settings["replace_term"], str(old_value))
            
            if str(old_value) != new_value:
                action = {
                    'type': 'edit',
                    'data': [{
                        'item': str(row),
                        'column': self.main_window.table_model.headerData(col, Qt.Horizontal),
                        'old': str(old_value),
                        'new': new_value
                    }]
                }
                self.main_window.undo_manager.add_action(action)
                self.main_window.apply_action(action, is_undo=False)
                self.main_window.show_operation_status("1件のセルを置換しました。")
                
                # 置換済みの結果を検索結果から削除
                self.search_results.pop(self.current_search_index)
                if not self.search_results:
                    self.clear_search_highlight()
                    self.main_window.show_operation_status("全ての検索結果を置換しました。")
                    return
                elif self.current_search_index >= len(self.search_results):
                    self.current_search_index = 0
                
                self._highlight_current_search_result()
                highlight_indexes = [self.main_window.table_model.index(r, c) for r, c in self.search_results]
                self.main_window.table_model.set_search_highlight_indexes(highlight_indexes)
            else:
                self.main_window.show_operation_status("変更がありませんでした。", 2000)
                
        except re.error as e:
            self.main_window.show_operation_status(f"正規表現エラー: {e}", 3000, is_error=True)
        except Exception as e:
            self.main_window.show_operation_status(f"置換エラー: {e}", 3000, is_error=True)
        
    def _execute_replace_all_with_results(self, settings, found_indices):
        """すべて置換処理（完全修正版）"""
        print(f"DEBUG: _execute_replace_all_with_results 開始 - 設定: {settings}") # デバッグログ追加
        
        if not found_indices:
            self.main_window.show_operation_status("置換対象が見つかりませんでした。", 3000)
            return

        # 親子関係モードでのフィルタリング
        filtered_indices = self._filter_results_by_parent_child_mode(found_indices, settings)

        if not filtered_indices:
            self.main_window.show_operation_status("親子関係の条件に一致する置換対象が見つかりませんでした。", 3000)
            return

        # 大量置換の警告
        if len(filtered_indices) > 5000:
            reply = QMessageBox.question(
                self.main_window,
                "大量の置換確認",
                f"{len(filtered_indices):,}件の置換を実行します。\n"
                f"処理に時間がかかる可能性があります。続行しますか？",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if reply == QMessageBox.No:
                return

        # 正規表現のコンパイルを最適化
        try:
            if settings["is_regex"]:
                flags = 0
                if not settings["is_case_sensitive"]:
                    flags |= re.IGNORECASE
                # 行頭・行末のメタ文字がある場合はMULTILINEを追加
                if '^' in settings["search_term"] or '$' in settings["search_term"]:
                    flags |= re.MULTILINE
                
                pattern = re.compile(settings["search_term"], flags)
            else:
                pattern = re.compile(
                    re.escape(settings["search_term"]),
                    0 if settings["is_case_sensitive"] else re.IGNORECASE
                )
        except re.error as e:
            self.main_window.show_operation_status(f"正規表現エラー: {e}", is_error=True)
            return

        # DBモードの場合
        if self.main_window.db_backend:
            print("DEBUG: DBモードで置換を実行") # デバッグログ追加
            
            # 🔥 修正: db_backend.execute_replace_all_in_db の戻り値に changes_for_undo を追加
            success, updated_count, changes_for_undo = self.main_window.db_backend.execute_replace_all_in_db(settings) # 修正

            if success:
                print(f"DEBUG: 置換成功 - {updated_count}件を更新") # デバッグログ追加
                
                # 🔥 追加: Undo履歴に追加
                if changes_for_undo: # changes_for_undo が空でない場合のみ追加
                    action = {'type': 'edit', 'data': changes_for_undo}
                    self.main_window.undo_manager.add_action(action)
                    print(f"DEBUG: Undo履歴に追加 - {len(changes_for_undo)}件の変更")
                
                # 🔥 重要: キャッシュを完全にクリア
                if hasattr(self.main_window.table_model, '_row_cache'): #
                    self.main_window.table_model._row_cache.clear() #
                if hasattr(self.main_window.table_model, '_cache_queue'): #
                    self.main_window.table_model._cache_queue.clear() #
                
                # 🔥 重要: モデルを完全にリセットしてUIを更新
                self.main_window.table_model.beginResetModel() #
                self.main_window.table_model.endResetModel() #
                
                # 🔥 重要: 検索ハイライトをクリア
                self.clear_search_highlight() #
                
                # 🔥 重要: 現在の検索インデックスもクリア
                self.main_window.table_model.set_current_search_index(QModelIndex()) #
                
                # 成功メッセージ
                self.main_window.show_operation_status(
                    f"{updated_count}件のセルを置換しました。" #
                )
            else:
                print("DEBUG: 置換失敗") # デバッグログ追加
                self.main_window.show_operation_status("置換に失敗しました。", is_error=True) #
            
            return
        
        # 通常のDataFrame処理（既存のコード）
        changes = []
        try:
            pattern = re.compile(
                settings["search_term"] if settings["is_regex"] else re.escape(settings["search_term"]),
                0 if settings["is_case_sensitive"] else re.IGNORECASE
            )
        except re.error as e:
            self.main_window.show_operation_status(f"正規表現エラー: {e}", is_error=True)
            return
        
        for row, col in filtered_indices:
            index = self.main_window.table_model.index(row, col)
            old_value = str(self.main_window.table_model.data(index, Qt.EditRole) or "")
            new_value = pattern.sub(settings["replace_term"], old_value)
            
            if old_value != new_value:
                changes.append({
                    'item': str(row),
                    'column': self.main_window.table_model.headerData(col, Qt.Horizontal),
                    'old': old_value,
                    'new': new_value
                })
        
        if changes:
            action = {'type': 'edit', 'data': changes}
            self.main_window.undo_manager.add_action(action)
            self.main_window.apply_action(action, is_undo=False)
            self.main_window.show_operation_status(
                f"{len(changes)}件のセルを置換しました。（親子関係: {settings.get('target_type', 'all')}）"
            )
            self.clear_search_highlight()
            self.replace_completed.emit(len(changes))
        else:
            self.main_window.show_operation_status("置換による変更はありませんでした。", 3000)
    
    def _execute_extract_with_results(self, found_indices): # 新規追加メソッド
        """抽出処理""" #
        print(f"DEBUG: _execute_extract_with_results 開始 - {len(found_indices)}件") # デバッグログ追加
        
        if not found_indices: #
            self.main_window.show_operation_status("抽出対象が見つかりませんでした。", 3000) #
            return #

        # 行インデックスを抽出 #
        row_indices = sorted(list({idx[0] for idx in found_indices})) #
        print(f"DEBUG: 抽出対象行インデックス: {row_indices[:5]}... ({len(row_indices)}件)") # デバッグログ追加

        extracted_df = None #
        
        if self.main_window.db_backend: #
            print("DEBUG: SQLiteBackendから行データを取得") # デバッグログ追加
            extracted_df = self.main_window.db_backend.get_rows_by_ids(row_indices) #
            
            # ヘッダー順序を保証 #
            if not extracted_df.empty and set(self.main_window.table_model._headers).issubset(extracted_df.columns): #
                extracted_df = extracted_df[self.main_window.table_model._headers] #
        else: #
            print("DEBUG: DataFrameから行データを取得") # デバッグログ追加
            extracted_df = self.main_window.table_model.get_rows_as_dataframe(row_indices).reset_index(drop=True) #

        if extracted_df is None or extracted_df.empty: #
            self.main_window.show_operation_status("抽出結果のデータが空です。", 3000, is_error=True) #
            return #

        print(f"DEBUG: 抽出されたDataFrameの形状: {extracted_df.shape}") # デバッグログ追加

        # 新しいウィンドウ作成シグナルをemit #
        self.main_window.create_extract_window_signal.emit(extracted_df.copy()) #
        self.extract_completed.emit(extracted_df) #
    
    def _filter_results_by_parent_child_mode(self, results, settings):
        """親子関係モードに基づいて検索結果をフィルタリング"""
        if not settings.get("is_parent_child_mode", False):
            return results
        
        if not self.main_window.parent_child_manager.parent_child_data:
            self.main_window.show_operation_status(
                "親子関係が分析されていません。先に分析を実行してください。", 
                is_error=True
            )
            return []
        
        target_type = settings.get("target_type", "all")
        filtered_results = []
        
        for row, col in results:
            if row in self.main_window.parent_child_manager.parent_child_data:
                parent_child_info = self.main_window.parent_child_manager.parent_child_data[row]
                is_parent = parent_child_info['is_parent']
                
                if target_type == "all":
                    filtered_results.append((row, col))
                elif target_type == "parent" and is_parent:
                    filtered_results.append((row, col))
                elif target_type == "child" and not is_parent:
                    filtered_results.append((row, col))
        
        return filtered_results
    
    def _analyze_parent_child_from_widget(self):
        """検索パネルからの親子関係分析要求処理"""
        settings = self.main_window.search_panel.get_settings()
        column_name = settings.get("key_column")
        analysis_mode = settings.get("analysis_mode", "consecutive") # デフォルト値を設定

        if not column_name:
            self.main_window.show_operation_status("親子関係分析のキー列を選択してください。", is_error=True)
            return
        
        if self.main_window.lazy_loader:
            QMessageBox.warning(self.main_window, "機能制限", "遅延読み込みモードでは親子関係の分析はできません。")
            if self.main_window.search_panel:
                self.main_window.search_panel.analysis_text.setText("遅延読み込みモードでは親子関係の分析はできません。")
            return

        self.main_window._show_progress_dialog("親子関係を分析中...", self.main_window.async_manager.cancel_current_task)
        
        if self.main_window.db_backend:
            # DBバックエンドがある場合
            self.main_window.async_manager.analyze_parent_child_async(self.main_window.db_backend, column_name, analysis_mode)
        else:
            # DataFrameモードの場合
            df_to_analyze = self.main_window.table_model.get_dataframe()
            
            if df_to_analyze is None or df_to_analyze.empty:
                self.main_window._close_progress_dialog()
                if self.main_window.search_panel:
                    self.main_window.search_panel.analysis_text.setText("分析対象のデータがありません。")
                self.main_window.show_operation_status("分析対象のデータがありません。", is_error=True)
                return

            success, msg, total_rows = self.main_window.parent_child_manager.analyze_relationships(df_to_analyze, column_name, analysis_mode)
            self.main_window._close_progress_dialog()
            
            if success:
                if self.main_window.search_panel:
                    self.main_window.search_panel.analysis_text.setText(self.main_window.parent_child_manager.get_groups_summary())
                self.main_window.show_operation_status("親子関係を分析しました。")
            else:
                if self.main_window.search_panel:
                    self.main_window.search_panel.analysis_text.setText(f"分析エラー:\n{msg}")
                self.main_window.show_operation_status("親子関係の分析に失敗しました。", is_error=True)

    # 以下の _execute_individual_replace_for_parent_child, _execute_extract_with_results, _filter_results_by_parent_child_mode, _analyze_parent_child_from_widget
    # は、_execute_replace_all_with_results の直後に重複して存在していたため、最初の定義以外は削除
    # Pythonでは同じ名前のメソッドが複数定義された場合、最後の定義が有効になる。
    # しかし、コードの可読性と保守性のため、重複は避けるべき。
    # 提示された修正ガイドは、_execute_replace_all_with_results のみに焦点を当てているが、
    # その後の重複部分を削除し、一貫性を保つ。