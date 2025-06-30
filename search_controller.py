import re
import pandas as pd
from PySide6.QtCore import QObject, Signal, Qt, QModelIndex, QItemSelectionModel
from PySide6.QtWidgets import QApplication, QMessageBox, QAbstractItemView # QAbstractItemView を追加

# TooltipEventFilterクラスとその前のコメント（7-19行目）を完全に削除

class SearchController(QObject):
    """検索・置換・抽出機能を管理するコントローラー"""
    
    # シグナル定義
    search_completed = Signal(list)  # search results (list of (row, col))
    replace_completed = Signal(int)  # replaced count
    extract_completed = Signal(object)  # extracted dataframe
    
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window # CsvEditorAppQtのインスタンス
        self.search_results = [] # 検索結果を (row, col) のタプルのリストとして保持
        self.current_search_index = -1
        self._last_search_settings = None # 前回の検索設定を保持
        self._pending_operations = { # 検索後に実行する保留中の操作
            'replace_all': False,
            'replace_current': False,
            'extract': False
        }
        self._pending_replace_current_settings = None
        self._pending_replace_settings = None # replace_all用
        self._pending_extract_settings = None # extract用

    def find_next(self, settings):
        """次を検索"""
        if not settings["search_term"]:
            self.main_window.show_operation_status("検索条件を入力してください。", is_error=True)
            return
        
        settings_changed = self._last_search_settings != settings
        if not self.search_results or settings_changed:
            self._last_search_settings = settings.copy()
            self.main_window.show_operation_status("検索中です...", duration=0)
            self._call_async_search(settings) # 非同期検索を呼び出し
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
        if not settings["search_term"]:
            self.main_window.show_operation_status("検索条件を入力してください。", is_error=True)
            return
        
        settings_changed = self._last_search_settings != settings
        if not self.search_results or settings_changed:
            self.main_window._last_search_settings = settings.copy()
            self._pending_operations['extract'] = True
            self._pending_extract_settings = settings.copy()
            self.main_window.show_operation_status("抽出対象を検索中です...", duration=0)
            self._call_async_search(settings)
            return
        
        self._execute_extract_with_results(self.search_results)
    
    def handle_search_results_ready(self, results):
        """検索結果受信処理（AsyncDataManagerから呼ばれる）"""
        self.main_window._close_progress_dialog()
        self.main_window.progress_bar.hide()
        
        # 親子関係モードでのフィルタリング
        if self._last_search_settings:
            results = self._filter_results_by_parent_child_mode(results, self._last_search_settings)
        
        self.search_results = sorted(list(set(results)))
        self.current_search_index = -1 # 検索結果が新しくなったのでリセット
        
        # ハイライト設定
        highlight_indexes = [self.main_window.table_model.index(row, col) for row, col in self.search_results]
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
            self._execute_replace_all_with_results(self._pending_replace_settings, self.search_results)
            self._pending_replace_settings = None # Clear pending settings 🔥 ここを修正
            return
        
        if self._pending_operations['extract']:
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
            self._highlight_current_search_result()
            self.main_window.show_operation_status(f"検索: {len(self.search_results)}件見つかりました。")
        
        self.search_completed.emit(self.search_results) # 外部に検索完了を通知
    
    def clear_search_highlight(self):
        """検索ハイライトをクリア"""
        self.main_window.table_model.set_search_highlight_indexes([])
        self.main_window.table_model.set_current_search_index(QModelIndex())
        self.search_results = []
        self.current_search_index = -1
    
    def _call_async_search(self, settings):
        """非同期検索を呼び出す"""
        self.main_window._show_progress_dialog("検索中...", None) # main_windowのメソッドを呼び出し
        
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
        if not self.search_results or self.current_search_index == -1:
            self.main_window.table_model.set_current_search_index(QModelIndex())
            return
        
        row, col = self.search_results[self.current_search_index]
        index = self.main_window.table_model.index(row, col)
        
        if index.isValid():
            # 🔥 修正: QAbstractItemView.PositionAtCenter を直接参照
            self.main_window.table_view.scrollTo(index, QAbstractItemView.PositionAtCenter)
            self.main_window.table_view.selectionModel().clearSelection()
            # 選択を単一セルにする
            self.main_window.table_view.selectionModel().setCurrentIndex(
                index, 
                QItemSelectionModel.ClearAndSelect # 既存選択をクリアして選択
            )
            self.main_window.table_model.set_current_search_index(index)
        else:
            self.main_window.table_model.set_current_search_index(QModelIndex())
    
    def _execute_current_replace(self, settings):
        """現在の検索結果を置換"""
        if self.current_search_index < 0 or self.current_search_index >= len(self.search_results):
            self.main_window.show_operation_status("置換する検索結果が選択されていません。", is_error=True)
            return
        
        row, col = self.search_results[self.current_search_index]
        index = self.main_window.table_model.index(row, col)
        old_value = self.main_window.table_model.data(index, Qt.DisplayRole)
        
        try:
            pattern = re.compile(
                settings["search_term"] if settings["is_regex"] else re.escape(settings["search_term"]),
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
                self.main_window.apply_action(action, is_undo=False) # main_windowのapply_actionを呼び出し
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
        """すべて置換処理"""
        if not found_indices:
            self.main_window.show_operation_status("置換対象が見つかりませんでした。", 3000)
            return
        
        # 親子関係モードでのフィルタリング
        filtered_indices = self._filter_results_by_parent_child_mode(found_indices, settings)
        
        if not filtered_indices:
            self.main_window.show_operation_status("親子関係の条件に一致する置換対象が見つかりませんでした。", 3000)
            return
        
        if self.main_window.db_backend:
            # DBモードの場合は個別処理
            # ⭐ ここで_execute_individual_replace_for_parent_child を呼び出す前にUndo履歴に追加するように修正が必要
            # ただし、_execute_individual_replace_for_parent_child の内部でUndoActionを生成する方が自然
            self._execute_individual_replace_for_parent_child(settings, filtered_indices)
            return
        
        # 通常のDataFrame処理
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
            old_value = str(self.main_window.table_model.data(index, Qt.DisplayRole) or "")
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
            self.replace_completed.emit(len(changes)) # 置換完了シグナル
        else:
            self.main_window.show_operation_status("置換による変更はありませんでした。", 3000)
    
    def _execute_extract_with_results(self, found_indices):
        """抽出処理"""
        if not found_indices:
            self.main_window.show_operation_status("抽出対象が見つかりませんでした。", 3000)
            return
        
        row_indices = sorted(list({idx[0] for idx in found_indices}))
        print(f"DEBUG: 抽出対象行インデックス: {row_indices[:5]}... ({len(row_indices)}件)")
        
        extracted_df = None
        if self.main_window.db_backend:
            print("DEBUG: SQLiteBackendから行データを取得")
            extracted_df = self.main_window.db_backend.get_rows_by_ids(row_indices)
            # ヘッダー順序を保証
            if not extracted_df.empty and set(self.main_window.table_model._headers).issubset(extracted_df.columns):
                extracted_df = extracted_df[self.main_window.table_model._headers]
        else:
            print("DEBUG: DataFrameから行データを取得")
            extracted_df = self.main_window.table_model.get_rows_as_dataframe(row_indices).reset_index(drop=True)
        
        if extracted_df is None or extracted_df.empty:
            self.main_window.show_operation_status("抽出結果のデータが空です。", 3000, is_error=True)
            return
        
        print(f"DEBUG: 抽出されたDataFrameの形状: {extracted_df.shape}")
        # 新しいウィンドウ作成シグナルをmain_windowにemit
        self.main_window.create_extract_window_signal.emit(extracted_df.copy())
        self.extract_completed.emit(extracted_df) # 抽出完了シグナル
    
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
        # このメソッドはsearch_widgetから呼ばれるが、実際にはmain_windowのsearch_controllerに委譲されている
        # したがって、main_windowのsearch_panelから設定を取得し、main_windowのasync_managerを介して処理を行う
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

            success, msg = self.main_window.parent_child_manager.analyze_relationships(df_to_analyze, column_name, analysis_mode)
            self.main_window._close_progress_dialog()
            
            if success:
                if self.main_window.search_panel:
                    self.main_window.search_panel.analysis_text.setText(self.main_window.parent_child_manager.get_groups_summary())
                self.main_window.show_operation_status("親子関係を分析しました。")
            else:
                if self.main_window.search_panel:
                    self.main_window.search_panel.analysis_text.setText(f"分析エラー:\n{msg}")
                self.main_window.show_operation_status("親子関係の分析に失敗しました。", is_error=True)

    def _execute_individual_replace_for_parent_child(self, settings, filtered_indices):
        """親子関係モード用の個別置換処理 (DBモードの場合)"""
        changes = []
        try:
            pattern = re.compile(
                settings["search_term"] if settings["is_regex"] else re.escape(settings["search_term"]),
                0 if settings["is_case_sensitive"] else re.IGNORECASE
            )
        except re.error as e:
            self.main_window.show_operation_status(f"正規表現エラー: {e}", is_error=True)
            return
        
        # 影響を受ける行をデータベースから一度に取得
        rows_to_fetch = sorted(list(set(row for row, col in filtered_indices)))
        df_rows = self.main_window.db_backend.get_rows_by_ids(rows_to_fetch)
        
        for row, col in filtered_indices:
            if row in df_rows.index:
                col_name = self.main_window.table_model.headerData(col, Qt.Horizontal)
                old_value = str(df_rows.loc[row, col_name] if col_name in df_rows.columns else "")
                new_value = pattern.sub(settings["replace_term"], old_value)
                
                if old_value != new_value:
                    changes.append({
                        'row_idx': row,
                        'col_name': col_name,
                        'new_value': new_value,
                        'old_value': old_value # Undoのために旧値も保存
                    })
        
        if changes:
            # ⭐ DBモードでの置換もUndo/Redo履歴に追加
            undo_data = []
            for change in changes:
                undo_data.append({
                    'item': str(change['row_idx']),
                    'column': change['col_name'],
                    'old': change['old_value'],
                    'new': change['new_value']
                })
            action = {'type': 'edit', 'data': undo_data}
            self.main_window.undo_manager.add_action(action)
            self.main_window.db_backend.update_cells(changes)
            self.main_window.table_model.layoutChanged.emit() # モデル更新を通知
            self.main_window.show_operation_status(
                f"{len(changes)}件のセルを置換しました。（親子関係: {settings.get('target_type', 'all')}）"
            )
        else:
            self.main_window.show_operation_status("置換による変更はありませんでした。", 3000)