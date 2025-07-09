# table_operations.py
"""
テーブル編集操作の専門ファイル
- コピー、ペースト、削除などの基本操作
- セルや列の連結操作
- main_qt.pyから分離して整理する目的
"""

from PySide6.QtWidgets import QApplication, QMessageBox, QDialog, QInputDialog
from PySide6.QtCore import Qt, QModelIndex # QModelIndex をインポート
import pandas as pd
from io import StringIO
import re # re をインポート

from dialogs import PasteOptionDialog, MergeSeparatorDialog, RemoveDuplicatesDialog


class TableOperationsManager:
    """
    テーブル編集操作の専門クラス
    
    役割：
    - CSVテーブルの編集に関する機能をまとめて管理
    - main_qt.pyから編集関連のコードを分離
    """
    
    def __init__(self, main_window):
        """
        初期化：必要な情報をmain_windowから受け取る
        
        Args:
            main_window: メインウィンドウ（CsvEditorAppQt）
        """
        # main_window への参照のみを保存
        self.main_window = main_window
        self.column_clipboard = None  # 列コピー用クリップボード
    
    # プロパティで動的にアクセス
    @property
    def table_view(self):
        """動的に table_view を取得"""
        return self.main_window.table_view
        
    @property
    def table_model(self):
        """動的に table_model を取得"""
        return self.main_window.table_model
        
    @property
    def undo_manager(self):
        """動的に undo_manager を取得"""
        return self.main_window.undo_manager
    
    def copy(self):
        """
        選択されたセルをクリップボードにコピー
        """
        # プロパティ経由でアクセス
        selected = self.table_view.selectionModel().selectedIndexes()
        
        # 何も選択されていない場合は終了
        if not selected:
            self.main_window.show_operation_status("コピーするセルを選択してください。", is_error=True)
            return
        
        min_r = min(idx.row() for idx in selected)
        max_r = max(idx.row() for idx in selected)

        selected_col_indices = sorted(list(set(idx.column() for idx in selected)))
        selected_col_names = [self.table_model.headerData(idx, Qt.Horizontal) for idx in selected_col_indices]

        # get_rows_as_dataframe を使用して選択行のDataFrameを取得
        # selectedIndexes() が飛び飛びの行を持つ可能性があるため、min_r から max_r までの範囲ではなく、
        # 実際に選択された行のインデックスのみを渡すように修正
        actual_selected_rows = sorted(list(set(idx.row() for idx in selected)))
        df_selected_rows = self.table_model.get_rows_as_dataframe(actual_selected_rows)

        # 選択された列だけを抽出
        df_to_copy = df_selected_rows[selected_col_names]

        output = StringIO()
        df_to_copy.to_csv(output, sep='\t', index=False, header=False)
        QApplication.clipboard().setText(output.getvalue().strip())
        output.close()

        self.main_window.show_operation_status(f"{len(selected)}個のセルをコピーしました")

    def cut(self):
        """切り取り = コピー + 削除"""
        if self.main_window.is_readonly_mode(for_edit=True):
            self.main_window.show_operation_status(
                "このモードでは切り取りはできません。", is_error=True
            )
            return
        
        # コピーを実行してから、削除を実行
        self.copy()
        self.delete()

    def paste(self):
        """クリップボードの内容を選択位置に貼り付け（自動行追加対応版）"""
        if self.main_window.is_readonly_mode(for_edit=True):
            self.main_window.show_operation_status("このモードでは貼り付けできません。", is_error=True)
            return
        
        selection = self.table_view.selectionModel()
        clipboard_text = QApplication.clipboard().text()
        
        if not clipboard_text:
            self.main_window.show_operation_status("クリップボードにデータがありません。", is_error=True)
            return
        
        selected_indexes = selection.selectedIndexes()
        if not selected_indexes:
            self.main_window.show_operation_status("貼り付け開始位置を選択してください。", is_error=True)
            return
        
        # 最小の行と列を取得 (貼り付け開始位置)
        start_row = min(idx.row() for idx in selected_indexes)
        start_col = min(idx.column() for idx in selected_indexes)
        
        num_model_rows = self.table_model.rowCount()
        num_model_cols = self.table_model.columnCount()
        
        # クリップボードデータの解析
        pasted_df_raw = None
        is_single_value_clipboard = False
        
        try:
            pasted_df_raw = pd.read_csv(StringIO(clipboard_text), sep='\t', header=None, dtype=str, on_bad_lines='skip').fillna('')
        except Exception as e:
            print(f"Initial clipboard parsing failed with tab delimiter: {e}")
            pass
        
        if pasted_df_raw is None or pasted_df_raw.empty or (pasted_df_raw.shape[0] == 1 and pasted_df_raw.shape[1] == 1):
            is_single_value_clipboard = True
            value = clipboard_text.strip()
            if value == '""':
                value = ''
            pasted_df_raw = pd.DataFrame([[value]], dtype=str)
            print(f"DEBUG: クリップボードは単一値と判定: '{pasted_df_raw.iloc[0,0]}'")
        
        # ペーストオプションダイアログ
        paste_dialog = PasteOptionDialog(self.main_window, not is_single_value_clipboard and pasted_df_raw.shape[1] > 1)
        if paste_dialog.exec() != QDialog.Accepted:
            return
        
        paste_mode = paste_dialog.get_selected_mode()
        custom_delimiter = paste_dialog.get_custom_delimiter()
        
        # ペーストデータの準備
        pasted_df = None
        if is_single_value_clipboard:
            pasted_df = pasted_df_raw
        elif paste_mode == 'normal':
            pasted_df = pasted_df_raw
        elif paste_mode == 'single_column':
            single_column_lines = clipboard_text.split('\n')
            pasted_df = pd.DataFrame([line.strip() for line in single_column_lines], columns=[0], dtype=str).fillna('')
        elif paste_mode == 'custom_delimiter':
            try:
                pasted_df = pd.read_csv(StringIO(clipboard_text), sep=custom_delimiter, header=None, dtype=str, on_bad_lines='skip').fillna('')
            except Exception as e:
                self.main_window.show_operation_status(f"カスタム区切り文字での解析に失敗しました: {e}", is_error=True)
                return
        
        if pasted_df is None:
            return
        
        # 空セル正規化
        if pasted_df is not None:
            pasted_df = pasted_df.applymap(lambda x: '' if isinstance(x, str) and x == '""' else x)
        
        num_pasted_rows, num_pasted_cols = pasted_df.shape
        print(f"DEBUG: 貼り付け対象データ形状: {num_pasted_rows}行, {num_pasted_cols}列")
        
        # 🔥 重要：必要に応じて行を追加
        required_rows = start_row + num_pasted_rows
        if required_rows > num_model_rows:
            rows_to_add = required_rows - num_model_rows
            
            # ユーザーに確認
            reply = QMessageBox.question(
                self.main_window,
                "行の自動追加",
                f"貼り付けるデータが現在の行数を超えています。\n"
                f"{rows_to_add}行を自動的に追加しますか？\n\n"
                f"現在の行数: {num_model_rows}行\n"
                f"必要な行数: {required_rows}行",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.Yes
            )
            
            if reply == QMessageBox.Yes:
                # 行を追加
                print(f"DEBUG: {rows_to_add}行を追加します")
                
                # SQLiteモードやLazyモードの場合の警告
                if self.main_window.is_readonly_mode():
                    QMessageBox.warning(
                        self.main_window,
                        "警告",
                        "読み取り専用モードでは行の追加ができません。\n"
                        "通常モードで開き直してください。"
                    )
                    return
                
                # 行追加の実行
                for _ in range(rows_to_add):
                    self.table_model.insertRows(self.table_model.rowCount(), 1)
                
                # 行数を更新
                num_model_rows = self.table_model.rowCount()
                self.main_window.show_operation_status(f"{rows_to_add}行を追加しました", 2000)
            else:
                # ユーザーが行追加を拒否した場合、既存の行数内でペースト
                num_pasted_rows = min(num_pasted_rows, num_model_rows - start_row)
                pasted_df = pasted_df.iloc[:num_pasted_rows]
                self.main_window.show_operation_status("既存の行数内でペーストします", 2000)
        
        # 🔥 追加の改善提案：自動列追加機能も実装
        required_cols = start_col + num_pasted_cols
        if required_cols > num_model_cols:
            cols_to_add = required_cols - num_model_cols
            
            reply = QMessageBox.question(
                self.main_window,
                "列の自動追加",
                f"貼り付けるデータが現在の列数を超えています。\n"
                f"{cols_to_add}列を自動的に追加しますか？",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.Yes
            )
            
            if reply == QMessageBox.Yes:
                if self.main_window.is_readonly_mode():
                    QMessageBox.warning(
                        self.main_window,
                        "警告",
                        "読み取り専用モードでは列の追加ができません。\n"
                        "通常モードで開き直してください。"
                    )
                    return
                for i in range(cols_to_add):
                    col_name_base = "新規列"
                    counter = 1
                    # 既存のヘッダー名と衝突しないように調整
                    while f"{col_name_base}{num_model_cols + i + counter}" in self.table_model._headers:
                        counter += 1
                    final_col_name = f"{col_name_base}{num_model_cols + i + counter}"
                    self.table_model.insertColumns(num_model_cols + i, 1, names=[final_col_name])
                num_model_cols = self.table_model.columnCount() # 列数を更新
                self.main_window.show_operation_status(f"{cols_to_add}列を追加しました", 2000)
            else:
                # ユーザーが列追加を拒否した場合、既存の列数内でペースト
                num_pasted_cols = min(num_pasted_cols, num_model_cols - start_col)
                pasted_df = pasted_df.iloc[:, :num_pasted_cols]
                self.main_window.show_operation_status("既存の列数内でペーストします", 2000)

        # 変更履歴の収集
        changes = []
        
        # 単一値の処理（既存のコード）
        if is_single_value_clipboard:
            value_to_paste = pasted_df.iloc[0, 0]
            print(f"DEBUG: 単一値貼り付けモード: '{value_to_paste}'")

            # 選択範囲の解析
            selected_rows_indices = sorted(list(set(idx.row() for idx in selected_indexes)))
            selected_cols_indices = sorted(list(set(idx.column() for idx in selected_indexes)))

            is_full_column_selection = (len(selected_cols_indices) == 1 and len(selected_rows_indices) == num_model_rows)
            is_full_row_selection = (len(selected_rows_indices) == 1 and len(selected_cols_indices) == num_model_cols)

            if is_full_column_selection and num_model_rows > 0: # 列選択でデータがある場合
                target_col = selected_cols_indices[0]
                print(f"DEBUG: 1セルコピー → 1列全体選択 (列: {target_col})")
                for r_off in range(num_model_rows):
                    target_row = r_off
                    idx = self.table_model.index(target_row, target_col)
                    old_value = self.table_model.data(idx, Qt.EditRole)
                    if str(old_value) != value_to_paste:
                        changes.append({'item': str(target_row), 'column': self.table_model.headerData(target_col, Qt.Horizontal), 'old': str(old_value), 'new': value_to_paste})
            elif is_full_row_selection and num_model_cols > 0: # 行選択でデータがある場合
                target_row = selected_rows_indices[0]
                print(f"DEBUG: 1セルコピー → 1行全体選択 (行: {target_row})")
                for c_off in range(num_model_cols):
                    target_col = c_off
                    idx = self.table_model.index(target_row, target_col)
                    old_value = self.table_model.data(idx, Qt.EditRole)
                    if str(old_value) != value_to_paste:
                        changes.append({'item': str(target_row), 'column': self.table_model.headerData(target_col, Qt.Horizontal), 'old': str(old_value), 'new': value_to_paste})
            else:
                print(f"DEBUG: 単一セル貼り付けまたは複数セル塗りつぶし")
                for idx in selected_indexes:
                    row, col = idx.row(), idx.column()
                    old_value = self.table_model.data(idx, Qt.EditRole)
                    if str(old_value) != value_to_paste:
                        changes.append({'item': str(row), 'column': self.table_model.headerData(col, Qt.Horizontal), 'old': str(old_value), 'new': value_to_paste})
        
        else:
            # 複数セルの貼り付け
            print(f"DEBUG: 複数セル貼り付けモード")
            for r_off in range(num_pasted_rows):
                for c_off in range(num_pasted_cols):
                    r, c = start_row + r_off, start_col + c_off
        
                    # モデルの範囲内でのみ貼り付け
                    if r < num_model_rows and c < num_model_cols:
                        idx = self.table_model.index(r, c)
                        old_value = self.table_model.data(idx, Qt.EditRole)
                        new_value = pasted_df.iloc[r_off, c_off]
                        if str(old_value) != new_value:
                            changes.append({
                                'item': str(r),
                                'column': self.table_model.headerData(c, Qt.Horizontal),
                                'old': str(old_value),
                                'new': new_value
                            })
        
        # 変更の適用
        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action)
            self.main_window.apply_action(action, False)
            self.main_window.show_operation_status(f"{len(changes)}個のセルを貼り付けました。")
        else:
            self.main_window.show_operation_status("貼り付けによる変更はありませんでした。", 2000)
    
    def delete(self):
        """
        選択されたセルをクリア（削除）
        """
        if self.main_window.is_readonly_mode(for_edit=True):
            self.main_window.show_operation_status("このモードでは削除はできません。", is_error=True)
            return

        # プロパティ経由でアクセス
        selected = self.table_view.selectionModel().selectedIndexes()
        if not selected:
            self.main_window.show_operation_status("削除するセルを選択してください。", is_error=True)
            return

        changes = []
        for i in selected:
            # EditRoleで現在の完全な値を取得
            current_value = self.table_model.data(i, Qt.EditRole) # プロパティ経由でアクセス
            if current_value: # 値がある場合のみ変更として記録
                changes.append({
                    'item': str(i.row()),
                    'column': self.table_model.headerData(i.column(), Qt.Horizontal), # プロパティ経由でアクセス
                    'old': str(current_value),
                    'new': ""
                })

        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action) # プロパティ経由でアクセス
            self.main_window.apply_action(action, False)
            self.main_window.show_operation_status(f"{len(changes)}個のセルをクリアしました。")
        else:
            self.main_window.show_operation_status("削除する対象のセルがありませんでした。", 2000)

    def select_all(self):
        """
        テーブルの全セルを選択
        """
        # プロパティ経由でアクセス
        self.table_view.selectAll()
        self.main_window._update_action_button_states() # UIの状態を更新

    def copy_columns(self):
        """
        選択された列のデータを内部クリップボードにコピー
        """
        # プロパティ経由でアクセス
        selected_columns = self.table_view.selectionModel().selectedColumns()
        if not selected_columns:
            self.main_window.show_operation_status("コピーする列を選択してください。", is_error=True)
            return

        # 最初の選択列のインデックスを取得（複数列選択されていても最初の1列のみをコピー対象とする）
        col_index = selected_columns[0].column()
        col_name = self.table_model.headerData(col_index, Qt.Horizontal) # プロパティ経由でアクセス

        # 巨大ファイルモードで全列コピー時に警告
        if self.main_window.is_readonly_mode(for_edit=True) and self.table_model.rowCount() > 500000: # プロパティ経由でアクセス
             QMessageBox.warning(self.main_window, "警告", "巨大な列データをメモリにロードします。時間がかかる場合があります。")

        # モデルから列データを取得
        self.column_clipboard = self.table_model.get_column_data(col_index) # プロパティ経由でアクセス
        
        self.main_window.show_operation_status(f"列「{col_name}」({len(self.column_clipboard):,}行)をコピーしました。")
        self.main_window._update_action_button_states() # UIの状態を更新

    def paste_columns(self):
        """
        内部クリップボードに保存された列データを、選択された列に貼り付け
        """
        if self.main_window.is_readonly_mode(for_edit=True):
            self.main_window.show_operation_status("このモードでは貼り付けできません。", is_error=True)
            return

        if self.column_clipboard is None:
            self.main_window.show_operation_status("貼り付ける列データがありません。先に列をコピーしてください。", is_error=True)
            return

        # プロパティ経由でアクセス
        selected_columns = self.table_view.selectionModel().selectedColumns()
        if not selected_columns:
            self.main_window.show_operation_status("貼り付け先の列を選択してください。", is_error=True)
            return

        # 最初の選択列のインデックスを取得
        dest_col_index = selected_columns[0].column()
        dest_col_name = self.table_model.headerData(dest_col_index, Qt.Horizontal) # プロパティ経由でアクセス

        num_rows_to_paste = len(self.column_clipboard)
        if num_rows_to_paste != self.table_model.rowCount(): # プロパティ経由でアクセス
            reply = QMessageBox.question(self.main_window, "行数不一致の確認",
                                       f"コピー元の行数({num_rows_to_paste:,})と現在の行数({self.table_model.rowCount():,})が異なります。\n\n可能な限り貼り付けますか？", # プロパティ経由でアクセス
                                       QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            if reply == QMessageBox.No:
                return

        changes = []
        paste_limit = min(num_rows_to_paste, self.table_model.rowCount()) # プロパティ経由でアクセス

        for i in range(paste_limit):
            # EditRoleで現在の完全な値を取得
            old_val = self.table_model.data(self.table_model.index(i, dest_col_index), Qt.EditRole) # プロパティ経由でアクセス
            new_val = self.column_clipboard[i]
            
            # 値が異なる場合のみ変更として記録
            if str(old_val) != str(new_val):
                changes.append({
                    'item': str(i),
                    'column': dest_col_name,
                    'old': str(old_val), # old_valをstrに変換して保存
                    'new': str(new_val)  # new_valもstrに変換して保存
                })

        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action) # プロパティ経由でアクセス
            self.main_window.apply_action(action, is_undo=False)
            self.main_window.show_operation_status(f"{len(changes)}件を列「{dest_col_name}」に貼り付けました。")
        else:
            self.main_window.show_operation_status("変更はありませんでした。", 2000)

    def concatenate_cells(self, is_column_merge=False):
        """
        セルの値を連結、または列の値を連結
        """
        if self.main_window.is_readonly_mode(for_edit=True):
            self.main_window.show_operation_status("このモードではセル結合/列連結はできません。", is_error=True)
            return

        # プロパティ経由でアクセス
        current_index = self.table_view.currentIndex()
        if not current_index.isValid():
            self.main_window.show_operation_status("連結するセルを選択してください。", is_error=True)
            return

        current_row = current_index.row()
        current_col = current_index.column()

        # ターゲットの列が存在するか確認
        if current_col + 1 >= self.table_model.columnCount(): # プロパティ経由でアクセス
            self.main_window.show_operation_status("連結する隣の列/セルがありません。", is_error=True)
            return
            
        # 区切り文字ダイアログの表示
        dialog = MergeSeparatorDialog(self.main_window, is_column_merge=is_column_merge)
        if dialog.exec() != QDialog.Accepted:
            return
        separator = dialog.get_separator()

        changes = []
        current_col_name = self.table_model.headerData(current_col, Qt.Horizontal) # プロパティ経由でアクセス
        next_col_name = self.table_model.headerData(current_col + 1, Qt.Horizontal) # プロパティ経由でアクセス

        if is_column_merge: # 列連結の場合
            for row_idx in range(self.table_model.rowCount()): # プロパティ経由でアクセス
                # 現在のセルと隣のセルの値を取得
                current_value = str(self.table_model.data(self.table_model.index(row_idx, current_col), Qt.EditRole) or "") # プロパティ経由でアクセス
                next_value = str(self.table_model.data(self.table_model.index(row_idx, current_col + 1), Qt.EditRole) or "") # プロパティ経由でアクセス

                new_value = self._get_concatenated_value(current_value, next_value, separator)

                # 変更がある場合のみ記録
                if current_value != new_value:
                    changes.append({
                        'item': str(row_idx),
                        'column': current_col_name,
                        'old': current_value,
                        'new': new_value
                    })
                # 隣のセルが空でない場合、クリアする変更を記録
                if next_value:
                    changes.append({
                        'item': str(row_idx),
                        'column': next_col_name,
                        'old': next_value,
                        'new': ""
                    })
            
            status_message_base = f"列「{current_col_name}」と「{next_col_name}」を連結し、「{next_col_name}」をクリアしました"
            if changes:
                # 実際に値が変更された元の列の変更数のみをカウント
                num_main_col_changes = len([c for c in changes if c['column'] == current_col_name and c['old'] != c['new']])
                status_message = f"{status_message_base}（{num_main_col_changes}行）。"
            else:
                status_message = "連結による変更はありませんでした。"

        else: # セル連結の場合
            current_value = str(self.table_model.data(current_index, Qt.EditRole) or "") # プロパティ経由でアクセス
            next_index = self.table_model.index(current_row, current_col + 1)
            next_value = str(self.table_model.data(next_index, Qt.EditRole) or "") # プロパティ経由でアクセス

            new_value = self._get_concatenated_value(current_value, next_value, separator)

            # 変更がある場合のみ記録
            if current_value != new_value:
                changes.append({
                    'item': str(current_row),
                    'column': current_col_name,
                    'old': current_value,
                    'new': new_value
                })
            # 隣のセルが空でない場合、クリアする変更を記録
            if next_value:
                changes.append({
                    'item': str(current_row),
                    'column': next_col_name,
                    'old': next_value,
                    'new': ""
                })
            
            status_message = "セルを連結し、隣のセルをクリアしました。" if changes else "連結による変更はありませんでした。"

        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action) # プロパティ経由でアクセス
            self.main_window.apply_action(action, is_undo=False)
            self.main_window.show_operation_status(status_message)
        else:
            self.main_window.show_operation_status(status_message, 2000)

    def _get_concatenated_value(self, val1, val2, separator):
        """値と区切り文字を考慮して連結するヘルパーメソッド"""
        if val1 and val2:
            return f"{val1}{separator}{val2}"
        elif val1:
            return val1
        elif val2:
            return val2
        else:
            return ""

    def add_row(self):
        """
        現在の選択行の下に新しい行を追加
        """
        if self.main_window.is_readonly_mode(for_edit=True):
            self.main_window.show_operation_status("このモードでは行を追加できません。", is_error=True)
            return

        # プロパティ経由でアクセス
        current_index = self.table_view.currentIndex()
        row_pos = current_index.row() + 1 if current_index.isValid() else self.table_model.rowCount() # プロパティ経由でアクセス

        action = {'type': 'add_row', 'data': {'row_pos': row_pos}}
        self.undo_manager.add_action(action) # プロパティ経由でアクセス
        self.main_window.apply_action(action, is_undo=False)
        self.main_window.show_operation_status(f"{row_pos + 1}行目に行を追加しました。")

    def add_column(self):
        """
        現在の選択列の右に新しい列を追加
        """
        if self.main_window.is_readonly_mode(for_edit=True):
            self.main_window.show_operation_status("このモードでは列を追加できません。", is_error=True)
            return

        if self.main_window.db_backend:
            reply = QMessageBox.question(self.main_window, "確認",
                                       "データベースモードでの列追加は元に戻す(Undo)のに時間がかかる場合があります。\n続行しますか？",
                                       QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            if reply == QMessageBox.No:
                return

        col_name, ok = QInputDialog.getText(self.main_window, "新しい列の作成", "新しい列の名前を入力してください:")
        if not (ok and col_name): return # キャンセルまたは空入力の場合

        # プロパティ経由でアクセス
        if col_name in self.table_model._headers:
            self.main_window.show_operation_status(f"列名 '{col_name}' は既に存在します。", is_error=True)
            QMessageBox.warning(self.main_window, "エラー", f"列名 '{col_name}' は既に存在します。")
            return

        current_index = self.table_view.currentIndex() # プロパティ経由でアクセス
        col_pos = current_index.column() + 1 if current_index.isValid() else self.table_model.columnCount() # プロパティ経由でアクセス

        # ヘッダー変更前後の状態をUndo/Redoのために記録
        col_names_before = list(self.table_model._headers) # プロパティ経由でアクセス
        new_headers_temp = list(self.table_model._headers) # プロパティ経由でアクセス
        new_headers_temp.insert(col_pos, col_name)
        col_names_after = new_headers_temp

        action = {'type': 'add_column', 'data': {'col_pos': col_pos, 'col_name': col_name, 'col_names_before': col_names_before, 'col_names_after': col_names_after}}
        self.undo_manager.add_action(action) # プロパティ経由でアクセス
        self.main_window.apply_action(action, is_undo=False)
        self.main_window.show_operation_status(f"列 '{col_name}' を追加しました。")
        self.main_window.view_controller.recreate_card_view_fields() # カードビューも更新

    def delete_selected_rows(self):
        """
        選択された行を削除
        """
        if self.main_window.is_readonly_mode(for_edit=True):
            self.main_window.show_operation_status("このモードでは行を削除できません。", is_error=True)
            return

        # 選択されている行のインデックスを昇順で取得
        # selectionModel().selectedIndexes() はセル単位でインデックスを返すため、
        # 行番号のみを抽出し、重複を排除し、降順にソートする (削除時のインデックスずれを防ぐため)
        selected_rows = sorted(list({idx.row() for idx in self.table_view.selectionModel().selectedIndexes()}), reverse=True) # プロパティ経由でアクセス
        
        if not selected_rows:
            self.main_window.show_operation_status("削除する行を選択してください。", is_error=True)
            return

        reply = QMessageBox.question(self.main_window, "行の削除",
                                   f"{len(selected_rows)}行を削除しますか？\nこの操作は元に戻せません。", # この操作は元に戻せません
                                   QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        
        if reply == QMessageBox.No:
            return

        if self.main_window.db_backend and hasattr(self.main_window.db_backend, 'remove_rows'):
            # DBバックエンドを使用している場合はDBのremove_rowsを呼び出す
            self.main_window.db_backend.remove_rows(selected_rows)
            # モデル全体をリセットしてUIを更新
            self.table_model.beginResetModel() # プロパティ経由でアクセス
            self.table_model.endResetModel() # プロパティ経由でアクセス
        else:
            # DataFrameモードの場合は、降順に削除してインデックスずれを防ぐ
            for row in selected_rows:
                self.table_model.removeRows(row, 1) # プロパティ経由でアクセス

        self.main_window.show_operation_status(f"{len(selected_rows)}行を削除しました。")
        # 行の削除はUndoManagerに登録しない（QMessageBoxで警告済みのため）

    def delete_selected_columns(self):
        """
        選択された列を削除
        """
        # プロパティ経由でアクセス
        selected_columns = self.table_view.selectionModel().selectedColumns()
        if not selected_columns:
            self.main_window.show_operation_status("削除する列を選択してください。", is_error=True)
            return
        
        if self.main_window.is_readonly_mode(for_edit=True):
            self.main_window.show_operation_status("このモードでは列を削除できません。", is_error=True)
            return

        # 削除対象の列のインデックスと名前を取得
        # selectedColumns()はQModelIndexのリストを返す。最初の要素の列インデックスを使用
        col_idx = selected_columns[0].column()
        col_name = self.table_model.headerData(col_idx, Qt.Horizontal) # プロパティ経由でアクセス

        warning_message = f"列「{col_name}」を削除しますか？\nこの操作は元に戻せます。"
        if self.main_window.db_backend:
            warning_message += "\n\n注意: データベースモードでの列削除は元に戻す(Undo)のに時間がかかる場合があります。"

        if QMessageBox.question(self.main_window, "列の削除", warning_message, QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            # Undoのために旧データを取得
            col_data = []
            if not self.main_window.db_backend: # DBモードでない場合のみデータを取得（DBモードはデータ自体をUndoデータに含めない）
                col_data = self.table_model.get_column_data(col_idx) # プロパティ経由でアクセス

            # ヘッダー変更前後の状態をUndo/Redoのために記録
            col_names_before = list(self.table_model._headers) # プロパティ経由でアクセス
            new_headers_after_delete = [h for h in col_names_before if h != col_name]
            col_names_after = new_headers_after_delete # 削除後のヘッダーリスト

            action = {'type': 'delete_column', 'data': {'col_idx': col_idx, 'col_name': col_name, 'col_data': col_data, 'col_names_before': col_names_before, 'col_names_after': col_names_after}}
            self.undo_manager.add_action(action) # プロパティ経由でアクセス
            self.main_window.apply_action(action, False)
            self.main_window.show_operation_status(f"列「{col_name}」を削除しました。")

    def remove_duplicate_rows(self):
        """重複行を削除"""
        
        if self.main_window.is_readonly_mode(for_edit=True):
            self.main_window.show_operation_status("このモードでは重複行を削除できません。", is_error=True)
            return
        
        # Lazyモードの場合は警告して終了
        if self.main_window.lazy_loader:
            QMessageBox.warning(self.main_window, "機能制限", "遅延読み込みモードでは重複行の削除はできません。")
            self.main_window.show_operation_status("遅延読み込みモードでは重複行の削除はできません。", is_error=True)
            return

        # データが空の場合は警告して終了
        if self.table_model.rowCount() == 0:
            self.main_window.show_operation_status("データがありません。重複行を削除できません。", is_error=True)
            return
            
        # ダイアログを表示
        dialog = RemoveDuplicatesDialog(self.main_window, self.table_model._headers)
        
        if dialog.exec() != QDialog.Accepted:
            return
        
        settings = dialog.get_result()
        
        # 実際の削除処理
        if self.main_window.db_backend:
            self._remove_duplicates_in_db(settings)
        else:
            self._remove_duplicates_in_dataframe(settings)

    def _remove_duplicates_in_dataframe(self, settings):
        """DataFrameモードでの重複削除"""
        
        df = self.table_model.get_dataframe()
        if df is None or df.empty:
            self.main_window.show_operation_status("データがありません。", is_error=True)
            return
        
        original_count = len(df)
        
        # 重複削除の実行
        if settings['use_all_columns']:
            df_unique = df.drop_duplicates(keep=settings['keep'])
        else:
            if not settings['selected_columns']:
                QMessageBox.warning(self.main_window, "警告", "重複判定の基準となる列が選択されていません。")
                self.main_window.show_operation_status("重複判定の基準となる列が選択されていません。", is_error=True)
                return
            
            # 選択された列がDataFrameに存在するかチェック
            valid_columns = [col for col in settings['selected_columns'] if col in df.columns]
            if not valid_columns:
                QMessageBox.warning(self.main_window, "警告", "選択された列がデータに見つかりません。")
                self.main_window.show_operation_status("選択された列がデータに見つかりません。", is_error=True)
                return
            
            df_unique = df.drop_duplicates(subset=valid_columns, keep=settings['keep'])
        
        removed_count = original_count - len(df_unique)
        
        if removed_count == 0:
            self.main_window.show_operation_status("重複行は見つかりませんでした。", 2000)
            return
        
        # 確認ダイアログ
        reply = QMessageBox.question(
            self.main_window,
            "重複行の削除確認",
            f"{removed_count}行の重複が見つかりました。\n削除しますか？\n\n注意: この操作は元に戻せません。",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            # 削除を実行
            self.main_window._df = df_unique.reset_index(drop=True)
            self.table_model.set_dataframe(self.main_window._df)
            self.main_window.show_operation_status(f"{removed_count}行の重複を削除しました。")
            
            # Undo履歴をクリア（大量の変更のため）
            self.undo_manager.clear()
            self.main_window.update_menu_states() # Undo/Redoボタンの状態更新
            self.table_model.force_refresh() # モデルの強制更新

    def _remove_duplicates_in_db(self, settings):
        """SQLiteモードでの重複削除"""
        
        db = self.main_window.db_backend
        cursor = db.conn.cursor()
        
        try:
            self.main_window.show_operation_status("重複行を検索中...", duration=0)
            QApplication.setOverrideCursor(Qt.WaitCursor)

            table_name = db.table_name
            
            if settings['use_all_columns']:
                columns = db.header
            else:
                columns = settings['selected_columns']
            
            if not columns:
                QMessageBox.warning(self.main_window, "警告", "重複判定の基準となる列が選択されていません。")
                self.main_window.show_operation_status("重複判定の基準となる列が選択されていません。", is_error=True)
                QApplication.restoreOverrideCursor()
                return
            
            # 列名をエスケープ（f-string外で処理し、SQLインジェクション対策も兼ねる）
            escaped_columns = []
            for col in columns:
                # SQLiteの識別子エスケープは二重引用符
                escaped_col = col.replace('"', '""')
                escaped_columns.append(f'"{escaped_col}"')
            
            columns_str = ", ".join(escaped_columns)
            
            # 重複行数をカウント
            # SQLite 3.25.0 (PySide6で一般的に利用されるバージョン) 以降はROW_NUMBER() OVER()が使えるが、
            # 互換性を考慮し、GROUP BY と MIN/MAX(rowid) を使う方法で重複を特定
            
            # 影響を受ける重複グループの数を取得 (あくまでグループ数であり、実際の削除行数ではない)
            count_sql = f'''
                SELECT COUNT(*) FROM (
                    SELECT {columns_str}
                    FROM "{table_name}"
                    GROUP BY {columns_str}
                    HAVING COUNT(*) > 1
                )
            '''
            cursor.execute(count_sql)
            duplicate_groups = cursor.fetchone()[0]
            
            if duplicate_groups == 0:
                self.main_window.show_operation_status("重複行は見つかりませんでした。", 2000)
                QApplication.restoreOverrideCursor()
                return
            
            # 実際の削除行数を概算、または総行数を取得して確認ダイアログのメッセージを構築
            total_rows_before_delete = db.get_total_rows() # 削除前の総行数
            
            # 確認ダイアログ
            reply = QMessageBox.question(
                self.main_window,
                "重複行の削除確認",
                f"重複グループが{duplicate_groups}個見つかりました。\n"
                f"現在の総行数: {total_rows_before_delete}行。\n"
                f"重複行を削除しますか？\n\n"
                f"注意: この操作は元に戻せません。", # SQLiteモードではUndoManagerでUndoできないため明記
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if reply == QMessageBox.No:
                QApplication.restoreOverrideCursor()
                return
            
            self.main_window.show_operation_status("重複行を削除中...", duration=0)
            
            # 一時テーブルを作成して重複を削除する堅牢な方法
            temp_table = "temp_unique_rows_for_deduplication" # 一時テーブル名をよりユニークに
            
            # 既存の一時テーブルを削除 (念のため)
            cursor.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
            
            # 重複を除外した行を一時テーブルに保存
            # rowidは1から始まるため、rowidをそのまま使う
            if settings['keep'] == 'first':
                # 各重複グループで最小のrowidを持つ行（最初の出現）を保持
                create_temp_sql = f'''
                    CREATE TABLE "{temp_table}" AS
                    SELECT * FROM "{table_name}"
                    WHERE rowid IN (
                        SELECT MIN(rowid)
                        FROM "{table_name}"
                        GROUP BY {columns_str}
                    )
                '''
            else: # settings['keep'] == 'last'
                # 各重複グループで最大のrowidを持つ行（最後の出現）を保持
                create_temp_sql = f'''
                    CREATE TABLE "{temp_table}" AS
                    SELECT * FROM "{table_name}"
                    WHERE rowid IN (
                        SELECT MAX(rowid)
                        FROM "{table_name}"
                        GROUP BY {columns_str}
                    )
                '''
            
            cursor.execute(create_temp_sql)
            
            # 元のテーブルを削除して、一時テーブルを元の名前にリネーム
            cursor.execute(f'DROP TABLE "{table_name}"')
            cursor.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{table_name}"')
            
            # ヘッダー情報をDBバックエンドに反映
            db.header = db.get_all_column_names() # 新しいメソッドを仮定、または既存のheaderを維持
            
            # インデックスを再作成 (パフォーマンス維持のため重要)
            # db_backend._create_indexes が private だが、ここでは明示的に呼び出す
            # あるいは、db_backendにpublicなrebuild_indexesメソッドを追加することも検討
            # db_backendの_create_indexesは引数にcolumnsリストを取る
            db._create_indexes(db.header) 
            
            db.conn.commit() # トランザクションをコミット
            
            # 削除後の行数を取得
            total_rows_after_delete = db.get_total_rows()
            removed_count = total_rows_before_delete - total_rows_after_delete
            
            # モデルをリセットしてUIを更新
            self.table_model.beginResetModel()
            self.table_model.endResetModel()
            
            # キャッシュをクリア
            self.table_model._row_cache.clear()
            self.table_model._cache_queue.clear()
            
            self.main_window.show_operation_status(f"{removed_count}行の重複を削除しました。")
            self.main_window.update_menu_states() # Undo/Redoボタンの状態更新

        except Exception as e:
            db.conn.rollback() # エラー時はロールバック
            QMessageBox.critical(
                self.main_window,
                "エラー",
                f"重複削除中にエラーが発生しました:\n{str(e)}\n\n詳細:\n{traceback.format_exc()}"
            )
            self.main_window.show_operation_status("重複削除中にエラーが発生しました。", is_error=True)
        finally:
            QApplication.restoreOverrideCursor() # カーソルを元に戻す