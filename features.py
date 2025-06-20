# features.py

"""
データ処理やビジネスロジックなど、UIから独立した「機能」を担当するクラス群をまとめます。
Undo/Redo、CSVフォーマット、クリップボード、セル/列結合、親子関係分析などのManagerクラスが含まれます。
"""

import csv
import pandas as pd
from tkinter import messagebox, simpledialog

# ダイアログはui.pyで定義されているため、そこからインポートします。
# このimportは、Managerクラスが直接ダイアログを呼び出すために必要です。
from ui import MergeSeparatorDialog

#==============================================================================
# 1. 新機能管理クラス（既存）
#==============================================================================
class UndoRedoManager:
    """操作履歴を管理し、アンドゥ/リドゥ機能を提供するクラス"""
    def __init__(self, app, max_history=50):
        self.app = app
        self.history = []
        self.current_index = -1
        self.max_history = max_history

    def add_action(self, action):
        """新しいアクションを履歴に記録する"""
        if self.current_index < len(self.history) - 1:
            self.history = self.history[:self.current_index + 1]
        
        self.history.append(action)
        
        if len(self.history) > self.max_history:
            self.history.pop(0)
        else:
            self.current_index += 1
        
        self.app.update_menu_states()

    def undo(self):
        """操作を元に戻す"""
        if not self.can_undo(): return
        
        action = self.history[self.current_index]
        self.app.apply_action(action, is_undo=True)
        self.current_index -= 1
        self.app.update_menu_states()

    def redo(self):
        """元に戻した操作をやり直す"""
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
        self.app.update_menu_states()

#==============================================================================
# ★★★ 新機能：CSVフォーマット管理クラス ★★★
#==============================================================================
class CSVFormatManager:
    """CSV形式の判定と管理を行うクラス"""
    
    def __init__(self, app):
        self.app = app
        self.current_format = {
            'quoting': csv.QUOTE_MINIMAL,
            'delimiter': ',',
            'quotechar': '"',
            'detected_has_quotes': False
        }
    
    def detect_format(self, filepath, encoding='utf-8'):
        """CSVファイルのフォーマットを検出"""
        try:
            with open(filepath, 'r', encoding=encoding, newline='') as f:
                sample = f.read(4096)
                if not sample:
                    return self.current_format
            
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)
            
            has_quotes = False
            if dialect.quotechar:
                lines = sample.split('\n')[:10]
                for line in lines:
                    if dialect.quotechar in line:
                        has_quotes = True
                        break
            
            self.current_format = {
                'quoting': csv.QUOTE_ALL if has_quotes else csv.QUOTE_MINIMAL,
                'delimiter': dialect.delimiter,
                'quotechar': dialect.quotechar or '"',
                'detected_has_quotes': has_quotes,
                'dialect': dialect
            }
            
            return self.current_format
            
        except (csv.Error, UnicodeDecodeError):
            return {
                'quoting': csv.QUOTE_MINIMAL,
                'delimiter': ',',
                'quotechar': '"',
                'detected_has_quotes': False
            }

    def save_with_format(self, filepath, dataframe, quoting_style=None, encoding='utf-8'):
        """指定されたフォーマットでCSVを保存"""
        if quoting_style is None:
            quoting_style = self.current_format['quoting']
        
        try:
            dataframe.to_csv(
                filepath,
                index=False,
                encoding=encoding,
                sep=self.current_format['delimiter'],
                quotechar=self.current_format['quotechar'],
                quoting=quoting_style
            )
            return True
        except Exception as e:
            messagebox.showerror("保存エラー", f"ファイルの保存中にエラーが発生しました:\n{e}")
            return False

class ClipboardManager:
    """クリップボード操作（コピー、ペースト）を管理する静的メソッドを持つクラス"""
    @staticmethod
    def copy_cells_to_clipboard(app, cells_data):
        """セルデータをTSV形式でクリップボードにコピーする"""
        if not cells_data: return
        
        for cell in cells_data:
            cell['row_idx'] = app.tree.index(cell['item'])
            cell['col_idx'] = app.header.index(cell['column'])

        sorted_cells = sorted(cells_data, key=lambda x: (x['row_idx'], x['col_idx']))
        
        min_row = sorted_cells[0]['row_idx']
        tsv_data = []
        row_values = []

        for cell in sorted_cells:
            if cell['row_idx'] > min_row:
                tsv_data.append('\t'.join(row_values))
                row_values = []
                min_row = cell['row_idx']
            row_values.append(str(cell['value']))
        
        if row_values:
            tsv_data.append('\t'.join(row_values))
            
        clipboard_text = '\n'.join(tsv_data)
        app.clipboard_clear()
        app.clipboard_append(clipboard_text)

    @staticmethod
    def get_paste_data_from_clipboard(app, start_item, start_col_idx):
        """クリップボードからペースト用データを生成する"""
        try:
            clipboard_text = app.clipboard_get()
        except app.tk.TclError:
            return []

        lines = clipboard_text.strip().split('\n')
        start_row_idx = app.tree.index(start_item)
        
        paste_actions = []
        for r_offset, line in enumerate(lines):
            values = line.split('\t')
            for c_offset, value in enumerate(values):
                target_row_idx = start_row_idx + r_offset
                target_col_idx = start_col_idx + c_offset
                
                if target_row_idx < len(app.displayed_indices) and target_col_idx < len(app.header):
                    item_id = app.tree.get_children()[target_row_idx]
                    col_name = app.header[target_col_idx]
                    paste_actions.append({'item': item_id, 'column': col_name, 'value': value})
        
        return paste_actions

#==============================================================================
# 3. セル結合管理クラス
#==============================================================================
class CellMergeManager:
    """セル結合機能を管理するクラス"""
    
    def __init__(self, app):
        self.app = app
        
    def can_merge_cells(self, cell1_info, cell2_info):
        """2つのセルが結合可能かチェック"""
        if not cell1_info or not cell2_info:
            return False, "セルの情報が不正です"
            
        item1, col1 = cell1_info
        item2, col2 = cell2_info
        
        if item1 != item2:
            return False, "同じ行のセルのみ結合できます"
            
        try:
            col1_idx = self.app.header.index(col1)
            col2_idx = self.app.header.index(col2)
            if abs(col1_idx - col2_idx) != 1:
                return False, "隣接するセルのみ結合できます"
        except ValueError:
            return False, "列の情報が不正です"
            
        return True, ""
    
    def merge_cells_right(self, target_cell):
        """選択セルを右のセルと結合"""
        return self._merge_cells_direction(target_cell, direction="right")
    
    def merge_cells_left(self, target_cell):
        """選択セルを左のセルと結合"""
        return self._merge_cells_direction(target_cell, direction="left")
    
    def _merge_cells_direction(self, target_cell, direction="right"):
        """指定方向のセルと結合"""
        if not target_cell:
            return False, "結合するセルが選択されていません"
            
        item, col_name = target_cell
        
        try:
            col_idx = self.app.header.index(col_name)
            
            if direction == "right":
                adjacent_col_idx = col_idx + 1
                if adjacent_col_idx >= len(self.app.header):
                    return False, "右に結合できるセルがありません"
            else:
                adjacent_col_idx = col_idx - 1
                if adjacent_col_idx < 0:
                    return False, "左に結合できるセルがありません"
                    
            adjacent_col_name = self.app.header[adjacent_col_idx]
            
            can_merge, error_msg = self.can_merge_cells(
                (item, col_name), (item, adjacent_col_name)
            )
            if not can_merge:
                return False, error_msg
                
            separator = self._get_merge_separator()
            if separator is None:
                return False, "結合がキャンセルされました"
                
            return self._perform_merge(item, col_name, adjacent_col_name, separator, direction)
            
        except (ValueError, IndexError) as e:
            return False, f"結合処理中にエラーが発生しました: {e}"
    
    def _get_merge_separator(self):
        """結合時のセパレータを取得"""
        dialog = MergeSeparatorDialog(self.app, self.app.theme)
        self.app.wait_window(dialog)
        return dialog.result
    
    def _perform_merge(self, item, col1_name, col2_name, separator, direction):
        """実際のセル結合を実行"""
        try:
            original_index = int(item)
            row_data = self.app.df.loc[original_index]
            
            value1 = str(row_data.get(col1_name, ""))
            value2 = str(row_data.get(col2_name, ""))
            
            if direction == "right":
                merged_value = f"{value1}{separator}{value2}" if value1 and value2 else (value1 or value2)
                target_col, empty_col = col1_name, col2_name
            else:
                merged_value = f"{value2}{separator}{value1}" if value1 and value2 else (value1 or value2)
                target_col, empty_col = col2_name, col1_name
            
            action = {
                'type': 'merge_cells',
                'data': {
                    'item': str(original_index),
                    'target_column': target_col,
                    'empty_column': empty_col,
                    'target_old': row_data.get(target_col, ""),
                    'empty_old': row_data.get(empty_col, ""),
                    'merged_value': merged_value,
                    'direction': direction
                }
            }
            
            self.app.undo_manager.add_action(action)
            self.app.apply_action(action, is_undo=False)
            
            return True, f"セルを{direction}方向に結合しました"
            
        except Exception as e:
            return False, f"結合処理中にエラーが発生しました: {e}"

#==============================================================================
# 4. 列結合管理クラス
#==============================================================================
class ColumnMergeManager:
    """列結合機能を管理するクラス"""
    
    def __init__(self, app):
        self.app = app
        
    def can_merge_columns(self, col1_name, col2_name):
        """2つの列が結合可能かチェック"""
        if not col1_name or not col2_name:
            return False, "列の情報が不正です"
            
        if col1_name not in self.app.header or col2_name not in self.app.header:
            return False, "指定された列が存在しません"
            
        try:
            col1_idx = self.app.header.index(col1_name)
            col2_idx = self.app.header.index(col2_name)
            if abs(col1_idx - col2_idx) != 1:
                return False, "隣接する列のみ結合できます"
        except ValueError:
            return False, "列の情報が不正です"
            
        return True, ""
    
    def merge_column_right(self, target_column):
        """指定列を右の列と結合"""
        return self._merge_column_direction(target_column, direction="right")
    
    def merge_column_left(self, target_column):
        """指定列を左の列と結合"""
        return self._merge_column_direction(target_column, direction="left")
    
    def _merge_column_direction(self, target_column, direction="right"):
        """指定方向の列と結合"""
        if not target_column:
            return False, "結合する列が指定されていません"
            
        try:
            col_idx = self.app.header.index(target_column)
            
            if direction == "right":
                adjacent_col_idx = col_idx + 1
                if adjacent_col_idx >= len(self.app.header):
                    return False, "右に結合できる列がありません"
            else:
                adjacent_col_idx = col_idx - 1
                if adjacent_col_idx < 0:
                    return False, "左に結合できる列がありません"
                    
            adjacent_col_name = self.app.header[adjacent_col_idx]
            
            can_merge, error_msg = self.can_merge_columns(target_column, adjacent_col_name)
            if not can_merge:
                return False, error_msg
                
            separator = self._get_merge_separator()
            if separator is None:
                return False, "結合がキャンセルされました"
                
            return self._perform_column_merge(target_column, adjacent_col_name, separator, direction)
            
        except (ValueError, IndexError) as e:
            return False, f"列結合処理中にエラーが発生しました: {e}"
    
    def _get_merge_separator(self):
        """結合時のセパレータを取得"""
        dialog = MergeSeparatorDialog(self.app, self.app.theme, is_column_merge=True)
        self.app.wait_window(dialog)
        return dialog.result
    
    def _perform_column_merge(self, col1_name, col2_name, separator, direction):
        """実際の列結合を実行"""
        try:
            changes = []
            
            if direction == "right":
                target_col, empty_col = col1_name, col2_name
            else:
                target_col, empty_col = col2_name, col1_name
            
            for i, row in self.app.df.iterrows():
                value1 = str(row.get(col1_name, ""))
                value2 = str(row.get(col2_name, ""))
                
                if direction == "right":
                    merged_value = f"{value1}{separator}{value2}" if value1 and value2 else (value1 or value2)
                else:
                    merged_value = f"{value2}{separator}{value1}" if value1 and value2 else (value1 or value2)
                
                changes.append({
                    'row_index': i,
                    'target_column': target_col,
                    'empty_column': empty_col,
                    'target_old': row.get(target_col, ""),
                    'empty_old': row.get(empty_col, ""),
                    'merged_value': merged_value
                })
            
            action = {
                'type': 'merge_columns',
                'data': {
                    'target_column': target_col,
                    'empty_column': empty_col,
                    'changes': changes,
                    'direction': direction
                }
            }
            
            self.app.undo_manager.add_action(action)
            self.app.apply_action(action, is_undo=False)
            
            return True, f"列「{col1_name}」と「{col2_name}」を結合しました"
            
        except Exception as e:
            return False, f"列結合処理中にエラーが発生しました: {e}"

#==============================================================================
# 5. 親子関係管理クラス
#==============================================================================
class ParentChildManager:
    """列の値に基づく親子関係を管理するクラス"""
    
    def __init__(self, app):
        self.app = app
        self.parent_child_data = {}
        self.current_group_column = None
        
    def analyze_parent_child_relationships(self, column_name):
        """指定列の親子関係を分析"""
        if column_name not in self.app.header:
            return False, f"列「{column_name}」が見つかりません"
        
        self.current_group_column = column_name
        self.parent_child_data.clear()
        
        if self.app.df is None or self.app.df.empty:
            return False, "データがありません"
        
        current_group_id = 0
        current_value = None
        group_start_index = 0
        
        for i, row in self.app.df.iterrows():
            value = str(row.get(column_name, '')).strip()
            
            if current_value != value:
                current_value = value
                current_group_id += 1
                group_start_index = i
                is_parent = True
            else:
                is_parent = False
            
            self.parent_child_data[i] = {
                'group_id': current_group_id,
                'is_parent': is_parent,
                'group_value': value,
                'group_start_index': group_start_index
            }
        
        return True, f"列「{column_name}」で{current_group_id}個のグループを識別しました"
    
    def get_parent_rows(self):
        """親行のインデックスリストを取得"""
        return [i for i, data in self.parent_child_data.items() if data['is_parent']]
    
    def get_child_rows(self):
        """子行のインデックスリストを取得"""
        return [i for i, data in self.parent_child_data.items() if not data['is_parent']]
    
    def get_group_info(self, row_index):
        """指定行のグループ情報を取得"""
        return self.parent_child_data.get(row_index, None)
    
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
        for group_id, info in sorted(group_counts.items()):
            parent_count = 1
            child_count = info['count'] - 1
            summary += f"グループ{group_id}: 「{info['value']}」\n"
            summary += f"  親: 1行, 子: {child_count}行, 合計: {info['count']}行\n\n"
        
        total_parents = len(self.get_parent_rows())
        total_children = len(self.get_child_rows())
        summary += f"全体: 親 {total_parents}行, 子 {total_children}行"
        
        return summary
    
    def filter_rows_by_type(self, target_rows, parent_child_mode):
        """親子タイプに基づいて行をフィルタリング"""
        if parent_child_mode == "all":
            return target_rows
        elif parent_child_mode == "parent":
            parent_indices = set(self.get_parent_rows())
            return [row for row in target_rows if int(row.get('item', -1)) in parent_indices]
        elif parent_child_mode == "child":
            child_indices = set(self.get_child_rows())
            return [row for row in target_rows if int(row.get('item', -1)) in child_indices]
        else:
            return target_rows