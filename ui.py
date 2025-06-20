# ui.py

"""
GUIの見た目やインタラクションを担当するクラス群をまとめます。
各種ダイアログ、セル選択やナビゲーションのためのMixinクラス、インラインエディタなどが含まれます。
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, font, simpledialog
import csv
import os
import re
import pandas as pd

#==============================================================================
# 6. セパレータ選択ダイアログ（サイズ拡大版）
#==============================================================================
class MergeSeparatorDialog(tk.Toplevel):
    """セル結合時のセパレータを選択するダイアログ"""
    
    def __init__(self, parent, theme, is_column_merge=False):
        super().__init__(parent)
        self.parent = parent
        self.theme = theme
        self.is_column_merge = is_column_merge
        self.result = None
        
        merge_type = "列" if is_column_merge else "セル"
        self.title(f"{merge_type}結合設定")
        self.geometry("520x360")
        self.resizable(False, False)
        
        self.transient(parent)
        self.grab_set()
        
        self._create_widgets()
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        
        self.focus_set()
        self.bind("<Return>", self._on_ok)
        self.bind("<Escape>", self._on_cancel)
    
    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        merge_type = "列" if self.is_column_merge else "セル"
        title_label = ttk.Label(main_frame, text=f"{merge_type}結合の設定", 
                               font=("", 14, "bold"))
        title_label.pack(pady=(0, 20))
        
        desc_text = "隣接する列の全行を結合します" if self.is_column_merge else "選択されたセルを隣接するセルと結合します"
        desc_label = ttk.Label(main_frame, text=desc_text, 
                              font=("", 10), foreground="gray")
        desc_label.pack(pady=(0, 15))
        
        separator_frame = ttk.LabelFrame(main_frame, text="結合時のセパレータ", padding=15)
        separator_frame.pack(fill=tk.X, pady=(0, 20))
        
        self.separator_var = tk.StringVar(value="space")
        
        separators = [
            ("スペース", "space", " "),
            ("なし（直接結合）", "none", ""),
            ("カンマ + スペース", "comma", ", "),
            ("ハイフン", "hyphen", " - "),
            ("カスタム", "custom", "")
        ]
        
        for text, value, preview in separators:
            rb = ttk.Radiobutton(separator_frame, text=f"{text}", 
                               variable=self.separator_var, value=value)
            rb.pack(anchor=tk.W, pady=4)
        
        custom_frame = ttk.Frame(separator_frame)
        custom_frame.pack(fill=tk.X, pady=(10, 0))
        
        ttk.Label(custom_frame, text="カスタム:").pack(side=tk.LEFT)
        self.custom_entry = ttk.Entry(custom_frame, width=20)
        self.custom_entry.pack(side=tk.LEFT, padx=(10, 0))
        
        preview_frame = ttk.LabelFrame(main_frame, text="結合プレビュー", padding=15)
        preview_frame.pack(fill=tk.X, pady=(0, 20))
        
        self.preview_var = tk.StringVar()
        preview_label = ttk.Label(preview_frame, textvariable=self.preview_var,
                                 background="white", relief=tk.SUNKEN, padding=10,
                                 font=("", 11))
        preview_label.pack(fill=tk.X)
        
        self.separator_var.trace_add("write", self._update_preview)
        self.custom_entry.bind("<KeyRelease>", self._update_preview)
        self._update_preview()
        
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0))
        
        cancel_btn = ttk.Button(button_frame, text="キャンセル", 
                               command=self._on_cancel, width=12)
        cancel_btn.pack(side=tk.RIGHT, padx=(10, 0))
        
        ok_btn = ttk.Button(button_frame, text="OK", 
                           command=self._on_ok, width=12)
        ok_btn.pack(side=tk.RIGHT)
    
    def _update_preview(self, *args):
        """プレビューを更新"""
        separator = self._get_current_separator()
        if self.is_column_merge:
            sample_text = f"列A{separator}列B"
        else:
            sample_text = f"セル1{separator}セル2"
        self.preview_var.set(sample_text)
    
    def _get_current_separator(self):
        """現在選択されているセパレータを取得"""
        sep_type = self.separator_var.get()
        
        if sep_type == "space":
            return " "
        elif sep_type == "none":
            return ""
        elif sep_type == "comma":
            return ", "
        elif sep_type == "hyphen":
            return " - "
        elif sep_type == "custom":
            return self.custom_entry.get()
        else:
            return " "
    
    def _on_ok(self, event=None):
        """OKボタンの処理"""
        self.result = self._get_current_separator()
        self.destroy()
    
    def _on_cancel(self, event=None):
        """キャンセルボタンの処理"""
        self.result = None
        self.destroy()

class PriceCalculatorDialog(tk.Toplevel):
    """金額計算ツールの設定を行うダイアログ"""

    def __init__(self, parent, headers, theme):
        super().__init__(parent)
        self.parent = parent
        self.theme = theme
        self.result = None

        self.title("金額計算ツール")
        self.geometry("450x340")
        self.resizable(False, False)

        self.transient(parent)
        self.grab_set()

        self.column_var = tk.StringVar()
        self.tax_status_var = tk.StringVar(value="exclusive")
        self.discount_var = tk.DoubleVar(value=0.0)

        self._create_widgets(headers)
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)

        self.focus_set()
        self.bind("<Return>", self._on_ok)
        self.bind("<Escape>", self._on_cancel)

    def _create_widgets(self, headers):
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)

        col_frame = ttk.LabelFrame(main_frame, text="1. 計算対象の列", padding=10)
        col_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(col_frame, text="列を選択:").pack(side=tk.LEFT, padx=(0, 10))
        col_combo = ttk.Combobox(col_frame, textvariable=self.column_var, values=headers, state="readonly")
        col_combo.pack(fill=tk.X, expand=True)
        if headers:
            col_combo.set(headers[0])

        tax_frame = ttk.LabelFrame(main_frame, text="2. 元の金額の消費税の状態", padding=10)
        tax_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Radiobutton(tax_frame, text="消費税 別 （税抜価格）", variable=self.tax_status_var, value="exclusive").pack(anchor=tk.W)
        ttk.Radiobutton(tax_frame, text="消費税 込み （税込価格）", variable=self.tax_status_var, value="inclusive").pack(anchor=tk.W)
        
        discount_frame = ttk.LabelFrame(main_frame, text="3. 割引率", padding=10)
        discount_frame.pack(fill=tk.X, pady=(0, 20))

        ttk.Label(discount_frame, text="割引率 (%):").pack(side=tk.LEFT, padx=(0, 10))
        discount_entry = ttk.Entry(discount_frame, textvariable=self.discount_var, width=10)
        discount_entry.pack(side=tk.LEFT)
        
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X)

        ttk.Button(button_frame, text="キャンセル", command=self._on_cancel).pack(side=tk.RIGHT, padx=(10, 0))
        ttk.Button(button_frame, text="計算実行", command=self._on_ok).pack(side=tk.RIGHT)
        
    def _on_ok(self, event=None):
        target_column = self.column_var.get()
        if not target_column:
            messagebox.showwarning("入力エラー", "対象の列を選択してください。", parent=self)
            return
            
        try:
            discount_rate = self.discount_var.get()
            if not (0 <= discount_rate <= 100):
                raise ValueError
        except (tk.TclError, ValueError):
            messagebox.showwarning("入力エラー", "割引率には0から100までの数値を入力してください。", parent=self)
            return

        self.result = {
            'column': target_column,
            'tax_status': self.tax_status_var.get(),
            'discount': discount_rate
        }
        self.destroy()

    def _on_cancel(self, event=None):
        self.result = None
        self.destroy()

class ReplaceFromFileDialog(tk.Toplevel):
    """ファイル参照置換のための設定を行うダイアログ"""

    def __init__(self, parent, headers, theme):
        super().__init__(parent)
        self.parent = parent
        self.theme = theme
        self.result = None
        self.lookup_df_headers = []

        self.title("ファイルを参照して置換")
        self.geometry("550x450")
        self.resizable(False, False)

        self.transient(parent)
        self.grab_set()

        self.target_col_var = tk.StringVar()
        self.lookup_filepath_var = tk.StringVar()
        self.lookup_key_col_var = tk.StringVar()
        self.replace_val_col_var = tk.StringVar()

        self._create_widgets(headers)
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.focus_set()

    def _create_widgets(self, headers):
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        main_frame.grid_columnconfigure(1, weight=1)

        frame1 = ttk.LabelFrame(main_frame, text="1. 置換対象（現在開いているファイル）", padding=10)
        frame1.pack(fill=tk.X, pady=5)
        ttk.Label(frame1, text="対象項目:").pack(side=tk.LEFT, padx=5)
        self.target_col_combo = ttk.Combobox(frame1, textvariable=self.target_col_var, values=headers, state="readonly")
        self.target_col_combo.pack(fill=tk.X, expand=True, padx=5)
        if headers:
            self.target_col_combo.set(headers[0])

        frame2 = ttk.LabelFrame(main_frame, text="2. 参照ファイル（マスターデータ）", padding=10)
        frame2.pack(fill=tk.X, pady=5)
        ttk.Label(frame2, text="ファイル名:").pack(side=tk.LEFT, padx=5)
        ttk.Entry(frame2, textvariable=self.lookup_filepath_var, state="readonly").pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(frame2, text="参照...", command=self._browse_lookup_file).pack(side=tk.LEFT, padx=5)

        frame3 = ttk.LabelFrame(main_frame, text="3. 検索キーの項目", padding=10)
        frame3.pack(fill=tk.X, pady=5)
        ttk.Label(frame3, text="検索語の項目:").pack(side=tk.LEFT, padx=5)
        self.lookup_key_combo = ttk.Combobox(frame3, textvariable=self.lookup_key_col_var, values=[], state="disabled")
        self.lookup_key_combo.pack(fill=tk.X, expand=True, padx=5)

        frame4 = ttk.LabelFrame(main_frame, text="4. 置換後の値の項目", padding=10)
        frame4.pack(fill=tk.X, pady=5)
        ttk.Label(frame4, text="置換語の項目:").pack(side=tk.LEFT, padx=5)
        self.replace_val_combo = ttk.Combobox(frame4, textvariable=self.replace_val_col_var, values=[], state="disabled")
        self.replace_val_combo.pack(fill=tk.X, expand=True, padx=5)

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=20)
        ttk.Button(button_frame, text="キャンセル", command=self._on_cancel).pack(side=tk.RIGHT, padx=(10, 0))
        self.ok_button = ttk.Button(button_frame, text="置換実行", command=self._on_ok, state="disabled")
        self.ok_button.pack(side=tk.RIGHT)

    def _browse_lookup_file(self):
        filepath = filedialog.askopenfilename(
            title="参照ファイルを開く",
            filetypes=[("CSVファイル", "*.csv"), ("テキストファイル", "*.txt"), ("すべてのファイル", "*.*")]
        )
        if not filepath:
            return

        try:
            lookup_df = pd.read_csv(filepath, encoding=self.parent.encoding, nrows=0, dtype=str)
            self.lookup_df_headers = lookup_df.columns.tolist()
            
            self.lookup_filepath_var.set(filepath)
            
            self.lookup_key_combo.config(values=self.lookup_df_headers, state="readonly")
            self.replace_val_combo.config(values=self.lookup_df_headers, state="readonly")
            if self.lookup_df_headers:
                self.lookup_key_combo.set(self.lookup_df_headers[0])
                self.replace_val_combo.set(self.lookup_df_headers[0])
            
            self.ok_button.config(state="normal")

        except Exception as e:
            messagebox.showerror("ファイル読み込みエラー", f"参照ファイルの読み込みに失敗しました。\n{e}", parent=self)
            self.lookup_filepath_var.set("")
            self.lookup_key_combo.config(values=[], state="disabled")
            self.replace_val_combo.config(values=[], state="disabled")
            self.ok_button.config(state="disabled")

    def _on_ok(self):
        if not all([self.target_col_var.get(), self.lookup_filepath_var.get(), 
                    self.lookup_key_col_var.get(), self.replace_val_col_var.get()]):
            messagebox.showwarning("設定不足", "すべての項目を設定してください。", parent=self)
            return

        self.result = {
            'target_col': self.target_col_var.get(),
            'lookup_filepath': self.lookup_filepath_var.get(),
            'lookup_key_col': self.lookup_key_col_var.get(),
            'replace_val_col': self.replace_val_col_var.get()
        }
        self.destroy()

    def _on_cancel(self):
        self.result = None
        self.destroy()

#==============================================================================
# 7. 機能別 Mixin クラス群
#==============================================================================
class CellSelectionMixin:
    """セル選択機能を提供するMixinクラス"""
    def _init_cell_selection(self):
        self.selected_cells = set()
        self.last_selected_cell = None
        self.selected_column = None
        self.dragging = False
        self.drag_start_cell = None
        
        self.tree.bind("<Button-1>", self._on_header_or_cell_click) 
        self.tree.bind("<Control-Button-1>", self._on_cell_ctrl_click)
        self.tree.bind("<Shift-Button-1>", self._on_cell_shift_click)
        self.tree.bind("<B1-Motion>", self._on_cell_drag)
        self.tree.bind("<ButtonRelease-1>", self._on_cell_release)
        
        self.tree.tag_configure('selected_cell_oddrow', background=self.theme.CELL_SELECT_START)
        self.tree.tag_configure('selected_cell_evenrow', background=self.theme.CELL_SELECT_START)
        
        self.tree.bind("<Button-3>", self._on_right_click)
        self.tree.bind("<Double-Button-1>", self._on_header_double_click)

    def _on_right_click(self, event):
        """右クリックイベントの統合ハンドラ"""
        region = self.tree.identify_region(event.x, event.y)
        
        if region == "heading":
            column_id = self.tree.identify_column(event.x)
            col_name = self.tree.heading(column_id, "text").strip("🔷 ")
            self._show_context_menu(event, context='header', column_id=column_id, col_name=col_name)
        elif region == "cell":
            item_id = self.tree.identify_row(event.y)
            if item_id:
                col_id = self.tree.identify_column(event.x)
                col_name = self.header[int(col_id.replace('#', '')) - 1]
                if (item_id, col_name) not in self.selected_cells:
                    self._clear_cell_selection()
                    self._on_cell_click(event)
                self._show_context_menu(event, context='cell', item_id=item_id)
        else:
            self._show_context_menu(event, context='general')

    def _show_context_menu(self, event, context, **kwargs):
        """状況に応じたコンテキストメニューを表示する"""
        self.context_menu = tk.Menu(self, tearoff=0)

        can_paste = False
        try:
            self.clipboard_get()
            can_paste = True
        except tk.TclError:
            pass

        state_selected = tk.NORMAL if self.selected_cells else tk.DISABLED
        state_paste = tk.NORMAL if can_paste and self.active_cell else tk.DISABLED

        self.context_menu.add_command(label="✂️ 切り取り", accelerator="Ctrl+X", command=self._cut, state=state_selected)
        self.context_menu.add_command(label="📋 コピー", accelerator="Ctrl+C", command=self._copy, state=state_selected)
        self.context_menu.add_command(label="📎 貼り付け", accelerator="Ctrl+V", command=self._paste, state=state_paste)
        self.context_menu.add_separator()

        if context == 'header':
            col_name = kwargs.get('col_name', '')
            column_id = kwargs.get('column_id', '')
            self.context_menu.add_command(label=f"🔼 昇順ソート", command=lambda: self._sort_column_asc(column_id, col_name))
            self.context_menu.add_command(label=f"🔽 降順ソート", command=lambda: self._sort_column_desc(column_id, col_name))
            self.context_menu.add_command(label="🔄 ソートをクリア", command=self._clear_sort)
            self.context_menu.add_separator()
            
            # ★★★ 機能修復箇所 ★★★
            # 欠損していた列のコピー・貼り付け機能をメニューに追加
            self.context_menu.add_command(label="B 列をコピー", command=self.copy_selected_column)
            paste_state = tk.NORMAL if self.column_clipboard is not None else tk.DISABLED
            self.context_menu.add_command(label="B 列に貼り付け", command=self.paste_to_selected_column, state=paste_state)
            self.context_menu.add_separator()
            # ★★★ 修復完了 ★★★
            
            self.context_menu.add_command(label=f"❌ 列を削除...", command=lambda: self._delete_column_with_confirmation(col_name), foreground="red")

        elif context == 'cell':
            merge_menu = tk.Menu(self.context_menu, tearoff=0)
            self.context_menu.add_cascade(label="🔗 セル結合", menu=merge_menu)
            merge_menu.add_command(label="右と結合 →", command=self._merge_right)
            merge_menu.add_command(label="← 左と結合", command=self._merge_left)
            self.context_menu.add_separator()
            self.context_menu.add_command(label="🗑️ 選択行を削除", command=self.delete_selected_rows, foreground="red")
        
        self._add_dynamic_menu_items()

        try:
            self.context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.context_menu.grab_release()

    def _add_dynamic_menu_items(self):
        """選択状態に応じた動的メニュー項目"""
        if len(self.selected_cells) > 1:
             pass
        
        if self._detect_pattern():
            if self.context_menu.index('end') is not None:
                self.context_menu.add_separator()
            self.context_menu.add_command(label="✨ パターンで自動入力...", command=self._smart_fill)

    def _detect_pattern(self):
        """スマートフィルが可能かどうかのパターンを検出（ダミー実装）"""
        return False
        
    def _smart_fill(self):
        """スマートフィルを実行（ダミー実装）"""
        messagebox.showinfo("未実装", "この機能は現在開発中です。")

    def _on_header_or_cell_click(self, event):
        """ヘッダーまたはセルのクリックを処理"""
        region = self.tree.identify_region(event.x, event.y)
        
        if region == "heading":
            column_id = self.tree.identify_column(event.x)
            if column_id:
                try:
                    col_index = int(column_id.replace('#', '')) - 1
                    if 0 <= col_index < len(self.header):
                        col_name = self.header[col_index]
                        self._select_column(col_name)
                        return
                except (ValueError, IndexError):
                    pass
        else:
            self._on_cell_click(event)
    
    def _on_header_double_click(self, event):
        """列ヘッダーのダブルクリックでソート実行"""
        region = self.tree.identify_region(event.x, event.y)
        
        if region == "heading":
            column_id = self.tree.identify_column(event.x)
            if column_id:
                try:
                    col_index = int(column_id.replace('#', '')) - 1
                    if 0 <= col_index < len(self.header):
                        col_name = self.header[col_index]
                        self.sort_by_column(col_id)
                        return
                except (ValueError, IndexError):
                    pass
    
    def _sort_column_asc(self, column_id, col_name):
        """列を昇順でソート"""
        self.sort_column = col_name
        self.sort_reverse = False
        self.filter_data()
        self.show_operation_status(f"列「{col_name}」で昇順ソートしました")
    
    def _sort_column_desc(self, column_id, col_name):
        """列を降順でソート"""
        self.sort_column = col_name
        self.sort_reverse = True
        self.filter_data()
        self.show_operation_status(f"列「{col_name}」で降順ソートしました")
    
    def _clear_sort(self):
        """ソートをクリア"""
        self.sort_column = None
        self.sort_reverse = False
        self.filter_data()
        self.show_operation_status("ソートをクリアしました")
    
    def _delete_column_with_confirmation(self, col_name):
        """確認ダイアログ付きで列を削除"""
        if len(self.header) <= 1:
            messagebox.showwarning("削除不可", "最後の列は削除できません。\n少なくとも1つの列が必要です。")
            return
        
        has_data = not self.df[col_name].isnull().all()
        
        warning_msg = f"列「{col_name}」を削除しますか？"
        if has_data:
            warning_msg += f"\n\nこの列にはデータが含まれています。\n削除すると{len(self.df)}行のデータが失われます。"
        warning_msg += "\n\n※この操作は「元に戻す」で復元可能です。"
        
        if messagebox.askyesno("列の削除", warning_msg, icon='warning'):
            self._delete_column(col_name)
    
    def _delete_selected_column(self):
        """選択中の列を削除"""
        if not self.selected_column:
            messagebox.showinfo("情報", "削除する列を選択してください。\n（列ヘッダーをクリックして列を選択）")
            return
        
        self._delete_column_with_confirmation(self.selected_column)
    
    def _delete_column(self, col_name):
        """列を削除（アンドゥ対応）"""
        if col_name not in self.header:
            messagebox.showerror("エラー", f"列「{col_name}」が見つかりません。")
            return
        
        if len(self.header) <= 1:
            messagebox.showwarning("削除不可", "最後の列は削除できません。")
            return
        
        try:
            col_position = self.header.index(col_name)
            deleted_series = self.df[col_name].copy()
            
            action = {
                'type': 'delete_column',
                'data': {
                    'column_name': col_name,
                    'position': col_position,
                    'deleted_data': deleted_series
                }
            }
            
            self.undo_manager.add_action(action)
            self.apply_action(action, is_undo=False)
            
            self.show_operation_status(f"列「{col_name}」を削除しました")
            
        except Exception as e:
            messagebox.showerror("削除エラー", f"列の削除中にエラーが発生しました：\n{e}")
    
    def _select_column(self, col_name):
        """列全体を選択（セル選択状態はクリアし、列選択のみ設定）"""
        self._clear_cell_selection_only()
        
        self.selected_column = col_name
        
        self._highlight_column_header(col_name)
        
        self._update_action_button_states()
        self._update_status_bar()
        self.show_context_hint('column_selected')

    def _clear_cell_selection_only(self):
        """セル選択のみをクリア（列選択はそのまま）"""
        self._update_active_highlight(None)
        items_to_refresh = {item for item, col in self.selected_cells}
        self.selected_cells.clear()
        self.last_selected_cell = None
        for item in items_to_refresh:
            if self.tree.exists(item):
                self._refresh_row_display(item)
        self.show_context_hint(None)

    def _highlight_column_header(self, col_name):
        """列ヘッダーを視覚的にハイライト"""
        try:
            col_idx = self.header.index(col_name)
            col_id = f"#{col_idx + 1}"
            
            for idx, header in enumerate(self.header):
                header_col_id = f"#{idx + 1}"
                self.tree.heading(header_col_id, text=header)
            
            self.tree.heading(col_id, text=f"🔷 {col_name} 🔷")
            
        except (ValueError, IndexError):
            pass
    
    def _clear_column_selection(self):
        """列選択をクリア"""
        if self.selected_column:
            try:
                col_idx = self.header.index(self.selected_column)
                col_id = f"#{col_idx + 1}"
                self.tree.heading(col_id, text=self.selected_column)
            except (ValueError, IndexError):
                pass
            
            self.selected_column = None

    def _get_cell_at_position(self, event):
        if self.tree.identify_region(event.x, event.y) != "cell": return None, None, None
        column_id = self.tree.identify_column(event.x)
        item_id = self.tree.identify_row(event.y)
        if column_id and item_id:
            try:
                col_index = int(column_id.replace('#', '')) - 1
                if 0 <= col_index < len(self.header):
                    return item_id, column_id, self.header[col_index]
            except (ValueError, IndexError):
                return None, None, None
        return None, None, None

    def _on_cell_click(self, event):
        if hasattr(self, 'cell_editor') and self.cell_editor.edit_entry: return
        self.dragging = True
        
        self._clear_column_selection()
        
        item, col_id, col_name = self._get_cell_at_position(event)
        if item and col_name:
            if not (event.state & 0x0001) and not (event.state & 0x0004): # Shift or Ctrl
                self._clear_cell_selection_only()
            self._select_cell(item, col_id, col_name)
            self.last_selected_cell = (item, col_name)
            self.drag_start_cell = (item, col_name)
            
            try:
                row_idx_in_view = self.tree.index(item)
                df_index = self.displayed_indices[row_idx_in_view]
                col_idx = self.header.index(col_name)
                self.active_cell = (df_index, col_idx)
            except (ValueError, IndexError):
                pass

    def _on_cell_ctrl_click(self, event):
        item, col_id, col_name = self._get_cell_at_position(event)
        if item and col_name:
            if (item, col_name) in self.selected_cells: self._deselect_cell(item, col_id, col_name)
            else: self._select_cell(item, col_id, col_name)
            self.last_selected_cell = (item, col_name)

    def _on_cell_shift_click(self, event):
        item, col_id, col_name = self._get_cell_at_position(event)
        if item and col_name and self.last_selected_cell:
            self._clear_cell_selection()
            self._select_cell_range(self.last_selected_cell, (item, col_name))

    def _on_cell_drag(self, event):
        if self.dragging:
            item, col_id, col_name = self._get_cell_at_position(event)
            if item and col_name and self.drag_start_cell:
                self._clear_cell_selection()
                self._select_cell_range(self.drag_start_cell, (item, col_name))

    def _on_cell_release(self, event):
        self.dragging = False

    def _select_cell(self, item, col_id, col_name):
        self._update_active_highlight(item)
        if (item, col_name) not in self.selected_cells:
            self.selected_cells.add((item, col_name))
            self._update_cell_visual(item, col_id, selected=True)
        self._update_action_button_states()
        self._update_status_bar()
        self.show_context_hint('cell_selected')

    def _deselect_cell(self, item, col_id, col_name):
        if (item, col_name) in self.selected_cells:
            self.selected_cells.discard((item, col_name))
            self._update_cell_visual(item, col_id, selected=False)
        self._update_action_button_states()
        self._update_status_bar()

    def _clear_cell_selection(self):
        self._update_active_highlight(None)
        items_to_refresh = {item for item, col in self.selected_cells}
        self.selected_cells.clear()
        self._clear_column_selection()
        for item in items_to_refresh:
            if self.tree.exists(item):
                self._refresh_row_display(item)
        self.show_context_hint(None)

    def _update_active_highlight(self, new_item):
        if new_item and self.tree.exists(new_item):
            self.tree.selection_set(new_item)
        else:
            if self.tree.selection(): self.tree.selection_remove(self.tree.selection())

    def _select_cell_range(self, start_cell, end_cell):
        start_item, start_col = start_cell; end_item, end_col = end_cell
        all_items = self.tree.get_children('')
        try:
            start_idx, end_idx = all_items.index(start_item), all_items.index(end_item)
            start_col_idx, end_col_idx = self.header.index(start_col), self.header.index(end_col)
        except (ValueError, IndexError): return
        row_start, row_end = min(start_idx, end_idx), max(start_idx, end_idx)
        col_start, col_end = min(start_col_idx, end_col_idx), max(start_col_idx, end_col_idx)
        for r_idx in range(row_start, row_end + 1):
            item = all_items[r_idx]
            for c_idx in range(col_start, col_end + 1):
                col_id = f"#{c_idx + 1}"; col_name = self.header[c_idx]
                self._select_cell(item, col_id, col_name)

    def _update_cell_visual(self, item, col_id, selected):
        self._refresh_row_display(item)
    
    def _refresh_row_display(self, item):
        if not self.tree.exists(item): return
        row_data = self._get_row_data_by_iid(item)
        if row_data is None: return
        
        values = []
        for col_name in self.header:
            value = str(row_data.get(col_name, ''))
            if (item, col_name) in self.selected_cells:
                value = f"▶ {value}"
            values.append(value)
        
        self.tree.item(item, values=values)
        
        current_tags = list(self.tree.item(item, 'tags'))
        current_tags = [tag for tag in current_tags if not tag.startswith('selected_')]
        
        has_selected_cells = any((item, col) in self.selected_cells for col in self.header)
        if has_selected_cells:
            if 'oddrow' in current_tags: current_tags.append('selected_cell_oddrow')
            else: current_tags.append('selected_cell_evenrow')
        
        self.tree.item(item, tags=current_tags)
    
    def _refresh_all_rows(self):
        for item in self.tree.get_children(''): self._refresh_row_display(item)
    
    def get_selected_cell_data(self):
        return [{'item': iid, 'column': col, 'value': self._get_row_data_by_iid(iid).get(col, '')}
                for iid, col in sorted(list(self.selected_cells), key=lambda x: (int(x[0]), self.header.index(x[1]))) if self.tree.exists(x[0])]
    
    def _get_row_data_by_iid(self, iid):
        try:
            return self.df.loc[int(iid)]
        except (ValueError, IndexError, TypeError, KeyError):
            return None

class KeyboardNavigationMixin:
    """キーボードでのナビゲーション機能を提供"""
    def _init_keyboard_navigation(self):
        self.tree.bind("<Up>", self._on_arrow_up)
        self.tree.bind("<Down>", self._on_arrow_down)
        self.tree.bind("<Left>", self._on_arrow_left)
        self.tree.bind("<Right>", self._on_arrow_right)
        self.tree.bind("<Return>", self._on_enter)
        self.tree.bind("<F2>", self._on_f2)
        self.active_cell = None

    def _move_active_cell(self, row_offset, col_offset):
        if not self.active_cell:
            self._set_active_cell_by_view_index(0, 0)
            return "break"
        
        current_df_index, current_col_index = self.active_cell
        try:
            current_view_index = self.displayed_indices.index(current_df_index)
        except (ValueError, AttributeError):
            current_view_index = 0
        
        new_view_index = max(0, min(current_view_index + row_offset, len(self.displayed_indices) - 1))
        new_col_index = max(0, min(current_col_index + col_offset, len(self.header) - 1))
        
        self._set_active_cell_by_view_index(new_view_index, new_col_index)
        return "break"

    def _on_arrow_up(self, event): return self._move_active_cell(-1, 0)
    def _on_arrow_down(self, event): return self._move_active_cell(1, 0)
    def _on_arrow_left(self, event): return self._move_active_cell(0, -1)
    def _on_arrow_right(self, event): return self._move_active_cell(0, 1)

    def _set_active_cell_by_view_index(self, view_index, col_index):
        if not self.header or not self.displayed_indices: return
        
        try:
            df_index = self.displayed_indices[view_index]
            item_id = str(df_index)
        except IndexError:
            return
            
        if not self.tree.exists(item_id):
            self.populate_list_view(start_index=view_index)
        
        self.active_cell = (df_index, col_index)

        self._clear_cell_selection()
        col_id = f"#{col_index + 1}"
        col_name = self.header[col_index]
        self._select_cell(item_id, col_id, col_name)
        self.last_selected_cell = (item_id, col_name)
        self.tree.see(item_id)
    
    def _start_cell_edit(self):
        if self.active_cell and hasattr(self, 'cell_editor'):
            df_index, col_index = self.active_cell
            item_id = str(df_index)
            col_id = f"#{col_index + 1}"
            if self.tree.exists(item_id):
                bbox = self.tree.bbox(item_id, col_id)
                if bbox:
                    x, y = bbox[0] + bbox[2]//2, bbox[1] + bbox[3]//2
                    self.tree.event_generate("<Double-1>", x=x, y=y)
        return "break"

    def _on_enter(self, event): return self._start_cell_edit()
    def _on_f2(self, event): return self._start_cell_edit()

class CardViewNavigationMixin:
    """カードビューでのナビゲーション機能"""
    def _init_card_navigation(self):
        self.card_nav_bindings = {
            "<Left>": self._card_previous, "<Right>": self._card_next,
            "<Prior>": lambda e: self._card_jump(-10), "<Next>": lambda e: self._card_jump(10),
            "<Home>": self._card_first, "<End>": self._card_last,
            "<Up>": self._card_field_up, "<Down>": self._card_field_down,
            "<Tab>": self._card_field_next, "<Shift-Tab>": self._card_field_previous
        }

    def _bind_card_navigation(self):
        for key, handler in self.card_nav_bindings.items():
            self.bind_all(key, handler)

    def _unbind_card_navigation(self):
        for key in self.card_nav_bindings:
            self.unbind_all(key)

    def _card_navigate(self, new_display_index):
        if not hasattr(self, 'card_current_original_index') or self.card_current_original_index is None: return "break"
        
        new_display_index = max(0, min(new_display_index, len(self.displayed_indices) - 1))
        
        current_display_index = self._get_current_card_display_index()
        
        if new_display_index != current_display_index:
            self._populate_card_view(new_display_index)
            self._update_card_navigation_status()
        return "break"
    
    def _get_current_card_display_index(self):
        try:
            return self.displayed_indices.index(self.card_current_original_index)
        except (ValueError, AttributeError):
            return -1

    def _card_previous(self, event=None): return self._card_navigate(self._get_current_card_display_index() - 1)
    def _card_next(self, event=None): return self._card_navigate(self._get_current_card_display_index() + 1)
    def _card_jump(self, offset): return self._card_navigate(self._get_current_card_display_index() + offset)
    def _card_first(self, event=None): return self._card_navigate(0)
    def _card_last(self, event=None): return self._card_navigate(len(self.displayed_indices) - 1)

    def _card_field_nav(self, event, offset):
        focused = self.focus_get()
        entries = list(self.card_entries.values())
        if focused in entries:
            try:
                current_idx = entries.index(focused)
                next_idx = (current_idx + offset) % len(entries)
                entries[next_idx].focus_set()
            except ValueError:
                if entries: entries[0].focus_set()
        return "break"

    def _card_field_up(self, event=None): return self._card_field_nav(event, -1)
    def _card_field_down(self, event=None): return self._card_field_nav(event, 1)
    def _card_field_next(self, event=None): return self._card_field_nav(event, 1)
    def _card_field_previous(self, event=None): return self._card_field_nav(event, -1)

    def _update_card_navigation_status(self):
        if hasattr(self, 'card_current_original_index') and self.card_current_original_index is not None:
            if hasattr(self, 'show_context_hint'):
                self.show_context_hint(f"レコード {self._get_current_card_display_index() + 1} / {len(self.displayed_indices)}")

#==============================================================================
# 8. インラインセル編集機能（Undo対応）
#==============================================================================
class InlineCellEditor:
    def __init__(self, app_instance):
        self.app = app_instance; self.tree = app_instance.tree
        self.edit_entry = None; self.editing_cell = None
        self.tree.bind("<Double-1>", self.start_edit)
    
    def start_edit(self, event):
        if self.edit_entry: self.finish_edit()
        item, col_id, col_name = self.app._get_cell_at_position(event)
        if not item or not col_id: return
        
        if hasattr(self.app, 'show_context_hint'):
            self.app.show_context_hint('editing')

        bbox = self.tree.bbox(item, col_id)
        if not bbox: return
        x, y, w, h = bbox
        
        values = self.app.tree.item(item, 'values')
        col_idx = int(col_id.replace('#','')) - 1
        original_value_with_marker = values[col_idx]
        original_value = original_value_with_marker[2:] if original_value_with_marker.startswith("▶ ") else original_value_with_marker
        
        self.editing_cell = (item, col_name, original_value)
        entry_font = font.nametofont("TkTextFont")
        self.edit_entry = ttk.Entry(self.tree, font=entry_font)
        self.edit_entry.place(x=x, y=y, width=w, height=h)
        self.edit_entry.insert(0, original_value)
        self.edit_entry.focus(); self.edit_entry.select_range(0, 'end')
        self.edit_entry.bind("<Return>", self.finish_edit)
        self.edit_entry.bind("<Escape>", self.cancel_edit)
        self.edit_entry.bind("<FocusOut>", self.finish_edit)

    def finish_edit(self, event=None):
        if not self.edit_entry: return "break"
        new_value = self.edit_entry.get()
        item, column_name, original_value = self.editing_cell
        
        self.cleanup_edit()
        
        if new_value != original_value:
            if '価格' in column_name and new_value:
                try: float(new_value)
                except ValueError: messagebox.showwarning("入力エラー", f"「{column_name}」には数値を入力してください。"); self.app._refresh_row_display(item); return "break"
            
            original_index = int(item)
            action = {'type': 'edit', 'data': [{'item': str(original_index), 'column': column_name, 'old': original_value, 'new': new_value}]}
            self.app.undo_manager.add_action(action)
            self.app.apply_action(action, is_undo=False)
            
        return "break"

    def cancel_edit(self, event=None):
        if not self.editing_cell: return "break"
        item, _, _ = self.editing_cell
        self.cleanup_edit()
        if self.app.tree.exists(item):
            self.app._refresh_row_display(item)
        return "break"

    def cleanup_edit(self):
        if self.edit_entry: 
            self.edit_entry.destroy()
            self.edit_entry = None
            if hasattr(self.app, 'show_context_hint'):
                self.app.show_context_hint('cell_selected')

        self.editing_cell = None

#==============================================================================
# 9. 検索・置換ダイアログ（親子モード対応版）
#==============================================================================
class SearchReplaceDialog(tk.Toplevel):
    def __init__(self, parent, headers, theme, mode='search_replace'):
        super().__init__(parent)
        self.parent = parent
        self.headers = headers
        self.theme = theme
        self.mode = mode 
        self.result = None

        if self.mode == 'extract':
            self.title("検索して抽出")
        else:
            self.title("検索と置換")
        
        self.geometry("600x640")
        self.resizable(False, False)

        self.search_var=tk.StringVar()
        self.replace_var=tk.StringVar()
        self.regex_var=tk.BooleanVar()
        self.case_var=tk.BooleanVar()
        self.in_selection_var=tk.BooleanVar()
        self.ignore_blanks_var = tk.BooleanVar(value=True)
        
        self.parent_child_mode_var = tk.StringVar(value="normal")
        self.parent_child_column_var = tk.StringVar()
        
        self._create_widgets()
        self._update_button_state()
        self.transient(parent)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self.close)
        self.focus_set()

    def _create_widgets(self):
        main_frame=ttk.Frame(self,padding=10)
        main_frame.pack(fill=tk.BOTH,expand=True)
        main_frame.grid_columnconfigure(1, weight=1)

        ttk.Label(main_frame,text="検索する文字列:").grid(row=0,column=0,sticky=tk.W,pady=2)
        search_entry=ttk.Entry(main_frame,textvariable=self.search_var,width=40)
        search_entry.grid(row=0,column=1,columnspan=2,sticky=tk.EW,pady=2)
        search_entry.bind("<KeyRelease>",self._update_button_state)
        
        if self.mode == 'search_replace':
            ttk.Label(main_frame,text="置換後の文字列:").grid(row=1,column=0,sticky=tk.W,pady=2)
            ttk.Entry(main_frame,textvariable=self.replace_var,width=40).grid(row=1,column=1,columnspan=2,sticky=tk.EW,pady=2)
        
        parent_child_frame = ttk.LabelFrame(main_frame, text="★NEW★ 親子関係モード", padding=10)
        parent_child_frame.grid(row=2, column=0, columnspan=3, sticky="ew", pady=5, padx=2)
        parent_child_frame.grid_columnconfigure(1, weight=1)
        
        mode_frame = ttk.Frame(parent_child_frame)
        mode_frame.grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 5))
        
        ttk.Radiobutton(mode_frame, text="通常モード", variable=self.parent_child_mode_var, 
                       value="normal", command=self._on_mode_change).pack(side=tk.LEFT)
        ttk.Radiobutton(mode_frame, text="親子モード", variable=self.parent_child_mode_var, 
                       value="parent_child", command=self._on_mode_change).pack(side=tk.LEFT, padx=(10, 0))
        
        ttk.Label(parent_child_frame, text="基準列:").grid(row=1, column=0, sticky="w")
        self.column_combo = ttk.Combobox(parent_child_frame, textvariable=self.parent_child_column_var, 
                                        values=self.headers, state="readonly", width=15)
        self.column_combo.grid(row=1, column=1, sticky="w", padx=(5, 0))
        
        self.analyze_button = ttk.Button(parent_child_frame, text="分析実行", 
                                        command=self._analyze_parent_child)
        self.analyze_button.grid(row=1, column=2, padx=(5, 0))
        
        target_frame = ttk.Frame(parent_child_frame)
        target_frame.grid(row=2, column=0, columnspan=3, sticky="w", pady=(5, 0))
        
        ttk.Label(target_frame, text="対象:").pack(side=tk.LEFT)
        self.target_var = tk.StringVar(value="all")
        ttk.Radiobutton(target_frame, text="全て", variable=self.target_var, value="all").pack(side=tk.LEFT, padx=(5, 0))
        ttk.Radiobutton(target_frame, text="親のみ", variable=self.target_var, value="parent").pack(side=tk.LEFT, padx=(5, 0))
        ttk.Radiobutton(target_frame, text="子のみ", variable=self.target_var, value="child").pack(side=tk.LEFT, padx=(5, 0))
        
        self.analysis_text = tk.Text(parent_child_frame, height=4, width=50, state=tk.DISABLED, 
                                    font=("", 9), bg=self.theme.BG_LEVEL_2, fg=self.theme.TEXT_SECONDARY)
        self.analysis_text.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(5, 0))
        
        self._on_mode_change()
        
        col_frame = ttk.LabelFrame(main_frame, text="対象列 (Ctrl/Shiftで複数選択可)")
        col_frame.grid(row=3, column=0, columnspan=3, sticky="ew", pady=5, padx=2)
        col_frame.grid_columnconfigure(0, weight=1)

        col_scrollbar = ttk.Scrollbar(col_frame, orient=tk.VERTICAL)
        self.col_listbox = tk.Listbox(col_frame, selectmode=tk.EXTENDED, yscrollcommand=col_scrollbar.set, height=7)
        col_scrollbar.config(command=self.col_listbox.yview)

        self.col_listbox.grid(row=0, column=0, sticky="ew")
        col_scrollbar.grid(row=0, column=1, sticky="ns")

        for header in self.headers:
            self.col_listbox.insert(tk.END, header)
        self.col_listbox.selection_set(0, tk.END)
        
        option_frame1 = ttk.Frame(main_frame)
        option_frame1.grid(row=4,column=0,columnspan=3,sticky=tk.W,pady=(5,0))
        ttk.Checkbutton(option_frame1,text="正規表現",variable=self.regex_var).pack(side=tk.LEFT)
        ttk.Checkbutton(option_frame1,text="大文字/小文字を区別",variable=self.case_var).pack(side=tk.LEFT,padx=10)
        self.in_selection_cb=ttk.Checkbutton(option_frame1,text="選択範囲のみ",variable=self.in_selection_var)
        self.in_selection_cb.pack(side=tk.LEFT)
        if not self.parent.selected_cells: self.in_selection_cb.config(state=tk.DISABLED)

        option_frame2 = ttk.Frame(main_frame)
        option_frame2.grid(row=5,column=0,columnspan=3,sticky=tk.W)
        ttk.Checkbutton(option_frame2,text="空白セルは無視する",variable=self.ignore_blanks_var).pack(side=tk.LEFT)

        button_frame=ttk.Frame(main_frame)
        button_frame.grid(row=6,column=1,columnspan=2,sticky=tk.E,pady=10)

        if self.mode == 'extract':
            self.extract_btn = ttk.Button(button_frame, text="抽出", command=self.extract)
            self.extract_btn.pack(side=tk.LEFT, padx=2)
        else:
            self.find_all_btn=ttk.Button(button_frame,text="すべて検索",command=self.find_all)
            self.find_all_btn.pack(side=tk.LEFT,padx=2)
            self.replace_all_btn=ttk.Button(button_frame,text="すべて置換",command=self.replace_all)
            self.replace_all_btn.pack(side=tk.LEFT,padx=2)
        
        self.result_var=tk.StringVar()
        ttk.Label(main_frame,textvariable=self.result_var,foreground="blue").grid(row=7,column=0,columnspan=3,sticky=tk.W)

    def _on_mode_change(self):
        """モード変更時の処理"""
        is_parent_child_mode = self.parent_child_mode_var.get() == "parent_child"
        
        state = tk.NORMAL if is_parent_child_mode else tk.DISABLED
        self.column_combo.config(state="readonly" if is_parent_child_mode else tk.DISABLED)
        self.analyze_button.config(state=state)
        
        for widget in self.analyze_button.master.winfo_children():
            if isinstance(widget, ttk.Frame):
                for child in widget.winfo_children():
                    if isinstance(child, ttk.Radiobutton) and str(child.cget('variable')).endswith('target_var'):
                        child.config(state=state)
    
    def _analyze_parent_child(self):
        """親子関係の分析を実行"""
        column_name = self.parent_child_column_var.get()
        if not column_name:
            messagebox.showwarning("列未選択", "基準列を選択してください。", parent=self)
            return
        
        success, message = self.parent.parent_child_manager.analyze_parent_child_relationships(column_name)
        
        if success:
            summary = self.parent.parent_child_manager.get_groups_summary()
            self.analysis_text.config(state=tk.NORMAL)
            self.analysis_text.delete(1.0, tk.END)
            self.analysis_text.insert(1.0, summary)
            self.analysis_text.config(state=tk.DISABLED)
            
            messagebox.showinfo("分析完了", message, parent=self)
        else:
            messagebox.showerror("分析エラー", message, parent=self)

    def _update_button_state(self,*args):
        state = tk.NORMAL if self.search_var.get() else tk.DISABLED
        if self.mode == 'extract':
            self.extract_btn.config(state=state)
        else:
            self.find_all_btn.config(state=state); self.replace_all_btn.config(state=state)
    
    def _compile_regex(self):
        try:
            flags=0 if self.case_var.get() else re.IGNORECASE
            pattern=self.search_var.get()
            if not self.regex_var.get(): pattern=re.escape(pattern)
            return re.compile(pattern,flags)
        except re.error as e: messagebox.showerror("正規表現エラー", f"無効な正規表現です:\n{e}", parent=self); return None

    def _get_search_scope(self):
        selected_indices = self.col_listbox.curselection()
        if not selected_indices:
            messagebox.showwarning("列未選択", "対象の列を少なくとも1つ選択してください。", parent=self)
            return None
        
        target_columns = {self.headers[i] for i in selected_indices}

        base_scope = []
        if self.in_selection_var.get() and self.parent.selected_cells:
            base_scope = self.parent.get_selected_cell_data()
        else:
            base_scope = [{'item': str(idx), 'column': col, 'value': self.parent.df.at[idx, col]}
                          for idx in self.parent.df.index
                          for col in self.headers]
        
        final_scope = [cell for cell in base_scope if cell['column'] in target_columns]

        if self.ignore_blanks_var.get():
            final_scope = [cell for cell in final_scope if str(cell.get('value', '')).strip()]
        
        if self.parent_child_mode_var.get() == "parent_child":
            if not hasattr(self.parent, 'parent_child_manager') or not self.parent.parent_child_manager.parent_child_data:
                messagebox.showwarning("分析未実行", "親子関係の分析を先に実行してください。", parent=self)
                return None
            
            target_type = self.target_var.get()
            final_scope = self.parent.parent_child_manager.filter_rows_by_type(final_scope, target_type)

        return final_scope

    def find_all(self):
        regex=self._compile_regex()
        if not regex: return
        
        scope=self._get_search_scope()
        if scope is None: return

        self.parent._clear_cell_selection()
        found_count=0
        for cell in scope:
            try:
                if not self.parent.tree.exists(cell['item']): continue
                col_id = f"#{self.parent.header.index(cell['column']) + 1}"
                if regex.search(str(cell['value'])):
                    self.parent._select_cell(cell['item'],col_id,cell['column'])
                    found_count+=1
            except (ValueError, IndexError): continue
        
        if self.parent_child_mode_var.get() == "parent_child":
            target_type = self.target_var.get()
            type_text = {"all": "全て", "parent": "親のみ", "child": "子のみ"}[target_type]
            self.result_var.set(f"{found_count}件見つかりました（{type_text}）。")
        else:
            self.result_var.set(f"{found_count}件見つかりました。")

    def replace_all(self):
        regex=self._compile_regex()
        if not regex: return

        scope=self._get_search_scope()
        if scope is None: return

        replace_with=self.replace_var.get()
        replaced_count=0
        changes = []
        for cell in scope:
            original_value=str(cell.get('value',''))
            if regex.search(original_value):
                new_value=regex.sub(replace_with,original_value)
                original_index = int(cell['item'])
                if original_index in self.parent.df.index:
                    row_data = self.parent.df.loc[original_index]
                    if row_data.get(cell['column'])!=new_value:
                        changes.append({'item': str(original_index), 'column': cell['column'], 'old': original_value, 'new': new_value})
                        replaced_count+=1
        if changes:
            action = {'type': 'edit', 'data': changes}
            self.parent.undo_manager.add_action(action)
            self.parent.apply_action(action, is_undo=False)
        
        if self.parent_child_mode_var.get() == "parent_child":
            target_type = self.target_var.get()
            type_text = {"all": "全て", "parent": "親のみ", "child": "子のみ"}[target_type]
            self.parent.show_operation_status(f"{replaced_count}件置換しました（{type_text}）")
        else:
            self.parent.show_operation_status(f"{replaced_count}件置換しました")
    
    def extract(self):
        regex = self._compile_regex()
        if not regex: return
        
        scope = self._get_search_scope()
        if scope is None: return

        found_indices = set()
        for cell in scope:
            if regex.search(str(cell['value'])):
                found_indices.add(int(cell['item']))
        
        if not found_indices:
            self.result_var.set("抽出対象の行が見つかりませんでした。")
            return
        
        self.result = {'action': 'extract', 'indices': sorted(list(found_indices))}
        self.destroy()

    def close(self):
        self.destroy()

#==============================================================================
# 10. ★★★ CSVフォーマット選択ダイアログ ★★★
#==============================================================================
class CSVSaveFormatDialog(tk.Toplevel):
    """CSV保存時のフォーマットを選択するダイアログ"""
    
    def __init__(self, parent, theme, current_quoting_style=csv.QUOTE_MINIMAL):
        super().__init__(parent)
        self.parent = parent
        self.theme = theme
        self.result = None
        
        self.title("CSV保存フォーマット")
        self.geometry("450x300")
        self.resizable(False, False)
        
        self.transient(parent)
        self.grab_set()
        
        self.format_map = {
            "minimal": csv.QUOTE_MINIMAL,
            "all": csv.QUOTE_ALL,
            "none": csv.QUOTE_NONE
        }
        self.reverse_format_map = {v: k for k, v in self.format_map.items()}
        
        initial_value = self.reverse_format_map.get(current_quoting_style, "minimal")
        self.format_var = tk.StringVar(value=initial_value)

        self._create_widgets()
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        
        self.focus_set()
        self.bind("<Return>", self._on_ok)
        self.bind("<Escape>", self._on_cancel)
    
    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        title_label = ttk.Label(main_frame, text="CSV保存フォーマットの選択", 
                               font=("", 12, "bold"))
        title_label.pack(pady=(0, 20))
        
        format_frame = ttk.LabelFrame(main_frame, text="保存形式", padding=10)
        format_frame.pack(fill=tk.X, pady=(0, 20))
        
        formats = [
            ("必要最小限のクォート（推奨）", "minimal"),
            ("全ての値をクォートで囲む", "all"),
            ("クォートを使用しない", "none")
        ]
        
        for text, value in formats:
            rb = ttk.Radiobutton(format_frame, text=text, 
                               variable=self.format_var, value=value)
            rb.pack(anchor=tk.W, pady=2)
        
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=20)
        
        cancel_btn = ttk.Button(button_frame, text="キャンセル", 
                               command=self._on_cancel, width=10)
        cancel_btn.pack(side=tk.RIGHT, padx=(5, 0))
        
        ok_btn = ttk.Button(button_frame, text="保存", 
                           command=self._on_ok, width=10)
        ok_btn.pack(side=tk.RIGHT)
    
    def _on_ok(self, event=None):
        """OKボタンの処理"""
        selected_format_str = self.format_var.get()
        self.result = self.format_map.get(selected_format_str)

        if self.parent and hasattr(self.parent, 'csv_format_manager'):
            self.parent.csv_format_manager.current_format['quoting'] = self.result

        self.destroy()
    
    def _on_cancel(self, event=None):
        """キャンセルボタンの処理"""
        self.result = None
        self.destroy()

#==============================================================================
# 11. ★★★ ウェルカムスクリーン (フェーズ2-Aで追加) ★★★
#==============================================================================
class WelcomeScreen(ttk.Frame):
    """ファイル未開封時のウェルカム画面"""
    def __init__(self, parent, theme, on_file_select=None, on_sample_load=None, on_file_drop=None):
        super().__init__(parent)
        self.theme = theme
        self.on_file_select = on_file_select
        self.on_sample_load = on_sample_load
        self.on_file_drop = on_file_drop 
        
        container = ttk.Frame(self)
        container.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        
        drop_zone = tk.Canvas(
            container,
            width=400,
            height=250,
            bg=self.theme.BG_LEVEL_0,
            highlightthickness=1,
            highlightbackground=self.theme.BG_LEVEL_3,
            relief=tk.SOLID,
            borderwidth=0
        )
        drop_zone.pack(pady=20)
        
        drop_zone.create_text(
            200, 80,
            text="📁",
            font=("", 48),
            fill=self.theme.TEXT_SECONDARY
        )
        
        drop_zone.create_text(
            200, 140,
            text="ファイルを開くか、\nここにファイルをドロップしてください",
            font=("", 16),
            fill=self.theme.TEXT_PRIMARY,
            justify=tk.CENTER
        )
        
        drop_zone.create_text(
            200, 180,
            text="または",
            font=("", 12),
            fill=self.theme.TEXT_SECONDARY
        )

        select_btn = ttk.Button(
            container,
            text="ファイルを選択...",
            command=self.on_file_select,
        )
        select_btn.pack(pady=(0, 15))
        
        ttk.Button(
            container,
            text="📊 サンプルデータで試す",
            command=self.on_sample_load,
            style="Secondary.TButton" 
        ).pack()

#==============================================================================
# 12. ★★★ スマートツールチップ (フェーズ2-Dで追加) ★★★
#==============================================================================
class SmartTooltip:
    """コンテキストに応じた動的ツールチップ"""
    def __init__(self, widget, theme, text_callback=None, delay=800):
        self.widget = widget
        self.theme = theme
        self.text_callback = text_callback
        self.delay = delay
        self.tooltip_window = None
        self.show_timer = None
        widget.bind("<Enter>", self._on_enter, add='+')
        widget.bind("<Leave>", self._on_leave, add='+')
        widget.bind("<Motion>", self._on_motion, add='+')

    def _on_enter(self, event=None):
        self._schedule_show()

    def _on_leave(self, event=None):
        self._cancel_show()
        self._hide()
    
    def _on_motion(self, event=None):
        self._schedule_show()

    def _schedule_show(self):
        self._cancel_show()
        self.show_timer = self.widget.after(self.delay, self._show)

    def _cancel_show(self):
        if self.show_timer:
            self.widget.after_cancel(self.show_timer)
            self.show_timer = None

    def _show(self):
        if self.tooltip_window:
            return

        text = self.text_callback() if self.text_callback else ""
        if not text:
            return

        x = self.widget.winfo_rootx()
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 5

        self.tooltip_window = tk.Toplevel(self.widget)
        self.tooltip_window.wm_overrideredirect(True)
        self.tooltip_window.wm_geometry(f"+{x}+{y}")
        self.tooltip_window.attributes("-topmost", True)
        
        style = ttk.Style()
        style.configure(
            "Tooltip.TFrame",
            background=self.theme.BG_LEVEL_2,
            borderwidth=1,
            relief=tk.SOLID,
            bordercolor=self.theme.BG_LEVEL_3
        )
        style.configure(
            "Tooltip.TLabel",
            background=self.theme.BG_LEVEL_2,
            foreground=self.theme.TEXT_PRIMARY,
            padding=(5, 3)
        )
        frame = ttk.Frame(self.tooltip_window, style="Tooltip.TFrame")
        frame.pack()
        label = ttk.Label(frame, text=text, style="Tooltip.TLabel", justify=tk.LEFT)
        label.pack()

        self._fade_in()

    def _hide(self):
        if self.tooltip_window:
            try:
                self.tooltip_window.destroy()
            except tk.TclError:
                pass
            self.tooltip_window = None

    def _fade_in(self, alpha=0.0):
        if not self.tooltip_window or not self.tooltip_window.winfo_exists():
            return
        
        new_alpha = alpha + 0.1
        if new_alpha >= 0.95:
            self.tooltip_window.attributes("-alpha", 0.95)
            return
        
        self.tooltip_window.attributes("-alpha", new_alpha)
        self.widget.after(15, lambda: self._fade_in(new_alpha))