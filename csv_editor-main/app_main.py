# app_main.py

"""
アプリケーションの中枢となるメインクラス `CsvEditorApp` を定義します。
各機能モジュールやUIモジュールをインポートし、アプリケーション全体の動作を管理します。
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, font, simpledialog
import csv
import os
import re
import time
import math
import pandas as pd
from collections import defaultdict
import threading

# 自作モジュールからのインポート
import config
from config import VIRTUAL_LIST_CHUNK_SIZE
from features import (
    UndoRedoManager,
    CSVFormatManager,
    ClipboardManager,
    CellMergeManager,
    ColumnMergeManager,
    ParentChildManager,
    AsyncDataManager
)
from ui import (
    CellSelectionMixin,
    KeyboardNavigationMixin,
    CardViewNavigationMixin,
    InlineCellEditor,
    SearchReplaceDialog,
    CSVSaveFormatDialog,
    PriceCalculatorDialog,
    ReplaceFromFileDialog,
    WelcomeScreen,
    SmartTooltip,
    RippleButton
)
from lazy_loader import LazyCSVLoader
from db_backend import SQLiteBackend

#==============================================================================
# 11. メインアプリケーションクラス
#==============================================================================
class CsvEditorApp(ttk.Frame, CellSelectionMixin, KeyboardNavigationMixin, CardViewNavigationMixin):
    VIRTUAL_LIST_CHUNK_SIZE = VIRTUAL_LIST_CHUNK_SIZE

    def __init__(self, parent, dataframe=None, title="高機能CSVエディタ"):
        super().__init__(parent, style="App.TFrame")
        self.parent = parent

        self.parent.title(title)
        self.parent.geometry("1024x768")

        self.theme = config.CURRENT_THEME
        self.density = config.CURRENT_DENSITY

        self.filepath=None
        self.header=[]
        self.df = None
        self.lazy_loader = None
        self.db_backend = None

        self.displayed_indices = []
        self.sort_column=None
        self.sort_reverse=False
        self.current_view = 'list'
        self.selected_column = None
        self.encoding = None
        self.column_clipboard = None
        self.filter_var = tk.StringVar()

        self.search_var = tk.StringVar()
        self.search_case_sensitive_var = tk.BooleanVar(value=False)
        self.search_results = []
        self.current_search_index = -1
        
        self.async_manager = AsyncDataManager(self)
        self.performance_mode = False

        style=ttk.Style(self)

        self.view_parent = ttk.Frame(self)
        self.view_parent.pack(fill=tk.BOTH, expand=True)

        self.main_view_container = ttk.Frame(self.view_parent)
        self.list_view_frame = ttk.Frame(self.main_view_container)
        self.card_view_frame = ttk.Frame(self.main_view_container)
        self.tree_frame = ttk.Frame(self.list_view_frame)
        self.tree = ttk.Treeview(self.tree_frame, show='headings', selectmode='none')

        self.welcome_screen = WelcomeScreen(
            self.view_parent,
            self.theme,
            on_file_select=lambda: self.open_file(),
            on_sample_load=lambda: self.test_data()
        )

        self.undo_manager = UndoRedoManager(self)
        self.clipboard_manager = ClipboardManager()
        self.merge_manager = CellMergeManager(self)
        self.column_merge_manager = ColumnMergeManager(self)
        self.parent_child_manager = ParentChildManager(self)
        self.csv_format_manager = CSVFormatManager(self)

        self._init_cell_selection()
        self._init_keyboard_navigation()
        self._init_card_navigation()
        self._init_global_shortcuts()
        self.cell_editor = InlineCellEditor(self)

        self._create_menu()
        self._create_control_frame()
        self._create_search_bar()
        self._create_view_frames()
        self._create_status_bar()

        self._apply_theme()

        if dataframe is not None:
            self.load_dataframe(dataframe, "抽出結果 - 無題")
        else:
            self.show_welcome_screen()
            self._set_ui_state('welcome')

        self.parent.protocol("WM_DELETE_WINDOW", self._on_closing)

    def _cleanup_backend(self):
        """既存のバックエンドをクリーンアップする"""
        if self.db_backend:
            self.db_backend.close()
            self.db_backend = None
        if self.lazy_loader:
            self.lazy_loader = None
        self.df = None
        if hasattr(self, '_df_lower_str'):
            delattr(self, '_df_lower_str')

    def show_welcome_screen(self):
        """WelcomeScreenを表示し、メインビューを隠す"""
        self.main_view_container.pack_forget()
        self.welcome_screen.pack(fill=tk.BOTH, expand=True)

    def show_main_view(self):
        """メインビューを表示し、WelcomeScreenを隠す"""
        self.welcome_screen.pack_forget()
        self.main_view_container.pack(fill=tk.BOTH, expand=True)

    def _apply_theme(self):
        """テーマと表示密度を ttk.Style に適用する"""
        style = ttk.Style(self)
        style.theme_use('clam')

        self.parent.configure(background=self.theme.BG_LEVEL_1)

        style.configure("App.TFrame", background=self.theme.BG_LEVEL_1)

        font_family = font.nametofont("TkDefaultFont").actual()['family']
        style.configure(
            "Treeview",
            rowheight=self.density['row_height'],
            background=self.theme.BG_LEVEL_0,
            fieldbackground=self.theme.BG_LEVEL_0,
            foreground=self.theme.TEXT_PRIMARY,
            font=(font_family, self.density['font_size'])
        )
        style.configure(
            "Treeview.Heading",
            font=(font_family, self.density['font_size'], 'bold'),
            background=self.theme.BG_LEVEL_2,
            foreground=self.theme.TEXT_PRIMARY
        )
        style.map("Treeview.Heading",
            background=[('active', self.theme.BG_LEVEL_3)]
        )

        style.configure("TFrame", background=self.theme.BG_LEVEL_1)
        style.configure("TLabel", background=self.theme.BG_LEVEL_1, foreground=self.theme.TEXT_PRIMARY)
        style.configure("TLabelframe", background=self.theme.BG_LEVEL_1, bordercolor=self.theme.BG_LEVEL_3)
        style.configure("TLabelframe.Label", background=self.theme.BG_LEVEL_1, foreground=self.theme.TEXT_SECONDARY)

        style.configure(
            "TButton",
            padding=self.density['padding'],
            font=(font_family, self.density['font_size']),
            foreground=self.theme.BG_LEVEL_0,
            background=self.theme.PRIMARY,
            borderwidth=1,
            focusthickness=3,
            focuscolor=self.theme.CELL_SELECT_BORDER
        )
        style.map("TButton",
            foreground=[('disabled', self.theme.TEXT_MUTED)],
            background=[
                ('active', self.theme.PRIMARY_ACTIVE),
                ('hover', self.theme.PRIMARY_HOVER),
                ('disabled', self.theme.BG_LEVEL_3)
            ]
        )
        style.configure(
            "Secondary.TButton",
            background=self.theme.BG_LEVEL_2,
            foreground=self.theme.TEXT_PRIMARY
        )
        style.map("Secondary.TButton",
             background=[
                ('active', self.theme.BG_LEVEL_3),
                ('hover', self.theme.BG_LEVEL_3)
            ]
        )

        style.configure("Search.TFrame", background=self.theme.BG_LEVEL_1)
        style.configure("Search.TLabel", background=self.theme.BG_LEVEL_1)
        style.configure("Search.TButton", padding=2)
        style.configure("Search.TCheckbutton", background=self.theme.BG_LEVEL_1)

        self.tree.tag_configure('search_highlight', background='yellow', foreground='black')
        self.tree.tag_configure('current_search_highlight', background='orange', foreground='white')
        self.tree.tag_configure('skeleton', background=self.theme.BG_LEVEL_2, foreground=self.theme.BG_LEVEL_2)

    def _create_menu(self):
        self.menubar=tk.Menu(self.parent)
        self.parent.config(menu=self.menubar)

        file_menu=tk.Menu(self.menubar,tearoff=0)
        self.menubar.add_cascade(label="ファイル",menu=file_menu)
        file_menu.add_command(label="開く...",command=lambda: self.open_file(),accelerator="Ctrl+O")
        file_menu.add_command(label="上書き保存",command=lambda: self.save_file(),accelerator="Ctrl+S")
        file_menu.add_command(label="名前を付けて保存...",command=lambda: self.save_file_as())
        file_menu.add_separator()
        file_menu.add_command(label="終了",command=lambda: self._on_closing())

        self.edit_menu=tk.Menu(self.menubar,tearoff=0)
        self.menubar.add_cascade(label="編集",menu=self.edit_menu)
        self.edit_menu.add_command(label="元に戻す", command=lambda: self._undo(), accelerator="Ctrl+Z")
        self.edit_menu.add_command(label="やり直し", command=lambda: self._redo(), accelerator="Ctrl+Y")
        self.edit_menu.add_separator()
        self.edit_menu.add_command(label="カット", command=lambda: self._cut(), accelerator="Ctrl+X")
        self.edit_menu.add_command(label="コピー", command=lambda: self._copy(), accelerator="Ctrl+C")
        self.edit_menu.add_command(label="ペースト", command=lambda: self._paste(), accelerator="Ctrl+V")
        self.edit_menu.add_command(label="削除", command=lambda: self._delete_selected(), accelerator="Delete")
        self.edit_menu.add_separator()
        self.edit_menu.add_command(label="検索...", command=lambda: self._show_search_bar(), accelerator="Ctrl+F")
        self.edit_menu.add_command(label="置換...",command=lambda: self.open_search_replace_dialog())

        self.edit_menu.add_separator()
        merge_menu = tk.Menu(self.edit_menu, tearoff=0)
        self.edit_menu.add_cascade(label="結合", menu=merge_menu)

        cell_merge_menu = tk.Menu(merge_menu, tearoff=0)
        merge_menu.add_cascade(label="セル結合", menu=cell_merge_menu)
        cell_merge_menu.add_command(label="右のセルと結合", command=lambda: self._merge_right(), accelerator="Ctrl+→")
        cell_merge_menu.add_command(label="左のセルと結合", command=lambda: self._merge_left(), accelerator="Ctrl+←")

        column_merge_menu = tk.Menu(merge_menu, tearoff=0)
        merge_menu.add_cascade(label="列結合", menu=column_merge_menu)
        column_merge_menu.add_command(label="選択列を右の列と結合", command=lambda: self._merge_column_right(), accelerator="Ctrl+Shift+→")
        column_merge_menu.add_command(label="選択列を左の列と結合", command=lambda: self._merge_column_left(), accelerator="Ctrl+Shift+←")

        self.edit_menu.add_separator()
        self.edit_menu.add_command(label="行を追加",command=lambda: self.add_row())
        self.edit_menu.add_command(label="右に列を挿入", command=lambda: self.add_column())
        self.edit_menu.add_command(label="選択行を削除",command=lambda: self.delete_selected_rows())
        self.edit_menu.add_command(label="選択列を削除", command=lambda: self._delete_selected_column())
        self.edit_menu.add_separator()
        self.edit_menu.add_command(label="すべて選択", command=lambda: self._select_all(), accelerator="Ctrl+A")

        sort_menu = tk.Menu(self.edit_menu, tearoff=0)
        self.edit_menu.add_cascade(label="ソート", menu=sort_menu)
        sort_menu.add_command(label="列を選択してソート（右クリック）", state=tk.DISABLED)
        sort_menu.add_separator()
        sort_menu.add_command(label="ソートをクリア", command=lambda: self._clear_sort())

        self.edit_menu.add_separator()
        self.edit_menu.add_command(label="ファイルを参照して置換...", command=lambda: self.open_replace_from_file_dialog())
        self.edit_menu.add_command(label="金額計算ツール...", command=lambda: self.open_price_calculator())
        self.edit_menu.add_command(label="抽出 - 検索語...", command=lambda: self.open_extract_dialog())

        csv_menu = tk.Menu(self.menubar, tearoff=0)
        self.menubar.add_cascade(label="CSVフォーマット", menu=csv_menu)
        csv_menu.add_command(label="保存形式を指定して保存...", command=lambda: self.save_file_as())

        help_menu=tk.Menu(self.menubar,tearoff=0)
        self.menubar.add_cascade(label="ヘルプ",menu=help_menu)

    def _init_global_shortcuts(self):
        self.parent.bind_all("<Control-o>", lambda e: self.open_file())
        self.parent.bind_all("<Control-s>", lambda e: self.save_file())
        self.parent.bind_all("<Control-f>", lambda e: self._show_search_bar())
        self.parent.bind_all("<Escape>", lambda e: self._close_search_bar() if hasattr(self, 'search_bar_visible') and self.search_bar_visible else None)
        self.parent.bind_all("<Control-a>", lambda e: self._select_all())
        self.parent.bind_all("<Control-z>", self._undo)
        self.parent.bind_all("<Control-y>", self._redo)
        self.parent.bind_all("<Control-x>", self._cut)
        self.parent.bind_all("<Control-c>", self._copy)
        self.parent.bind_all("<Control-v>", self._paste)
        self.parent.bind_all("<Delete>", self._delete_selected)
        self.parent.bind_all("<F1>", self._show_shortcuts_help)

        self.parent.bind_all("<Control-Delete>", self._delete_selected_column)

        self.parent.bind_all("<Control-Right>", self._merge_right)
        self.parent.bind_all("<Control-Left>", self._merge_left)
        self.parent.bind_all("<Control-Shift-Right>", self._merge_column_right)
        self.parent.bind_all("<Control-Shift-Left>", self._merge_column_left)

        self.parent.bind_all("<Control-Shift-c>", lambda e: self.copy_selected_column())
        self.parent.bind_all("<Control-Shift-C>", lambda e: self.copy_selected_column())
        self.parent.bind_all("<Control-Shift-v>", lambda e: self.paste_to_selected_column())
        self.parent.bind_all("<Control-Shift-V>", lambda e: self.paste_to_selected_column())

        for char in ['a', 'z', 'y', 'x', 'c', 'v', 'o', 's', 'f']:
            self.parent.bind_all(f"<Control-{char.upper()}>", self.bind_all(f"<Control-{char}>"))

    def _merge_right(self, event=None):
        if self.lazy_loader or self.db_backend: messagebox.showinfo("制限", "読み取り専用モードではこの機能は使用できません。"); return
        if not self.last_selected_cell or self.selected_column:
            messagebox.showinfo("情報", "セル結合を行うには、セルを選択してください。\n（列選択状態では実行できません）")
            return "break"

        success, message = self.merge_manager.merge_cells_right(self.last_selected_cell)
        if success:
            self.show_operation_status("セルを右に結合しました")
        else:
            messagebox.showwarning("セル結合エラー", message)
        return "break"

    def _merge_left(self, event=None):
        if self.lazy_loader or self.db_backend: messagebox.showinfo("制限", "読み取り専用モードではこの機能は使用できません。"); return
        if not self.last_selected_cell or self.selected_column:
            messagebox.showinfo("情報", "セル結合を行うには、セルを選択してください。\n（列選択状態では実行できません）")
            return "break"

        success, message = self.merge_manager.merge_cells_left(self.last_selected_cell)
        if success:
            self.show_operation_status("セルを左に結合しました")
        else:
            messagebox.showwarning("セル結合エラー", message)
        return "break"

    def _merge_column_right(self, event=None):
        if self.lazy_loader or self.db_backend: messagebox.showinfo("制限", "読み取り専用モードではこの機能は使用できません。"); return
        if not self.selected_column or self.selected_cells:
            messagebox.showinfo("情報", "列結合を行うには、列ヘッダーをクリックして列を選択してください。\n（セル選択状態では実行できません）")
            return "break"

        success, message = self.column_merge_manager.merge_column_right(self.selected_column)
        if success:
            self.show_operation_status("列を右に結合しました")
        else:
            messagebox.showwarning("列結合エラー", message)
        return "break"

    def _merge_column_left(self, event=None):
        if self.lazy_loader or self.db_backend: messagebox.showinfo("制限", "読み取り専用モードではこの機能は使用できません。"); return
        if not self.selected_column or self.selected_cells:
            messagebox.showinfo("情報", "列結合を行うには、列ヘッダーをクリックして列を選択してください。\n（セル選択状態では実行できません）")
            return "break"

        success, message = self.column_merge_manager.merge_column_left(self.selected_column)
        if success:
            self.show_operation_status("列を左に結合しました")
        else:
            messagebox.showwarning("列結合エラー", message)
        return "break"

    def update_menu_states(self):
        if not hasattr(self, 'edit_menu'): return

        has_data = self.df is not None or self.lazy_loader is not None or self.db_backend is not None
        is_readonly = self.lazy_loader is not None or self.db_backend is not None

        state = tk.NORMAL if has_data else tk.DISABLED

        self.edit_menu.entryconfig("元に戻す", state=tk.DISABLED if is_readonly else (tk.NORMAL if self.undo_manager.can_undo() else tk.DISABLED))
        self.edit_menu.entryconfig("やり直し", state=tk.DISABLED if is_readonly else (tk.NORMAL if self.undo_manager.can_redo() else tk.DISABLED))

        col_op_state = tk.NORMAL if self.active_cell and has_data and not is_readonly else tk.DISABLED
        self.edit_menu.entryconfig("右に列を挿入", state=col_op_state)

        column_delete_state = tk.NORMAL if self.selected_column and has_data and not is_readonly else tk.DISABLED
        self.edit_menu.entryconfig("選択列を削除", state=column_delete_state)

        try:
            cell_merge_available = self.last_selected_cell and not self.selected_column and has_data
            column_merge_available = self.selected_column and not self.selected_cells and has_data

            for i in range(self.edit_menu.index('end') + 1):
                try:
                    if self.edit_menu.entrycget(i, 'label') == '結合':
                        overall_state = tk.NORMAL if (cell_merge_available or column_merge_available) and not is_readonly else tk.DISABLED
                        self.edit_menu.entryconfig(i, state=overall_state)
                        break
                except tk.TclError:
                    continue
        except tk.TclError:
            pass

    def _create_control_frame(self):
        control_frame = ttk.Frame(self)
        control_frame.pack(fill=tk.X, padx=10, pady=5)

        test_button = RippleButton(control_frame, text="テスト", command=lambda: self.test_data())
        test_button.pack(side=tk.LEFT, padx=5)
        SmartTooltip(test_button, self.theme, text_callback=lambda: "動作確認用のテストデータを読み込みます。")

        ttk.Label(control_frame, text="フィルタ:").pack(side=tk.LEFT, padx=(0, 5))
        filter_entry = ttk.Entry(control_frame, textvariable=self.filter_var, width=30)
        filter_entry.pack(side=tk.LEFT, expand=True, fill=tk.X)
        filter_entry.bind("<KeyRelease>", lambda e: self.after(300, self.filter_data))

        merge_frame = ttk.LabelFrame(control_frame, text="結合", padding=5)
        merge_frame.pack(side=tk.RIGHT, padx=5)

        cell_merge_frame = ttk.Frame(merge_frame)
        cell_merge_frame.pack(fill=tk.X, pady=(0, 2))
        ttk.Label(cell_merge_frame, text="セル:", font=("", 8)).pack(side=tk.LEFT, padx=(0, 3))
        self.merge_left_button = RippleButton(cell_merge_frame, text="←", command=lambda: self._merge_left(), width=3)
        self.merge_left_button.pack(side=tk.LEFT, padx=1)
        SmartTooltip(self.merge_left_button, self.theme, text_callback=lambda: "選択セルを左のセルと結合します (Ctrl+←)")
        self.merge_right_button = RippleButton(cell_merge_frame, text="→", command=lambda: self._merge_right(), width=3)
        self.merge_right_button.pack(side=tk.LEFT, padx=1)
        SmartTooltip(self.merge_right_button, self.theme, text_callback=lambda: "選択セルを右のセルと結合します (Ctrl+→)")

        column_merge_frame = ttk.Frame(merge_frame)
        column_merge_frame.pack(fill=tk.X)
        ttk.Label(column_merge_frame, text="列:", font=("", 8)).pack(side=tk.LEFT, padx=(0, 3))
        self.column_merge_left_button = RippleButton(column_merge_frame, text="←", command=lambda: self._merge_column_left(), width=3)
        self.column_merge_left_button.pack(side=tk.LEFT, padx=1)
        SmartTooltip(self.column_merge_left_button, self.theme, text_callback=lambda: "選択列を左の列と結合します (Ctrl+Shift+←)")
        self.column_merge_right_button = RippleButton(column_merge_frame, text="→", command=lambda: self._merge_column_right(), width=3)
        self.column_merge_right_button.pack(side=tk.LEFT, padx=1)
        SmartTooltip(self.column_merge_right_button, self.theme, text_callback=lambda: "選択列を右の列と結合します (Ctrl+Shift+→)")

        self.save_button = RippleButton(control_frame, text="上書き保存", command=lambda: self.save_file())
        self.save_button.pack(side=tk.RIGHT, padx=(5, 0))
        SmartTooltip(self.save_button, self.theme, text_callback=lambda: f"現在の変更をファイルに上書き保存します (Ctrl+S)\nファイルパス: {self.filepath or '未保存'}")

        open_button = RippleButton(control_frame, text="ファイルを開く...", command=lambda: self.open_file())
        open_button.pack(side=tk.RIGHT, padx=(5, 0))
        SmartTooltip(open_button, self.theme, text_callback=lambda: "新しいCSVファイルを開きます (Ctrl+O)")

        search_button = RippleButton(control_frame, text="検索/置換...", command=lambda: self.open_search_replace_dialog())
        search_button.pack(side=tk.RIGHT, padx=(5, 0))
        SmartTooltip(search_button, self.theme, text_callback=lambda: "検索と置換ダイアログを開きます")

        self.view_toggle_button = RippleButton(control_frame, text="カードで編集", command=lambda: self.open_card_view())
        self.view_toggle_button.pack(side=tk.RIGHT, padx=(5,0))
        SmartTooltip(self.view_toggle_button, self.theme, text_callback=lambda: "選択中の行をカード形式で表示・編集します")

    def _create_search_bar(self):
        self.search_frame = ttk.Frame(self.parent, style="Search.TFrame")

        search_left = ttk.Frame(self.search_frame, style="Search.TFrame")
        search_left.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5, pady=5)

        ttk.Label(search_left, text="🔍", style="Search.TLabel").pack(side=tk.LEFT, padx=(0, 5))

        self.search_entry = ttk.Entry(search_left, textvariable=self.search_var, width=40)
        self.search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.search_entry.bind("<KeyRelease>", self._on_search_change)
        self.search_entry.bind("<Return>", self._search_next)

        self.search_counter = ttk.Label(self.search_frame, text="", style="Search.TLabel", foreground=self.theme.TEXT_SECONDARY)
        self.search_counter.pack(side=tk.LEFT, padx=10)

        nav_frame = ttk.Frame(self.search_frame, style="Search.TFrame")
        nav_frame.pack(side=tk.RIGHT, padx=5)

        self.search_prev_btn = RippleButton(nav_frame, text="↑", width=3, command=lambda: self._search_previous(), style="Search.TButton")
        self.search_prev_btn.pack(side=tk.LEFT)
        self.search_next_btn = RippleButton(nav_frame, text="↓", width=3, command=lambda: self._search_next(), style="Search.TButton")
        self.search_next_btn.pack(side=tk.LEFT, padx=(2, 0))

        case_btn = ttk.Checkbutton(
            nav_frame,
            text="Aa",
            variable=self.search_case_sensitive_var,
            command=self._on_search_change,
            style="Search.TCheckbutton"
        )
        case_btn.pack(side=tk.LEFT, padx=(10, 0))
        SmartTooltip(case_btn, self.theme, text_callback=lambda: "大文字/小文字を区別する")

        ttk.Button(nav_frame, text="✕", width=3, command=lambda: self._close_search_bar(), style="Search.TButton").pack(side=tk.LEFT, padx=(10, 0))

    def _show_search_bar(self, event=None):
        if self.df is None and self.lazy_loader is None and self.db_backend is None:
            self.show_operation_status("検索するファイルを開いてください", duration=2000)
            return

        if hasattr(self, 'search_bar_visible') and self.search_bar_visible:
            self.search_entry.focus_set()
            self.search_entry.select_range(0, tk.END)
            return

        self.search_bar_visible = True
        self.search_frame.place(relx=0, rely=1.0, relwidth=1, anchor='sw')
        self.search_entry.focus_set()
        self._on_search_change()

    def _close_search_bar(self, event=None):
        if not hasattr(self, 'search_bar_visible') or not self.search_bar_visible:
            return

        self.search_bar_visible = False
        self.search_frame.place_forget()
        self.search_var.set("")
        self._clear_search_highlight()
        self.search_results = []
        self.current_search_index = -1
        self.tree.focus_set()
        return "break"

    def _clear_search_highlight(self):
        for tag in ['search_highlight', 'current_search_highlight']:
            items_with_tag = self.tree.tag_has(tag)
            if items_with_tag:
                self.tree.tag_remove(tag, *items_with_tag)

    def _on_search_change(self, event=None):
        if hasattr(self, '_search_timer'):
            self.after_cancel(self._search_timer)
        self._search_timer = self.after(300, self._perform_search)

    def _perform_search(self):
        query = self.search_var.get()
        if not query:
            self.search_counter.config(text="検索語を入力")
            self._clear_search_highlight()
            self.search_results = []
            self.current_search_index = -1
            return

        if self.db_backend:
            self.search_counter.config(text="DB検索中...")
            self.update_idletasks()
            def search_in_background():
                results = self.db_backend.search(query, self.header)
                self.after(0, self._process_db_search_results, results)
            threading.Thread(target=search_in_background, daemon=True).start()
        else:
            self._perform_treeview_search(query)

    def _process_db_search_results(self, row_indices):
        self._clear_search_highlight()
        self.search_results = [(str(idx), "#1") for idx in row_indices]
        if self.search_results:
            self.current_search_index = 0
            visible_items = self.tree.get_children('')
            for iid, _ in self.search_results:
                if iid in visible_items:
                    self.tree.tag_add('search_highlight', iid)
            self._highlight_current_search_result()
        else:
            self.search_counter.config(text="0件")

    def _perform_treeview_search(self, query):
        self._clear_search_highlight()
        visible_items = self.tree.get_children('')
        self.search_results = []

        initial_search_count = min(100, len(visible_items))
        for iid in visible_items[:initial_search_count]:
            if iid.startswith('skeleton_'): continue
            values = self.tree.item(iid, 'values')
            for col_idx, value in enumerate(values):
                if query.lower() in str(value).lower():
                    self.search_results.append((iid, f"#{col_idx+1}"))
                    self.tree.tag_add('search_highlight', iid)

        if self.search_results:
            self.current_search_index = 0
            self._highlight_current_search_result()
        else:
            self.search_counter.config(text="0件")

        if len(visible_items) > initial_search_count:
            self.after(10, lambda: self._continue_background_search(initial_search_count))

    def _continue_background_search(self, start_index):
        visible_items = self.tree.get_children('')
        end_index = min(start_index + 100, len(visible_items))
        query = self.search_var.get()

        if not query: return

        for iid in visible_items[start_index:end_index]:
            if iid.startswith('skeleton_'): continue
            values = self.tree.item(iid, 'values')
            for col_idx, value in enumerate(values):
                if query.lower() in str(value).lower():
                    if not any(res[0] == iid for res in self.search_results):
                        self.search_results.append((iid, f"#{col_idx+1}"))
                        self.tree.tag_add('search_highlight', iid)

        if self.current_search_index != -1:
            self.search_counter.config(text=f"{self.current_search_index + 1}/{len(self.search_results)}")
        else:
            self.search_counter.config(text=f"{len(self.search_results)}件")

        if end_index < len(visible_items):
            self.after(10, lambda: self._continue_background_search(end_index))


    def _search_next(self, event=None):
        if not self.search_results: return "break"
        self.current_search_index = (self.current_search_index + 1) % len(self.search_results)
        self._highlight_current_search_result()
        return "break"

    def _search_previous(self, event=None):
        if not self.search_results: return "break"
        self.current_search_index = (self.current_search_index - 1 + len(self.search_results)) % len(self.search_results)
        self._highlight_current_search_result()
        return "break"

    def _highlight_current_search_result(self):
        self.tree.tag_remove('current_search_highlight', *self.tree.tag_has('current_search_highlight'))

        if self.current_search_index == -1 or not self.search_results:
            self.search_counter.config(text=f"{len(self.search_results)}件")
            return

        iid, col_id = self.search_results[self.current_search_index]
        self.tree.tag_add('current_search_highlight', iid)

        self.tree.see(iid)

        self.search_counter.config(text=f"{self.current_search_index + 1}/{len(self.search_results)}")

    def _create_view_frames(self):
        self.main_view_container.pack(fill=tk.BOTH, expand=True)
        self.main_view_container.grid_rowconfigure(0, weight=1)
        self.main_view_container.grid_columnconfigure(0, weight=1)

        self.list_view_frame.grid(row=0, column=0, sticky="nsew")
        self.card_view_frame.grid(row=0, column=0, sticky="nsew")

        self._populate_list_view_widgets()
        self._create_card_view_widgets()
        
        self.switch_view('list')

    def _populate_list_view_widgets(self):
        self.tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 5))
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        ysb = ttk.Scrollbar(self.tree_frame, orient=tk.VERTICAL, command=self.on_tree_scroll)
        xsb = ttk.Scrollbar(self.list_view_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        ysb.pack(side=tk.RIGHT, fill=tk.Y)
        xsb.pack(side=tk.BOTTOM, fill=tk.X, padx=10, pady=(0,5))

        self.tree.tag_configure('oddrow', background=self.theme.BG_LEVEL_0, foreground=self.theme.TEXT_PRIMARY)
        self.tree.tag_configure('evenrow', background=self.theme.BG_LEVEL_1, foreground=self.theme.TEXT_PRIMARY)

    def on_tree_scroll(self, *args):
        if args[0] == 'moveto':
            self.tree.yview_moveto(args[1])
        elif args[0] == 'scroll':
            self.tree.yview_scroll(args[1], args[2])

        self.after_cancel(getattr(self, '_scroll_timer', ''))
        self._scroll_timer = self.after(50, self.update_virtual_list)

    def update_virtual_list(self):
        first_visible_item_id = self.tree.identify_row(0)
        if not first_visible_item_id:
             if not self.tree.get_children(''):
                 self.populate_list_view(start_index=0)
             return

        try:
            first_view_index = self.displayed_indices.index(int(first_visible_item_id))
        except (ValueError, tk.TclError):
             return

        self.populate_list_view(start_index=first_view_index)

    def _setup_card_view_scrolling(self, canvas, scrollable_frame):
        def _on_mousewheel(event):
            if event.delta:
                scroll_val = int(-1 * (event.delta / 120)) if abs(event.delta) >= 120 else -event.delta
            else:
                scroll_val = -1 if event.num == 4 else 1
            canvas.yview_scroll(scroll_val, "units")

        def _bind_mousewheel(event=None):
            canvas.bind_all("<MouseWheel>", _on_mousewheel)
            canvas.bind_all("<Button-4>", _on_mousewheel)
            canvas.bind_all("<Button-5>", _on_mousewheel)

        def _unbind_mousewheel(event=None):
            canvas.unbind_all("<MouseWheel>")
            canvas.unbind_all("<Button-4>")
            canvas.unbind_all("<Button-5>")

        for widget in [canvas, scrollable_frame]:
            widget.bind("<Enter>", _bind_mousewheel)
            widget.bind("<Leave>", _unbind_mousewheel)

    def _create_card_view_widgets(self):
        self.card_entries = {}; self.card_current_original_index = None
        canvas = tk.Canvas(self.card_view_frame, highlightthickness=0)
        scrollbar = ttk.Scrollbar(self.card_view_frame, orient="vertical", command=canvas.yview)
        self.card_scrollable_frame = ttk.Frame(canvas)
        self.card_scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=self.card_scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        self._setup_card_view_scrolling(canvas, self.card_scrollable_frame)
        canvas.pack(side="left", fill="both", expand=True, padx=10, pady=5); scrollbar.pack(side="right", fill="y", pady=5)
        button_frame = ttk.Frame(self.card_scrollable_frame); button_frame.pack(fill=tk.X, pady=20, padx=10)
        RippleButton(button_frame, text="変更を保存", command=lambda: self.update_from_card()).pack(side=tk.LEFT, padx=5)

    def _create_status_bar(self):
        self.status_frame = ttk.Frame(self)
        self.status_frame.pack(side=tk.BOTTOM, fill=tk.X)

        basic_frame = ttk.Frame(self.status_frame)
        basic_frame.pack(fill=tk.X, padx=5, pady=2)
        self.status_basic = tk.StringVar()
        self.status_basic.set("ファイルを開いてください。")
        ttk.Label(basic_frame, textvariable=self.status_basic).pack(side=tk.LEFT)

        self.operation_frame = ttk.Frame(self.status_frame)
        self.status_operation = tk.StringVar()
        self.operation_label = ttk.Label(
            self.operation_frame,
            textvariable=self.status_operation,
            foreground=self.theme.SUCCESS
        )
        self.operation_label.pack(side=tk.LEFT, padx=5)

        self.hint_frame = ttk.Frame(self.status_frame)
        self.status_hint = tk.StringVar()
        ttk.Label(
            self.hint_frame,
            textvariable=self.status_hint,
            foreground=self.theme.TEXT_SECONDARY,
            font=("", 9)
        ).pack(side=tk.LEFT, padx=5)

        self.progress_bar = ttk.Progressbar(
            self.status_frame,
            mode='indeterminate',
            length=100
        )

    def _hide_operation_status(self):
        self.operation_frame.pack_forget()
        self.status_operation.set("")

    def show_operation_status(self, message, duration=3000):
        self.status_operation.set(f"✓ {message}")
        self.operation_frame.pack(fill=tk.X, padx=5, pady=2)
        if hasattr(self, '_operation_timer'):
            self.after_cancel(self._operation_timer)
        self._operation_timer = self.after(duration, self._hide_operation_status)

    def show_context_hint(self, hint_key):
        if not hint_key:
            self.hint_frame.pack_forget()
            return

        hints = {
            'cell_selected': "💡 Enter/F2で編集 | Ctrl+Cでコピー | Deleteで削除",
            'column_selected': "💡 右クリックでメニュー | Ctrl+Shift+Cで列コピー",
            'editing': "💡 Enterで確定 | Escでキャンセル",
            'filter_active': "💡 フィルタ適用中 (絞り込み表示)"
        }

        hint_text = hints.get(hint_key, hint_key if isinstance(hint_key, str) else "")
        if hint_text:
            self.status_hint.set(hint_text)
            self.hint_frame.pack(fill=tk.X, padx=5, pady=2)
        else:
            self.hint_frame.pack_forget()

    def _set_ui_state(self, state_name):
        is_readonly = self.lazy_loader is not None or self.db_backend is not None
        if state_name == 'welcome':
            self.menubar.entryconfig("ファイル", state=tk.NORMAL)
            self.menubar.entryconfig("編集", state=tk.DISABLED)
            self.menubar.entryconfig("CSVフォーマット", state=tk.DISABLED)
            self.menubar.entryconfig("ヘルプ", state=tk.NORMAL)
        elif state_name == 'disabled':
            self.menubar.entryconfig("編集", state=tk.DISABLED)
            self.menubar.entryconfig("CSVフォーマット", state=tk.DISABLED)
        else: # 'normal'
            self.menubar.entryconfig("ファイル", state=tk.NORMAL)
            self.menubar.entryconfig("編集", state=tk.NORMAL if not is_readonly else tk.DISABLED)
            self.menubar.entryconfig("CSVフォーマット", state=tk.NORMAL)
            self.menubar.entryconfig("ヘルプ", state=tk.NORMAL)

        if self.df is not None or self.lazy_loader is not None or self.db_backend is not None:
             self._update_action_button_states()

    def _update_action_button_states(self):
        has_data = self.df is not None or self.lazy_loader is not None or self.db_backend is not None
        is_readonly = self.lazy_loader is not None or self.db_backend is not None

        state = tk.NORMAL if has_data else tk.DISABLED
        pass

        has_selection = (self.selected_cells or self.active_cell) and has_data
        card_btn_state = tk.NORMAL if has_selection and not is_readonly else tk.DISABLED

        if hasattr(self, 'view_toggle_button'):
            self.view_toggle_button.config(state=card_btn_state)

        cell_merge_available = self.last_selected_cell and not self.selected_column and has_data and not is_readonly
        cell_merge_btn_state = tk.NORMAL if cell_merge_available else tk.DISABLED
        if hasattr(self, 'merge_left_button'): self.merge_left_button.config(state=cell_merge_btn_state)
        if hasattr(self, 'merge_right_button'): self.merge_right_button.config(state=cell_merge_btn_state)

        column_merge_available = self.selected_column and not self.selected_cells and has_data and not is_readonly
        column_merge_btn_state = tk.NORMAL if column_merge_available else tk.DISABLED
        if hasattr(self, 'column_merge_left_button'): self.column_merge_left_button.config(state=column_merge_btn_state)
        if hasattr(self, 'column_merge_right_button'): self.column_merge_right_button.config(state=column_merge_btn_state)

        self.update_menu_states()

    def _update_rows_from_df(self, df):
        """DataFrameからTreeViewを更新（行が存在しない場合は挿入）"""
        for idx, row in df.iterrows():
            iid = str(idx)
            try:
                # ▼▼▼ 修正箇所：列識別子を#1, #2...に戻すため、col_nameではなくcol_idを使う ▼▼▼
                view_index = self.displayed_indices.index(idx)
                tag = 'evenrow' if view_index % 2 == 0 else 'oddrow'
                values = [str(row.get(h, "")) for h in self.header]
                
                if self.tree.exists(iid):
                    self.tree.item(iid, values=values, tags=(tag,))
                else:
                    self.tree.insert("", "end", iid=iid, values=values, tags=(tag,))
            except (ValueError, tk.TclError):
                continue

    def _show_skeleton_screen(self, start_index, end_index):
        """指定範囲にスケルトン（プレースホルダー）を表示"""
        if not self.header: return
        placeholder_values = ["..." for _ in self.header]
        for i in range(start_index, end_index):
            if i >= len(self.displayed_indices): break
            iid = str(self.displayed_indices[i])
            if not self.tree.exists(iid):
                self.tree.insert("", "end", iid=iid, values=placeholder_values, tags=('skeleton',))

    def _on_closing(self):
        self.async_manager.shutdown()
        self._cleanup_backend()
        if hasattr(self.cell_editor, 'edit_entry') and self.cell_editor.edit_entry:
            if not messagebox.askokcancel("確認", "編集中のセルがあります。変更を破棄して終了しますか？"): return
        if self.df is not None and self.undo_manager.can_undo():
             if not messagebox.askokcancel("確認", "未保存の変更があります。変更を破棄して終了しますか？"): return
        if messagebox.askokcancel("終了", "アプリケーションを終了しますか？"): self.parent.destroy()

    def _select_all(self, event=None):
        if self.current_view != 'list': return "break"
        self._clear_cell_selection()
        for iid in self.tree.get_children():
            if iid.startswith('skeleton_'): continue
            for c_idx, col_name in enumerate(self.header):
                self._select_cell(iid, f"#{c_idx + 1}", col_name)
        return "break"

    def _undo(self, event=None):
        if self.lazy_loader or self.db_backend: return "break"
        self.undo_manager.undo(); return "break"

    def _redo(self, event=None):
        if self.lazy_loader or self.db_backend: return "break"
        self.undo_manager.redo(); return "break"

    def _copy(self, event=None):
        if self.current_view != 'list': return "break"
        selected = self.get_selected_cell_data()
        if selected:
            self.clipboard_manager.copy_cells_to_clipboard(self, selected)
            self.show_operation_status(f"{len(selected)}個のセルをコピーしました")
        return "break"

    def _cut(self, event=None):
        if self.lazy_loader or self.db_backend: return "break"
        self._copy(event); self._delete_selected(event); return "break"

    def _paste(self, event=None):
        if self.lazy_loader or self.db_backend: return "break"
        if self.current_view != 'list' or not self.active_cell: return "break"
        df_index, col_idx = self.active_cell

        try:
            item_id = str(df_index)
        except IndexError:
            return "break"

        changes_to_apply = self.clipboard_manager.get_paste_data_from_clipboard(self, item_id, col_idx)
        if not changes_to_apply: return "break"

        undo_data = []
        for change in changes_to_apply:
            original_index = int(change['item'])
            old_val = self.df.at[original_index, change['column']]
            undo_data.append({'item': str(original_index), 'column': change['column'], 'old': old_val, 'new': change['value']})

        action = {'type': 'edit', 'data': undo_data}
        self.undo_manager.add_action(action)
        self.apply_action(action, is_undo=False)
        self.show_operation_status("クリップボードから貼り付けました")
        return "break"

    def _delete_selected(self, event=None):
        if self.lazy_loader or self.db_backend: return "break"
        if self.current_view != 'list' or not self.selected_cells: return "break"

        changes = []
        for item, col in self.selected_cells:
            original_index = int(item)
            old_val = self.df.at[original_index, col]
            if old_val != "":
                changes.append({'item': str(original_index), 'column': col, 'old': old_val, 'new': ""})

        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action)
            self.apply_action(action, is_undo=False)
            self.show_operation_status(f"{len(changes)}個のセルの内容を削除しました")
        return "break"

    def copy_selected_column(self, event=None):
        if not self.selected_column:
            messagebox.showinfo("情報", "コピーする列を選択してください。")
            return
        if self.lazy_loader or self.db_backend:
             messagebox.showinfo("制限", "読み取り専用モードではこの機能は使用できません。")
             return

        self.column_clipboard = self.df[self.selected_column].tolist()
        self.show_operation_status(f"列「{self.selected_column}」をコピーしました")

    def paste_to_selected_column(self, event=None):
        if self.lazy_loader or self.db_backend: return
        if not self.selected_column or self.column_clipboard is None:
            messagebox.showinfo("情報", "貼り付ける先の列を選択し、事前に列をコピーしてください。")
            return

        dest_col = self.selected_column
        copied_data = self.column_clipboard

        if len(copied_data) != len(self.df):
            if not messagebox.askyesno("確認", f"コピー元の行数 ({len(copied_data)}) と現在の行数 ({len(self.df)}) が異なります。\n可能な限り貼り付けますか？"):
                return

        changes = []
        for i, new_val in zip(self.df.index, copied_data):
            old_val = self.df.at[i, dest_col]
            if str(old_val) != str(new_val):
                changes.append({'item': str(i), 'column': dest_col, 'old': old_val, 'new': new_val})

        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action)
            self.apply_action(action, is_undo=False)
            self.show_operation_status(f"{len(changes)}行を列「{dest_col}」に貼り付けました")
        else:
            messagebox.showinfo("情報", "変更はありませんでした。")

    def _show_shortcuts_help(self, event=None):
        help_text = """【ショートカットキー一覧】
（この部分は未実装です）
"""
        messagebox.showinfo("ショートカットキー", help_text, parent=self)
        return "break"

    def filter_data(self):
        """フィルタ条件に基づいてデータを絞り込み、表示を更新"""
        search_term = self.filter_var.get().strip().lower()

        if self.performance_mode:
            if self.db_backend:
                if not search_term: self.displayed_indices = self.db_backend.get_all_indices()
                else: self.displayed_indices = self.db_backend.search(search_term, self.header)
            elif self.lazy_loader:
                if not search_term: self.displayed_indices = list(range(self.lazy_loader.total_rows))
                else: self.displayed_indices = self.lazy_loader.search_in_file(search_term)
            
            self.populate_list_view(start_index=0)
            self._update_status_bar()
            self.show_context_hint('filter_active' if search_term else None)
            return

        if self.df is None: return
        if self.current_view == 'card': self.switch_view('list')

        temp_df = self.df
        if search_term:
            self.show_context_hint('filter_active')
            if not hasattr(self, '_df_lower_str'):
                self._df_lower_str = self.df.astype(str).apply(lambda col: col.str.lower())
            
            try:
                mask = self._df_lower_str.apply(lambda row: row.str.contains(search_term, na=False)).any(axis=1)
                temp_df = self.df.loc[mask]
            except Exception as e:
                print(f"Filter operation failed: {e}")
                temp_df = pd.DataFrame(columns=self.df.columns)
        else:
            self.show_context_hint(None)
            if hasattr(self, '_df_lower_str'):
                delattr(self, '_df_lower_str')

        if self.sort_column and self.sort_column in temp_df.columns:
            temp_df = temp_df.sort_values(by=self.sort_column, ascending=not self.sort_reverse)

        self.displayed_indices = temp_df.index.tolist()
        self.populate_list_view(start_index=0)
        self._update_status_bar()

    def populate_list_view(self, start_index=0, end_index=None):
        if not self.header: return

        # ▼▼▼ 修正箇所：列定義を初回描画時に集約 ▼▼▼
        if start_index == 0:
            self.tree.delete(*self.tree.get_children())
            # #1, #2...形式の列IDを生成
            column_ids = [f"#{i+1}" for i in range(len(self.header))]
            self.tree["columns"] = column_ids
            for col_id, col_name in zip(column_ids, self.header):
                self.tree.heading(col_id, text=col_name)
                self.tree.column(col_id, width=160, anchor=tk.W)

        total_rows = len(self.displayed_indices)
        if end_index is None:
            end_index = min(start_index + self.VIRTUAL_LIST_CHUNK_SIZE, total_rows)
        
        if self.performance_mode:
            self._show_skeleton_screen(start_index, end_index)
            indices_to_fetch = self.displayed_indices[start_index:end_index]
            self.async_manager.fetch_data_for_indices(indices_to_fetch)
        else:
            if self.df is not None:
                indices_to_show = self.displayed_indices[start_index:end_index]
                if indices_to_show:
                    df_to_show = self.df.loc[indices_to_show]
                    self._update_rows_from_df(df_to_show)

        if start_index == 0:
            self.after(50, self._adjust_column_widths)
        self._update_status_bar()

    def _clear_sort(self):
        """ソートをクリア"""
        if self.performance_mode:
            messagebox.showinfo("制限", "パフォーマンスモードではソートできません。")
            return
        self.sort_column = None
        self.sort_reverse = False
        self.filter_data()
        self.show_operation_status("ソートをクリアしました")

    def _delete_selected_column(self, event=None):
        is_readonly = self.lazy_loader is not None or self.db_backend is not None
        if is_readonly: messagebox.showinfo("制限", "読み取り専用モードではこの機能は使用できません。"); return
        if not self.selected_column:
            messagebox.showinfo("情報", "削除する列を選択してください。")
            return "break"

        if hasattr(self, '_delete_column_with_confirmation'):
            self._delete_column_with_confirmation(self.selected_column)

        return "break"

    def open_file(self, filepath=None):
        if not filepath:
            filepath = filedialog.askopenfilename(
                title="CSVファイルを開く",
                filetypes=[("CSVファイル", "*.csv"), ("テキストファイル", "*.txt"), ("すべてのファイル", "*.*")]
            )
        if not filepath: return

        self._cleanup_backend()
        self.performance_mode = False
        self.tree['columns'] = [] 

        self.encoding = self._detect_encoding(filepath)
        if not self.encoding:
            messagebox.showerror("エラー", "ファイルのエンコーディングを検出できませんでした。")
            return

        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        load_mode = 'normal'

        if file_size_mb > 10:
            self.performance_mode = True
            result = messagebox.askyesnocancel("大きなファイル",
                f"ファイルサイズが {file_size_mb:.1f} MBと大きいため、パフォーマンスモードを選択してください。\n\n"
                "・「はい」: SQLiteモード（推奨：初回読込は遅いが、後の操作が超高速）\n"
                "・「いいえ」: 遅延読み込みモード（初回読込は速いが、操作が重くなる可能性）\n"
                "・「キャンセル」: 読み込みを中止します",
                icon='question')
            if result is None: return
            load_mode = 'sqlite' if result else 'lazy'

        try:
            self.show_main_view()
            if load_mode == 'sqlite':
                self.db_backend = SQLiteBackend(self)
                delimiter = self.csv_format_manager.detect_format(filepath, self.encoding).get('delimiter', ',')
                columns, total_rows = self.db_backend.import_csv_with_progress(filepath, self.encoding, delimiter)

                if columns:
                    self.header = columns
                    self.displayed_indices = list(range(total_rows))
                    self.filepath = filepath
                    self.parent.title(f"高機能CSVエディタ - {os.path.basename(filepath)} [SQLiteモード]")
                    self._set_ui_state('normal')
                    self.undo_manager.clear()
                    self.populate_list_view(0)
                    self._recreate_card_view_entries()
                    self.show_operation_status(f"SQLiteモードで開きました ({total_rows:,}行)", duration=5000)

            elif load_mode == 'lazy':
                self.show_operation_status("大きなファイルを読み込んでいます...")
                self.update_idletasks()
                self.lazy_loader = LazyCSVLoader(filepath, self.encoding, self.theme)
                self.header = self.lazy_loader.header
                self.displayed_indices = list(range(self.lazy_loader.total_rows))
                self.filepath = filepath
                self.parent.title(f"高機能CSVエディタ - {os.path.basename(filepath)} [遅延読み込みモード]")
                self._set_ui_state('normal')
                self.undo_manager.clear()
                self.populate_list_view(0)
                self._recreate_card_view_entries()
                self.show_operation_status(f"遅延読み込みモードで開きました ({self.lazy_loader.total_rows:,}行)", duration=5000)

            else: # normal
                self.progress_bar.pack(side=tk.RIGHT, padx=5, in_=self.status_frame)
                self.progress_bar.start()
                self.update_idletasks()
                df = pd.read_csv(filepath, encoding=self.encoding, dtype=str).fillna('')
                self.csv_format_manager.detect_format(filepath, self.encoding)
                self.load_dataframe(df, os.path.basename(filepath), filepath=filepath, encoding=self.encoding)
                self.show_operation_status(f"ファイルを開きました ({len(df):,}行)")
                self.progress_bar.stop()
                self.progress_bar.pack_forget()

        except Exception as e:
            messagebox.showerror("予期せぬエラー", f"ファイルの読み込み中に予期せぬエラーが発生しました。\n\n{e}")
            self._cleanup_backend()
            self.show_welcome_screen()
            self._set_ui_state('welcome')

    def switch_view(self, view_name):
        self.current_view = view_name
        is_readonly = self.lazy_loader is not None or self.db_backend is not None
        if view_name == 'list':
            self._unbind_card_navigation()
            self.card_view_frame.grid_remove()
            self.list_view_frame.grid()
            self.view_toggle_button.config(text="カードで編集", command=self.open_card_view)
            if is_readonly:
                self.view_toggle_button.config(state=tk.DISABLED)
            self.tree.focus_set()
            self._refresh_all_rows()
        elif view_name == 'card':
            if is_readonly: messagebox.showinfo("制限", "読み取り専用モードではカードビューは使用できません。"); return
            self._bind_card_navigation()
            self.list_view_frame.grid_remove()
            self.card_view_frame.grid()
            self.view_toggle_button.config(text="一覧に戻る", command=lambda: self.switch_view('list'))
            self._update_card_navigation_status()

    def test_data(self):
        self._cleanup_backend()
        self.performance_mode = False
        header = ["商品名", "価格", "在庫数", "カテゴリ"]
        data = [
            {"商品名": "リンゴ", "価格": "100", "在庫数": "50", "カテゴリ": "果物"},
            {"商品名": "バナナ", "価格": "80", "在庫数": "100", "カテゴリ": "果物"},
            {"商品名": "牛肉", "価格": "1200", "在庫数": "20", "カテゴリ": "精肉"},
            {"商品名": "牛乳", "価格": "250", "在庫数": "30", "カテゴリ": "乳製品"},
            {"商品名": "パン", "価格": "180", "在庫数": "40", "カテゴリ": "パン"},
        ]

        df = pd.DataFrame(data, columns=header)
        self.load_dataframe(df, "テストデータ.csv", encoding='utf-8')

        self.show_operation_status("テストデータをロードしました")

    def _should_enable_animations(self):
        if not config.ENABLE_ANIMATIONS:
            return False
        
        if self.performance_mode:
            return False

        return True

    def _detect_encoding(self, filepath):
        for enc in ['utf-8', 'shift_jis', 'cp932', 'euc-jp', 'latin1']:
            try:
                pd.read_csv(filepath, encoding=enc, nrows=5)
                return enc
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        return None

    def load_dataframe(self, dataframe, title, filepath=None, encoding='utf-8'):
        if hasattr(self, '_df_lower_str'):
            delattr(self, '_df_lower_str')

        self.df = dataframe
        self.header = list(self.df.columns)
        self.displayed_indices = self.df.index.tolist()
        self.encoding = encoding
        self.filepath = filepath

        self.parent.title(f"高機能CSVエディタ - {title}")

        self.show_main_view()
        self._set_ui_state('normal')

        self.undo_manager.clear()
        self.filter_data()
        self._recreate_card_view_entries()
        self._update_status_bar()

    def open_card_view(self):
        if not self.active_cell and not self.selected_cells: messagebox.showinfo("情報", "カードで表示する行を1つ選択してください。"); return

        display_index = -1
        if self.active_cell:
            df_index, _ = self.active_cell
            if df_index in self.displayed_indices:
                display_index = self.displayed_indices.index(df_index)
        elif self.last_selected_cell:
            item, _ = self.last_selected_cell
            df_index = int(item)
            if df_index in self.displayed_indices:
                display_index = self.displayed_indices.index(df_index)

        if display_index != -1:
            self._populate_card_view(display_index)
            self.switch_view('card')
        else:
            messagebox.showinfo("情報", "選択された行が見つかりません。")

    def open_search_replace_dialog(self):
        if self.df is None and self.lazy_loader is None and self.db_backend is None: return
        if hasattr(self, 'header') and self.header:
            dialog = SearchReplaceDialog(self, self.header, self.theme, mode='search_replace')
            self.wait_window(dialog)

    def _update_status_bar(self):
        if self.df is None and self.lazy_loader is None and self.db_backend is None:
            self.status_basic.set("ファイルを開いてください。")
            return

        total_rows = 0
        if self.df is not None:
            total_rows = len(self.df)
        elif self.lazy_loader is not None:
            total_rows = self.lazy_loader.total_rows
        elif self.db_backend is not None:
            total_rows = self.db_backend.get_total_rows()

        displayed_rows = len(self.displayed_indices)

        status_text = f"表示中: {displayed_rows:,} / {total_rows:,} 件"

        if self.filepath:
            mode_text = ""
            if self.lazy_loader:
                mode_text = " [遅延読込]"
            elif self.db_backend:
                mode_text = " [SQLite]"

            format_info = ""
            if self.df is not None:
                detected_format = self.csv_format_manager.current_format
                format_info = "クォート付き" if detected_format.get('detected_has_quotes', False) else "クォートなし"
                format_info = f", {format_info}"

            status_text += f" | ファイル: {os.path.basename(self.filepath)}{mode_text} ({self.encoding}{format_info})"

        if self.df is not None and '価格' in self.header:
            try:
                prices = pd.to_numeric(self.df.loc[self.displayed_indices, '価格'], errors='coerce').dropna()
                if not prices.empty:
                    status_text += f" | '価格'の合計: {prices.sum():,.0f}"
            except Exception:
                pass

        self.status_basic.set(status_text)

    def _adjust_column_widths(self):
        """列幅をデータ内容に応じて最適化する"""
        for idx, col_name in enumerate(self.header):
            col_id = f"#{idx + 1}"
            try:
                header_width = font.Font().measure(col_name) + 20
            except tk.TclError:
                header_width = len(col_name) * 12 + 20

            items_to_check = self.tree.get_children('')[:50]
            if not items_to_check:
                max_content_width = header_width
            else:
                content_widths = [font.Font().measure(self.tree.set(item, col_id))
                                  for item in items_to_check if not self.tree.tag_has('skeleton', item)]
                
                if content_widths:
                    max_content_width = max(content_widths + [header_width])
                else:
                    max_content_width = header_width

            final_width = min(max(160, max_content_width + 20), 400)
            self.tree.column(col_id, width=final_width)

    def _recreate_card_view_entries(self):
        for widget in self.card_scrollable_frame.winfo_children():
            is_button_frame = False
            if isinstance(widget, ttk.Frame):
                if any(isinstance(child, RippleButton) for child in widget.winfo_children()):
                    is_button_frame = True
            
            if not is_button_frame:
                widget.destroy()

        button_frame = next((c for c in self.card_scrollable_frame.winfo_children() if isinstance(c, ttk.Frame)), None)

        self.card_entries.clear()
        for i, field in enumerate(self.header):
            row_frame = ttk.Frame(self.card_scrollable_frame)
            row_frame.pack(fill=tk.X, padx=10, pady=2)
            ttk.Label(row_frame, text=f"{field}:", width=20, anchor=tk.W).pack(side=tk.LEFT)
            entry = ttk.Entry(row_frame)
            entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
            self.card_entries[field] = entry

        if button_frame:
            button_frame.pack_forget()
            button_frame.pack(fill=tk.X, pady=20, padx=10)