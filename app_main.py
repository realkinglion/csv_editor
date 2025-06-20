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

# 自作モジュールからのインポート
import config 
from config import VIRTUAL_LIST_CHUNK_SIZE
from features import (
    UndoRedoManager,
    CSVFormatManager,
    ClipboardManager,
    CellMergeManager,
    ColumnMergeManager,
    ParentChildManager
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
    SmartTooltip
)

#==============================================================================
# 11. メインアプリケーションクラス
#==============================================================================
class CsvEditorApp(tk.Tk, CellSelectionMixin, KeyboardNavigationMixin, CardViewNavigationMixin):
    VIRTUAL_LIST_CHUNK_SIZE = VIRTUAL_LIST_CHUNK_SIZE

    def __init__(self, dataframe=None, title="高機能CSVエディタ"):
        super().__init__()
        self.title(title)
        self.geometry("1024x768")

        self.theme = config.CURRENT_THEME
        self.density = config.CURRENT_DENSITY
        
        self.filepath=None
        self.header=[]
        self.df = None
        self.displayed_indices = []
        self.sort_column=None
        self.sort_reverse=False
        self.current_view = 'list'
        self.selected_column = None
        self.encoding = None
        self.column_clipboard = None
        self.filter_var = tk.StringVar()
        
        style=ttk.Style(self)

        self.main_container = ttk.Frame(self)
        self.main_container.pack(fill=tk.BOTH, expand=True)

        self.view_parent = ttk.Frame(self.main_container)
        self.view_parent.pack(fill=tk.BOTH, expand=True)

        self.main_view_container = ttk.Frame(self.view_parent)
        self.list_view_frame = ttk.Frame(self.main_view_container)
        self.card_view_frame = ttk.Frame(self.main_view_container)
        self.tree_frame = ttk.Frame(self.list_view_frame)
        self.tree = ttk.Treeview(self.tree_frame, show='headings', selectmode='none')
        
        self.welcome_screen = WelcomeScreen(self.view_parent, self.theme, on_file_select=self.open_file, on_sample_load=self.test_data)

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
        self._create_view_frames()
        self._create_status_bar()
        
        self._apply_theme()

        if dataframe is not None:
            self.load_dataframe(dataframe, "抽出結果 - 無題")
        else:
            self.show_welcome_screen()
            self._set_ui_state('welcome')

        self.protocol("WM_DELETE_WINDOW", self._on_closing)

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

        self.configure(background=self.theme.BG_LEVEL_1)
        
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

    def _create_menu(self):
        self.menubar=tk.Menu(self)
        self.config(menu=self.menubar)
        
        file_menu=tk.Menu(self.menubar,tearoff=0)
        self.menubar.add_cascade(label="ファイル",menu=file_menu)
        file_menu.add_command(label="開く...",command=self.open_file,accelerator="Ctrl+O")
        file_menu.add_command(label="上書き保存",command=self.save_file,accelerator="Ctrl+S")
        file_menu.add_command(label="名前を付けて保存...",command=self.save_file_as)
        file_menu.add_separator()
        file_menu.add_command(label="終了",command=self._on_closing)
        
        self.edit_menu=tk.Menu(self.menubar,tearoff=0)
        self.menubar.add_cascade(label="編集",menu=self.edit_menu)
        self.edit_menu.add_command(label="元に戻す", command=self._undo, accelerator="Ctrl+Z")
        self.edit_menu.add_command(label="やり直し", command=self._redo, accelerator="Ctrl+Y")
        self.edit_menu.add_separator()
        self.edit_menu.add_command(label="カット", command=self._cut, accelerator="Ctrl+X")
        self.edit_menu.add_command(label="コピー", command=self._copy, accelerator="Ctrl+C")
        self.edit_menu.add_command(label="ペースト", command=self._paste, accelerator="Ctrl+V")
        self.edit_menu.add_command(label="削除", command=self._delete_selected, accelerator="Delete")
        self.edit_menu.add_separator()
        
        merge_menu = tk.Menu(self.edit_menu, tearoff=0)
        self.edit_menu.add_cascade(label="結合", menu=merge_menu)
        
        cell_merge_menu = tk.Menu(merge_menu, tearoff=0)
        merge_menu.add_cascade(label="セル結合", menu=cell_merge_menu)
        cell_merge_menu.add_command(label="右のセルと結合", command=self._merge_right, accelerator="Ctrl+→")
        cell_merge_menu.add_command(label="左のセルと結合", command=self._merge_left, accelerator="Ctrl+←")
        
        column_merge_menu = tk.Menu(merge_menu, tearoff=0)
        merge_menu.add_cascade(label="列結合", menu=column_merge_menu)
        column_merge_menu.add_command(label="選択列を右の列と結合", command=self._merge_column_right, accelerator="Ctrl+Shift+→")
        column_merge_menu.add_command(label="選択列を左の列と結合", command=self._merge_column_left, accelerator="Ctrl+Shift+←")
        
        self.edit_menu.add_separator()
        self.edit_menu.add_command(label="行を追加",command=self.add_row)
        self.edit_menu.add_command(label="右に列を挿入", command=self.add_column)
        self.edit_menu.add_command(label="選択行を削除",command=self.delete_selected_rows)
        self.edit_menu.add_command(label="選択列を削除", command=self._delete_selected_column)
        self.edit_menu.add_separator()
        self.edit_menu.add_command(label="検索と置換...",command=self.open_search_replace_dialog,accelerator="Ctrl+F")
        self.edit_menu.add_command(label="すべて選択", command=self._select_all, accelerator="Ctrl+A")
        
        sort_menu = tk.Menu(self.edit_menu, tearoff=0)
        self.edit_menu.add_cascade(label="ソート", menu=sort_menu)
        sort_menu.add_command(label="列を選択してソート（右クリック）", state=tk.DISABLED)
        sort_menu.add_separator()
        sort_menu.add_command(label="ソートをクリア", command=self._clear_sort)

        self.edit_menu.add_separator()
        self.edit_menu.add_command(label="ファイルを参照して置換...", command=self.open_replace_from_file_dialog)
        self.edit_menu.add_command(label="金額計算ツール...", command=self.open_price_calculator)
        self.edit_menu.add_command(label="抽出 - 検索語...", command=self.open_extract_dialog) 
        

        csv_menu = tk.Menu(self.menubar, tearoff=0)
        self.menubar.add_cascade(label="CSVフォーマット", menu=csv_menu)
        csv_menu.add_command(label="保存形式を指定して保存...", command=self.save_file_as)

        help_menu=tk.Menu(self.menubar,tearoff=0)
        self.menubar.add_cascade(label="ヘルプ",menu=help_menu)
    
    def _init_global_shortcuts(self):
        self.bind_all("<Control-o>", lambda e: self.open_file())
        self.bind_all("<Control-s>", lambda e: self.save_file())
        self.bind_all("<Control-f>", lambda e: self.open_search_replace_dialog())
        self.bind_all("<Control-a>", lambda e: self._select_all())
        self.bind_all("<Control-z>", self._undo)
        self.bind_all("<Control-y>", self._redo)
        self.bind_all("<Control-x>", self._cut)
        self.bind_all("<Control-c>", self._copy)
        self.bind_all("<Control-v>", self._paste)
        self.bind_all("<Delete>", self._delete_selected)
        self.bind_all("<F1>", self._show_shortcuts_help)
        
        self.bind_all("<Control-Delete>", self._delete_selected_column)
        
        self.bind_all("<Control-Right>", self._merge_right)
        self.bind_all("<Control-Left>", self._merge_left)
        self.bind_all("<Control-Shift-Right>", self._merge_column_right)
        self.bind_all("<Control-Shift-Left>", self._merge_column_left)
        
        self.bind_all("<Control-Shift-c>", lambda e: self.copy_selected_column())
        self.bind_all("<Control-Shift-C>", lambda e: self.copy_selected_column())
        self.bind_all("<Control-Shift-v>", lambda e: self.paste_to_selected_column())
        self.bind_all("<Control-Shift-V>", lambda e: self.paste_to_selected_column())
        
        for char in ['a', 'z', 'y', 'x', 'c', 'v', 'o', 's', 'f']:
            self.bind_all(f"<Control-{char.upper()}>", self.bind_all(f"<Control-{char}>"))
    
    def _merge_right(self, event=None):
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
        
        has_data = self.df is not None
        state = tk.NORMAL if has_data else tk.DISABLED

        self.edit_menu.entryconfig("元に戻す", state=tk.NORMAL if self.undo_manager.can_undo() else tk.DISABLED)
        self.edit_menu.entryconfig("やり直し", state=tk.NORMAL if self.undo_manager.can_redo() else tk.DISABLED)
        
        col_op_state = tk.NORMAL if self.active_cell and has_data else tk.DISABLED
        self.edit_menu.entryconfig("右に列を挿入", state=col_op_state)
        
        column_delete_state = tk.NORMAL if self.selected_column and has_data else tk.DISABLED
        self.edit_menu.entryconfig("選択列を削除", state=column_delete_state)
        
        try:
            cell_merge_available = self.last_selected_cell and not self.selected_column and has_data
            column_merge_available = self.selected_column and not self.selected_cells and has_data
            
            for i in range(self.edit_menu.index('end') + 1):
                try:
                    if self.edit_menu.entrycget(i, 'label') == '結合':
                        overall_state = tk.NORMAL if (cell_merge_available or column_merge_available) else tk.DISABLED
                        self.edit_menu.entryconfig(i, state=overall_state)
                        break
                except tk.TclError:
                    continue
        except tk.TclError:
            pass

    def _create_control_frame(self):
        control_frame = ttk.Frame(self.main_container)
        control_frame.pack(fill=tk.X, padx=10, pady=5)
        
        test_button = ttk.Button(control_frame, text="テスト", command=self.test_data)
        test_button.pack(side=tk.LEFT, padx=5)
        SmartTooltip(test_button, self.theme, text_callback=lambda: "動作確認用のテストデータを読み込みます。")

        ttk.Label(control_frame, text="フィルタ:").pack(side=tk.LEFT, padx=(0, 5))
        filter_entry = ttk.Entry(control_frame, textvariable=self.filter_var, width=30)
        filter_entry.pack(side=tk.LEFT, expand=True, fill=tk.X)
        self.filter_var.trace_add("write", lambda *args: self.filter_data())
        SmartTooltip(filter_entry, self.theme, text_callback=lambda: "入力した文字で全列をリアルタイムに絞り込みます。")

        merge_frame = ttk.LabelFrame(control_frame, text="結合", padding=5)
        merge_frame.pack(side=tk.RIGHT, padx=5)
        
        cell_merge_frame = ttk.Frame(merge_frame)
        cell_merge_frame.pack(fill=tk.X, pady=(0, 2))
        ttk.Label(cell_merge_frame, text="セル:", font=("", 8)).pack(side=tk.LEFT, padx=(0, 3))
        self.merge_left_button = ttk.Button(cell_merge_frame, text="←", command=self._merge_left, width=3)
        self.merge_left_button.pack(side=tk.LEFT, padx=1)
        SmartTooltip(self.merge_left_button, self.theme, text_callback=lambda: "選択セルを左のセルと結合します (Ctrl+←)")
        self.merge_right_button = ttk.Button(cell_merge_frame, text="→", command=self._merge_right, width=3)
        self.merge_right_button.pack(side=tk.LEFT, padx=1)
        SmartTooltip(self.merge_right_button, self.theme, text_callback=lambda: "選択セルを右のセルと結合します (Ctrl+→)")
        
        column_merge_frame = ttk.Frame(merge_frame)
        column_merge_frame.pack(fill=tk.X)
        ttk.Label(column_merge_frame, text="列:", font=("", 8)).pack(side=tk.LEFT, padx=(0, 3))
        self.column_merge_left_button = ttk.Button(column_merge_frame, text="←", command=self._merge_column_left, width=3)
        self.column_merge_left_button.pack(side=tk.LEFT, padx=1)
        SmartTooltip(self.column_merge_left_button, self.theme, text_callback=lambda: "選択列を左の列と結合します (Ctrl+Shift+←)")
        self.column_merge_right_button = ttk.Button(column_merge_frame, text="→", command=self._merge_column_right, width=3)
        self.column_merge_right_button.pack(side=tk.LEFT, padx=1)
        SmartTooltip(self.column_merge_right_button, self.theme, text_callback=lambda: "選択列を右の列と結合します (Ctrl+Shift+→)")

        self.save_button = ttk.Button(control_frame, text="上書き保存", command=self.save_file)
        self.save_button.pack(side=tk.RIGHT, padx=(5, 0))
        SmartTooltip(self.save_button, self.theme, text_callback=lambda: f"現在の変更をファイルに上書き保存します (Ctrl+S)\nファイルパス: {self.filepath or '未保存'}")
        
        open_button = ttk.Button(control_frame, text="ファイルを開く...", command=self.open_file)
        open_button.pack(side=tk.RIGHT, padx=(5, 0))
        SmartTooltip(open_button, self.theme, text_callback=lambda: "新しいCSVファイルを開きます (Ctrl+O)")

        search_button = ttk.Button(control_frame, text="検索/置換", command=self.open_search_replace_dialog)
        search_button.pack(side=tk.RIGHT, padx=(5, 0))
        SmartTooltip(search_button, self.theme, text_callback=lambda: "検索と置換ウィンドウを開きます (Ctrl+F)")

        self.view_toggle_button = ttk.Button(control_frame, text="カードで編集", command=self.open_card_view)
        self.view_toggle_button.pack(side=tk.RIGHT, padx=(5,0))
        SmartTooltip(self.view_toggle_button, self.theme, text_callback=lambda: "選択中の行をカード形式で表示・編集します")

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
        self._scroll_timer = self.after(100, self.update_virtual_list)

    def update_virtual_list(self):
        if not self.tree.get_children():
            self.populate_list_view(start_index=0)
            return

        first_visible_item_id = self.tree.identify_row(0)
        last_visible_item_id = self.tree.identify_row(self.tree.winfo_height())
        
        if not first_visible_item_id:
            return

        try:
            first_index = self.tree.index(first_visible_item_id)
            last_index = self.tree.index(last_visible_item_id) if last_visible_item_id else first_index
        except:
             return

        start = max(0, first_index - self.VIRTUAL_LIST_CHUNK_SIZE // 2)
        end = min(len(self.displayed_indices), last_index + self.VIRTUAL_LIST_CHUNK_SIZE // 2)
        
        self.populate_list_view(start_index=start, end_index=end)

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
        ttk.Button(button_frame, text="変更を保存", command=self.update_from_card).pack(side=tk.LEFT, padx=5)

    def _create_status_bar(self):
        """多層ステータスバーの作成"""
        self.status_frame = ttk.Frame(self.main_container, relief=tk.SUNKEN, borderwidth=1)
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
        """操作状態を非表示にする"""
        self.operation_frame.pack_forget()
        self.status_operation.set("")

    def show_operation_status(self, message, duration=3000):
        """操作状態を一時的に表示"""
        self.status_operation.set(f"✓ {message}")
        self.operation_frame.pack(fill=tk.X, padx=5, pady=2)
        if hasattr(self, '_operation_timer'):
            self.after_cancel(self._operation_timer)
        self._operation_timer = self.after(duration, self._hide_operation_status)

    def show_context_hint(self, hint_key):
        """コンテキストに応じたヒントを表示"""
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
            self.menubar.entryconfig("編集", state=tk.NORMAL)
            self.menubar.entryconfig("CSVフォーマット", state=tk.NORMAL)
            self.menubar.entryconfig("ヘルプ", state=tk.NORMAL)
        
        if self.df is not None:
             self._update_action_button_states()

    def _update_action_button_states(self):
        state = tk.NORMAL if self.df is not None else tk.DISABLED

        for child in self._create_control_frame.__self__.winfo_children():
            if isinstance(child, (ttk.Button, ttk.Entry, ttk.LabelFrame)):
                if child.winfo_class() == 'TButton' and 'ファイルを開く' in child.cget('text'):
                    continue
                child.config(state=state)
        
        has_selection = (self.selected_cells or self.active_cell) and state == tk.NORMAL
        card_btn_state = tk.NORMAL if has_selection else tk.DISABLED
        
        if hasattr(self, 'view_toggle_button'):
            self.view_toggle_button.config(state=card_btn_state)
        
        cell_merge_available = self.last_selected_cell and not self.selected_column and state == tk.NORMAL
        cell_merge_btn_state = tk.NORMAL if cell_merge_available else tk.DISABLED
        if hasattr(self, 'merge_left_button'): self.merge_left_button.config(state=cell_merge_btn_state)
        if hasattr(self, 'merge_right_button'): self.merge_right_button.config(state=cell_merge_btn_state)
        
        column_merge_available = self.selected_column and not self.selected_cells and state == tk.NORMAL
        column_merge_btn_state = tk.NORMAL if column_merge_available else tk.DISABLED
        if hasattr(self, 'column_merge_left_button'): self.column_merge_left_button.config(state=column_merge_btn_state)
        if hasattr(self, 'column_merge_right_button'): self.column_merge_right_button.config(state=column_merge_btn_state)
        
        self.update_menu_states()

    def open_search_replace_dialog(self):
        if self.df is None: return
        if hasattr(self, 'header') and self.header:
            dialog = SearchReplaceDialog(self, self.header, self.theme, mode='search_replace')
            self.wait_window(dialog)

    def open_file(self, filepath=None):
        if not filepath:
            filepath = filedialog.askopenfilename(
                title="CSVファイルを開く",
                filetypes=[("CSVファイル", "*.csv"), ("テキストファイル", "*.txt"), ("すべてのファイル", "*.*")]
            )
        if not filepath: return
        
        try:
            encoding = None
            for enc in ['utf-8', 'shift_jis', 'cp932', 'euc-jp']:
                try:
                    pd.read_csv(filepath, encoding=enc, nrows=1)
                    encoding = enc
                    break
                except (UnicodeDecodeError, pd.errors.ParserError):
                    continue

            if not encoding:
                 messagebox.showerror("ファイル読み込みエラー", "サポートされているエンコーディング（UTF-8, Shift_JISなど）でCSVファイルを正しく読み込めませんでした。")
                 return

            df = pd.read_csv(filepath, encoding=encoding, dtype=str).fillna('')
            self.load_dataframe(df, os.path.basename(filepath), filepath, encoding)

        except FileNotFoundError:
            messagebox.showerror("ファイルエラー", f"指定されたファイルが見つかりません:\n{filepath}")
            self._set_ui_state('disabled')
        except Exception as e:
            messagebox.showerror("予期せぬエラー", f"ファイルの読み込み中に予期せぬエラーが発生しました。\n\n{e}")
            self._set_ui_state('disabled')

    def load_dataframe(self, dataframe, title, filepath=None, encoding='utf-8'):
        self.df = dataframe
        self.header = list(self.df.columns)
        self.displayed_indices = self.df.index.tolist()
        self.encoding = encoding
        self.filepath = filepath
        
        self.title(f"高機能CSVエディタ - {title}")
        
        self.show_main_view()
        self._set_ui_state('normal')

        self.undo_manager.clear()
        self.filter_data()
        self._recreate_card_view_entries()
        self._update_status_bar()

    def save_file(self):
        if self.df is None: return
        if hasattr(self.cell_editor, 'edit_entry') and self.cell_editor.edit_entry: 
            self.cell_editor.finish_edit()
        if not self.filepath: 
            self.save_file_as() 
            return
        
        if self._save_to_path(self.filepath, self.csv_format_manager.current_format['quoting']):
             self.show_operation_status(f"ファイルを上書き保存しました")

    def save_file_as(self):
        if self.df is None: return
        if hasattr(self.cell_editor, 'edit_entry') and self.cell_editor.edit_entry: 
            self.cell_editor.finish_edit()
        
        filepath = filedialog.asksaveasfilename(
            title="名前を付けて保存", 
            initialfile=os.path.basename(self.filepath or "data.csv"), 
            defaultextension=".csv", 
            filetypes=[("CSV", "*.csv"),("All", "*.*")]
        )
        if not filepath: 
            return
        
        current_quoting = self.csv_format_manager.current_format['quoting']
        dialog = CSVSaveFormatDialog(self, self.theme, current_quoting_style=current_quoting)
        self.wait_window(dialog)
        
        if dialog.result is not None:
            if self._save_to_path(filepath, dialog.result):
                self.filepath = filepath
                self.title(f"高機能CSVエディタ - {os.path.basename(self.filepath)}")
                self.show_operation_status(f"名前を付けて保存しました")

    def _save_to_path(self, filepath, quoting_style=None):
        try:
            success = self.csv_format_manager.save_with_format(
                filepath, 
                self.df,
                quoting_style=quoting_style,
                encoding=self.encoding or 'utf-8'
            )
            if success:
                self._update_status_bar()
            return success
        except Exception as e:
            messagebox.showerror("ファイル保存エラー", f"ファイルの保存中にエラーが発生しました。\n\n{e}")
            return False
            
    def open_price_calculator(self):
        if self.df is None:
            messagebox.showwarning("情報", "ファイルを開いてから実行してください。")
            return
        
        dialog = PriceCalculatorDialog(self, self.header, self.theme)
        self.wait_window(dialog)

        if dialog.result:
            self._apply_price_calculation(dialog.result)

    def _apply_price_calculation(self, params):
        target_col = params['column']
        tax_status = params['tax_status']
        discount = params['discount']

        tax_rate = 1.10
        discount_multiplier = 1.0 - (discount / 100.0)
        
        changes = []
        for i, row in self.df.iterrows():
            original_value_str = str(row.get(target_col, ''))
            if not original_value_str.strip():
                continue

            try:
                price = float(original_value_str)
            except (ValueError, TypeError):
                continue
            
            new_price = 0
            if tax_status == 'exclusive':
                price_with_tax = price * tax_rate
                discounted_price_with_tax = price_with_tax * discount_multiplier
                new_price = discounted_price_with_tax / tax_rate
            else:
                discounted_price_with_tax = price * discount_multiplier
                new_price = discounted_price_with_tax / tax_rate

            new_value_str = str(math.floor(new_price))

            if new_value_str != original_value_str:
                changes.append({
                    'item': str(i), 
                    'column': target_col, 
                    'old': original_value_str, 
                    'new': new_value_str
                })
        
        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action)
            self.apply_action(action, is_undo=False)
            self.show_operation_status(f"{len(changes)}件の金額を更新しました")
        else:
            self.show_operation_status("金額の更新はありませんでした", duration=2000)

    def open_replace_from_file_dialog(self):
        if self.df is None:
            messagebox.showwarning("情報", "ファイルを開いてから実行してください。")
            return
        
        dialog = ReplaceFromFileDialog(self, self.header, self.theme)
        self.wait_window(dialog)

        if dialog.result:
            self._apply_replace_from_file(dialog.result)

    def _apply_replace_from_file(self, params):
        try:
            lookup_df = pd.read_csv(params['lookup_filepath'], encoding=self.encoding, dtype=str).fillna('')
        except Exception as e:
            messagebox.showerror("参照ファイルエラー", f"参照ファイルの読み込みに失敗しました。\n{e}")
            return

        if not all(k in lookup_df.columns for k in [params['lookup_key_col'], params['replace_val_col']]):
            messagebox.showerror("列エラー", "参照ファイルに必要な列が見つかりません。")
            return
        
        original_dtype = self.df[params['target_col']].dtype

        df_copy = self.df.copy()
        df_copy[params['target_col']] = df_copy[params['target_col']].astype(str)
        lookup_df[params['lookup_key_col']] = lookup_df[params['lookup_key_col']].astype(str)
        
        merged_df = df_copy.merge(
            lookup_df[[params['lookup_key_col'], params['replace_val_col']]],
            left_on=params['target_col'],
            right_on=params['lookup_key_col'],
            how='left'
        )
        
        new_values_col = params['replace_val_col']
        
        changed_mask = merged_df[new_values_col].notna() & (merged_df[params['target_col']] != merged_df[new_values_col])
        changed_indices = merged_df.index[changed_mask]
        
        if changed_indices.empty:
            messagebox.showinfo("情報", "置換対象となるデータが見つかりませんでした。")
            return

        changes = []
        for idx in changed_indices:
            changes.append({
                'item': str(idx),
                'column': params['target_col'],
                'old': self.df.at[idx, params['target_col']],
                'new': merged_df.at[idx, new_values_col]
            })

        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action)
            self.apply_action(action, is_undo=False)
            self.show_operation_status(f"{len(changes)}件のセルを参照置換しました")
    
    def open_extract_dialog(self):
        if self.df is None:
            messagebox.showwarning("情報", "ファイルを開いてから実行してください。")
            return
        
        dialog = SearchReplaceDialog(self, self.header, self.theme, mode='extract')
        self.wait_window(dialog)
        
        if dialog.result and dialog.result.get('action') == 'extract':
            indices = dialog.result.get('indices', [])
            if indices:
                self._extract_rows_to_new_window(indices)

    def _extract_rows_to_new_window(self, indices):
        if not indices:
            messagebox.showinfo("情報", "抽出対象の行がありません。")
            return
        
        new_df = self.df.loc[indices].copy().reset_index(drop=True)
        
        self.show_operation_status(f"{len(indices)}行を新しいウィンドウに抽出しました")
        new_window = CsvEditorApp(dataframe=new_df, title=f"抽出結果 ({len(new_df)}行)")

    def populate_list_view(self, start_index=0, end_index=None):
        if self.df is None: return

        if start_index == 0 and end_index is None:
            self._clear_cell_selection()
            self.tree.delete(*self.tree.get_children())
        
        if end_index is None:
            end_index = min(start_index + self.VIRTUAL_LIST_CHUNK_SIZE, len(self.displayed_indices))

        self.tree["columns"] = self.header
        for idx, col_name in enumerate(self.header):
            col_id = f"#{idx+1}"
            self.tree.heading(col_id, text=col_name)
            self.tree.column(col_id, anchor=tk.W, width=120, minwidth=80)
        
        existing_items = set(self.tree.get_children())
        
        for i in range(start_index, end_index):
            if i >= len(self.displayed_indices): break
            
            original_index = self.displayed_indices[i]
            row_data = self.df.loc[original_index]
            tag = 'evenrow' if i % 2 == 0 else 'oddrow'
            values = [str(row_data.get(h, "")) for h in self.header]
            
            iid = str(original_index)
            if iid in existing_items:
                self.tree.item(iid, values=values, tags=(tag,))
            else:
                self.tree.insert("", "end", iid=iid, values=values, tags=(tag,))
        
        self._refresh_all_rows()
        
        if self.selected_column:
            self._highlight_column_header(self.selected_column)
        
        self._update_status_bar()
    
    def filter_data(self):
        if self.df is None: return
        if self.current_view == 'card': self.switch_view('list')
        
        search_term = self.filter_var.get().strip().lower()
        
        temp_df = self.df
        
        if search_term:
            try:
                mask = temp_df.apply(lambda x: x.astype(str).str.lower()).apply(
                    lambda x: x.str.contains(search_term, na=False)
                ).any(axis=1)
                temp_df = temp_df[mask]
                self.show_context_hint('filter_active')
            except Exception as e:
                print(f"Filter error: {e}")
        else:
            self.show_context_hint(None)

        if self.sort_column:
            try:
                temp_df = temp_df.sort_values(by=self.sort_column, ascending=not self.sort_reverse)
            except Exception as e:
                print(f"Sort error: {e}")

        self.displayed_indices = temp_df.index.tolist()
        self.populate_list_view()

    def delete_selected_rows(self):
        if self.current_view == 'card': messagebox.showinfo("情報", "行の削除は一覧表示モードで行ってください。"); return
        if not self.selected_cells: messagebox.showinfo("情報", "削除する行を含む行を選択してください。"); return
        
        original_indices_to_delete = sorted(list({int(item) for item, col in self.selected_cells}), reverse=True)
        
        if not messagebox.askyesno("確認", f"{len(original_indices_to_delete)}件の行を削除しますか？"): return

        deleted_data = self.df.loc[original_indices_to_delete].copy()
        
        action = {'type': 'delete_rows', 'data': deleted_data}
        self.undo_manager.add_action(action)
        self.apply_action(action, is_undo=False)

    def add_column(self):
        if not self.active_cell:
            messagebox.showinfo("情報", "列を追加する基準となるセルを選択してください。")
            return

        new_col_name = simpledialog.askstring("新しい列の作成", "新しい列の名前を入力してください:", parent=self)
        if not new_col_name:
            return
        if new_col_name in self.header:
            messagebox.showerror("エラー", f"列名 '{new_col_name}' は既に存在します。")
            return

        _, col_index = self.active_cell
        insert_pos = col_index + 1

        action = {'type': 'add_column', 'data': {'name': new_col_name, 'position': insert_pos}}
        self.undo_manager.add_action(action)
        self.apply_action(action, is_undo=False)

    def _adjust_column_widths(self):
        for idx, col_name in enumerate(self.header):
            col_id = f"#{idx+1}"
            header_width = font.Font().measure(col_name) + 20
            
            items = self.tree.get_children('')
            if not items:
                max_content_width = header_width
            else:
                max_content_width = max(
                    [font.Font().measure(self.tree.set(item, col_id)) for item in items] + [header_width]
                )

            final_width = min(max(120, max_content_width + 20), 400)
            self.tree.column(col_id, width=final_width)

    def _recreate_card_view_entries(self):
        for widget in self.card_scrollable_frame.winfo_children():
            if not isinstance(widget, ttk.Button): widget.destroy()
        
        button_frame = next((c for c in self.card_scrollable_frame.winfo_children() if isinstance(c, ttk.Frame)), None)
        
        self.card_entries.clear()
        for i, field in enumerate(self.header):
            row_frame = ttk.Frame(self.card_scrollable_frame)
            row_frame.pack(fill=tk.X, padx=10, pady=2)
            ttk.Label(row_frame, text=f"{field}:", width=20, anchor=tk.W).pack(side=tk.LEFT)
            entry = ttk.Entry(row_frame); entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
            self.card_entries[field] = entry
        
        if button_frame: button_frame.pack_forget(); button_frame.pack(fill=tk.X, pady=20, padx=10)

    def add_row(self):
        self.switch_view('list')
        
        new_row = {h: "" for h in self.header}
        new_df_row = pd.DataFrame([new_row])
        
        action = {'type': 'add_row', 'data': new_df_row}
        self.undo_manager.add_action(action)
        self.apply_action(action, is_undo=False)
        self._set_active_cell_by_view_index(0,0)

    def sort_by_column(self, col_id):
        try: col_name = self.header[int(col_id.replace('#','')) - 1]
        except (ValueError, IndexError): return
        if self.sort_column == col_name: self.sort_reverse = not self.sort_reverse
        else: self.sort_column = col_name; self.sort_reverse = False
        self.filter_data()

    def _update_status_bar(self):
        if self.df is None:
            self.status_basic.set("ファイルを開いてください。")
            return
            
        total_rows = len(self.df)
        displayed_rows = len(self.displayed_indices)

        status_text = f"表示中: {displayed_rows} / {total_rows} 件"
        
        if self.filepath:
            detected_format = self.csv_format_manager.current_format
            format_info = "クォート付き" if detected_format.get('detected_has_quotes', False) else "クォートなし"
            status_text += f" | ファイル: {os.path.basename(self.filepath)} ({format_info}, {self.encoding})"
        
        if '価格' in self.header:
            try:
                prices = pd.to_numeric(self.df.loc[self.displayed_indices, '価格'], errors='coerce').dropna()
                if not prices.empty:
                    status_text += f" | '価格'の合計: {prices.sum():,.0f}"
            except Exception:
                pass
        
        self.status_basic.set(status_text)
        
    def switch_view(self, view_name):
        self.current_view = view_name
        if view_name == 'list':
            self._unbind_card_navigation()
            self.card_view_frame.grid_remove()
            self.list_view_frame.grid()
            self.view_toggle_button.config(text="カードで編集", command=self.open_card_view)
            self.tree.focus_set()
            self._refresh_all_rows()
        elif view_name == 'card':
            self._bind_card_navigation()
            self.list_view_frame.grid_remove()
            self.card_view_frame.grid()
            self.view_toggle_button.config(text="一覧に戻る", command=lambda: self.switch_view('list'))
            self._update_card_navigation_status()
            
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

    def _populate_card_view(self, display_index):
        if display_index >= len(self.displayed_indices): return
        
        self.card_current_original_index = self.displayed_indices[display_index]
        row_data = self.df.loc[self.card_current_original_index]

        if row_data is None: return
        for field, entry_widget in self.card_entries.items():
            entry_widget.delete(0, tk.END); entry_widget.insert(0, row_data.get(field, ""))
        if self.header: self.card_entries[self.header[0]].focus_set()

    def update_from_card(self):
        if self.card_current_original_index is None: return
        
        original_data = self.df.loc[self.card_current_original_index].to_dict()
        new_data_dict = {field: entry.get() for field, entry in self.card_entries.items()}
        
        item_id = str(self.card_current_original_index)
        changes = [{'item': item_id, 'column': col, 'old': original_data.get(col, ""), 'new': new_val} 
                   for col, new_val in new_data_dict.items() if original_data.get(col, "") != new_val]
        
        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action)
            self.apply_action(action, is_undo=False)
        self.switch_view('list')
        
    def _on_closing(self):
        if hasattr(self.cell_editor, 'edit_entry') and self.cell_editor.edit_entry:
            if not messagebox.askokcancel("確認", "編集中のセルがあります。変更を破棄して終了しますか？"): return
        if self.df is not None and self.undo_manager.can_undo():
             if not messagebox.askokcancel("確認", "未保存の変更があります。変更を破棄して終了しますか？"): return
        if messagebox.askokcancel("終了", "アプリケーションを終了しますか？"): self.destroy()

    def apply_action(self, action, is_undo):
        action_type, data = action['type'], action['data']
        
        if action_type == 'edit':
            for change in data:
                val = change['old'] if is_undo else change['new']
                row_index = int(change['item'])
                self.df.at[row_index, change['column']] = val
        elif action_type == 'add_row':
            if is_undo:
                self.df.drop(index=data.index, inplace=True)
            else:
                self.df = pd.concat([data, self.df]).reset_index(drop=True)
        elif action_type == 'delete_rows':
            if is_undo:
                self.df = pd.concat([self.df, data]).sort_index()
            else:
                self.df.drop(index=data.index, inplace=True)
        elif action_type == 'add_column':
            col_name = data['name']
            if is_undo:
                self.df.drop(columns=[col_name], inplace=True)
            else:
                pos = data['position']
                self.df.insert(pos, col_name, "")
            self.header = list(self.df.columns)
        elif action_type == 'delete_column':
            col_name = data['column_name']
            col_position = data['position']
            deleted_series = data['deleted_data']
            if is_undo:
                self.df.insert(col_position, col_name, deleted_series)
            else:
                self.df.drop(columns=[col_name], inplace=True)
            self.header = list(self.df.columns)
            if self.selected_column == col_name:
                self.selected_column = None
        elif action_type == 'merge_cells':
            row_index = int(data['item'])
            if is_undo:
                self.df.at[row_index, data['target_column']] = data['target_old']
                self.df.at[row_index, data['empty_column']] = data['empty_old']
            else:
                self.df.at[row_index, data['target_column']] = data['merged_value']
                self.df.at[row_index, data['empty_column']] = ""
        elif action_type == 'merge_columns':
            if is_undo:
                for change in data['changes']:
                    row_index = change['row_index']
                    self.df.at[row_index, change['target_column']] = change['target_old']
                    self.df.at[row_index, change['empty_column']] = change['empty_old']
            else:
                for change in data['changes']:
                    row_index = change['row_index']
                    self.df.at[row_index, change['target_column']] = change['merged_value']
                    self.df.at[row_index, change['empty_column']] = ""

        self.filter_data()
        self._update_status_bar()

    def _select_all(self, event=None):
        if self.current_view != 'list': return "break"
        self._clear_cell_selection()
        for iid in self.tree.get_children():
            for c_idx, col_name in enumerate(self.header):
                self._select_cell(iid, f"#{c_idx + 1}", col_name)
        return "break"

    def _undo(self, event=None): self.undo_manager.undo(); return "break"
    def _redo(self, event=None): self.undo_manager.redo(); return "break"

    def _copy(self, event=None):
        if self.current_view != 'list': return "break"
        selected = self.get_selected_cell_data()
        if selected:
            self.clipboard_manager.copy_cells_to_clipboard(self, selected)
            self.show_operation_status(f"{len(selected)}個のセルをコピーしました")
        return "break"

    def _cut(self, event=None):
        self._copy(event); self._delete_selected(event); return "break"

    def _paste(self, event=None):
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
        
        self.column_clipboard = self.df[self.selected_column].tolist()
        self.show_operation_status(f"列「{self.selected_column}」をコピーしました")

    def paste_to_selected_column(self, event=None):
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
(省略)
"""
        messagebox.showinfo("ショートカットキー", help_text, parent=self)
        return "break"

    def test_data(self):
        """テストデータを表示"""
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