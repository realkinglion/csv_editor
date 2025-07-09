# main_qt.py (コントローラー分割後の最終修正版)

import sys
import os
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QDialog,
    QVBoxLayout, QHBoxLayout, QGridLayout, QFormLayout,
    QLineEdit, QTextEdit, QPlainTextEdit, QComboBox, QCheckBox, QRadioButton,
    QSpinBox, QDoubleSpinBox, QPushButton,
    QLabel, QMessageBox, QProgressBar, QTableView, QListWidget,
    QGroupBox, QScrollArea, QDockWidget, QButtonGroup,
    QFileDialog, QInputDialog, QProgressDialog, QDialogButtonBox,
    QHeaderView, QAbstractItemView, QStyle, QMenu, QSizePolicy,
    QDataWidgetMapper, QToolBar
)
from PySide6.QtGui import QKeySequence, QGuiApplication, QTextOption, QFont, QAction, QPalette, QIcon
from PySide6.QtCore import Qt, Signal, Slot, QTimer, QModelIndex, QEvent, QItemSelectionModel, QObject, QItemSelection, QSize, QUrl, QPropertyAnimation

import config
import pandas as pd
import csv
import re
import traceback
import math
from io import StringIO

from themes_qt import ThemeQt, DarkThemeQt
from data_model import CsvTableModel
from db_backend import SQLiteBackend
from lazy_loader import LazyCSVLoader

# 個別インポートで問題を特定
try:
    from features import AsyncDataManager
    print("✅ AsyncDataManager imported")
except ImportError as e:
    print(f"❌ AsyncDataManager import failed: {e}")
    sys.exit(1)

try:
    from features import UndoRedoManager
    print("✅ UndoRedoManager imported")
except ImportError as e:
    print(f"❌ UndoRedoManager import failed: {e}")
    sys.exit(1)

try:
    from features import ParentChildManager
    print("✅ ParentChildManager imported")
except ImportError as e:
    print(f"❌ ParentChildManager import failed: {e}")
    # 緊急フォールバック
    from PySide6.QtCore import QObject, Signal
    
    class ParentChildManager(QObject):
        analysis_completed = Signal(str)
        analysis_error = Signal(str)
        
        def __init__(self):
            super().__init__()
            self.parent_child_data = {}
            self.current_group_column = None
            self.df = None
            self.db_backend = None
            print("WARNING: Using fallback ParentChildManager")

from search_widget import SearchWidget

# コントローラーのインポート
from file_io_controller import FileIOController
from view_controller import ViewController
from search_controller import SearchController
from table_operations import TableOperationsManager

# dialogs.py から必要なダイアログクラスをインポート
from dialogs import (
    MergeSeparatorDialog, PriceCalculatorDialog, PasteOptionDialog,
    CSVSaveFormatDialog, TooltipEventFilter, EncodingSaveDialog,
    TextProcessingDialog, RemoveDuplicatesDialog
)

from ui_main_window import Ui_MainWindow

# 既存のimport文の後に追加
from settings_manager import SettingsManager

# ローディングオーバーレイのインポート
from loading_overlay import LoadingOverlay


class CsvEditorAppQt(QMainWindow):
    """
    アプリケーションのメインロジックを担当するクラス。
    UIの定義はUi_MainWindowクラスから継承する。
    """
    data_fetched = Signal(pd.DataFrame)
    create_extract_window_signal = Signal(pd.DataFrame)
    progress_bar_update_signal = Signal(int)

    # ファイル読み込み開始・進捗・終了シグナル
    # AsyncDataManagerからemitされ、_show_loading_overlay等に接続
    file_loading_started = Signal()
    file_loading_progress = Signal(str, int, int)
    file_loading_finished = Signal()

    def __init__(self, dataframe=None, parent=None, filepath=None, encoding='shift_jis'):
        super().__init__(parent)

        print(f"DEBUG: CsvEditorAppQt 初期化開始")
        print(f"DEBUG: parent = {parent}")
        print(f"DEBUG: self.parent() = {self.parent()}")
        print(f"  - dataframe: {dataframe.shape if dataframe is not None else 'None'}")
        print(f"  - filepath: {filepath}")
        print(f"  - encoding: {encoding}")

        # `setupUi` の完了フラグを追加 (file_io_controller._is_welcome_screen_active で使用)
        self.main_window_is_initialized = False

        # 🔧 ここから追加：コマンドライン引数の処理
        # filepathが指定されていない場合、コマンドライン引数をチェック
        if filepath is None:
            print(f"DEBUG: コマンドライン引数をチェック中...")
            print(f"DEBUG: sys.argv = {sys.argv}")

            # sys.argv[0]はプログラム名、sys.argv[1]以降が引数
            # 🔧 複数ファイル引数の基本対応
            all_file_args = []
            if len(sys.argv) > 1:
                # 最初の引数以降がファイルパス候補
                for arg in sys.argv[1:]:
                    if os.path.exists(arg) and arg.lower().endswith(('.csv', '.txt')):
                        all_file_args.append(arg)

                if all_file_args:
                    print(f"DEBUG: 受信したファイル数: {len(all_file_args)}")
                    # 最初のファイルは現在のウィンドウで開く
                    filepath = all_file_args[0]

                    # 複数ファイルを扱うためのリストを保持
                    self.multi_file_list = all_file_args
                else:
                    self.multi_file_list = []
                    print(f"DEBUG: コマンドライン引数に有効なCSVファイルなし")
            else:
                self.multi_file_list = []
                print(f"DEBUG: コマンドライン引数なし（通常起動）")
        # 🔧 ここまで追加

        self.filepath = filepath
        self.encoding = encoding

        # 🔥 修正1: table_model の初期化を UI セットアップより前に移動し、コメントアウトを解除
        self.theme = config.CURRENT_THEME
        self.density = config.CURRENT_DENSITY

        self._df = dataframe # _df は CsvTableModel のコンストラクタに渡される
        self.header = list(self._df.columns) if self._df is not None and not self._df.empty else [] # ヘッダーも初期化時に設定

        # CsvTableModel の初期化（最重要）
        self.table_model = CsvTableModel(self._df, self.theme) # コメントアウトを解除
        self.table_model.set_app_instance(self) # コメントアウトを解除

        # UIのセットアップ (Ui_MainWindow の setupUi 内で table_view や card_mapper が使われるが、
        # それらは `setModel` や `addMapping` で `table_model` を参照するため、
        # `table_model` は `setupUi` 呼び出し前に初期化されている必要がある)
        ui = Ui_MainWindow()
        ui.setupUi(self)

        # UI要素の存在確認と手動作成（ui_main_window.pyがない場合のフォールバック）
        essential_attrs = ['table_view', 'welcome_widget', 'status_label',
                           'progress_bar', 'card_scroll_area', 'operation_label',
                           'new_action', 'open_action', 'save_action', 'save_as_action', 'exit_action',
                           'new_file_button_welcome', 'open_file_button_welcome',
                           'sample_data_button_welcome',
                           'undo_action', 'redo_action', 'cut_action', 'copy_action',
                           'paste_action', 'delete_action', 'cell_concatenate_action',
                           'column_concatenate_action', 'copy_column_action', 'paste_column_action',
                           'add_row_action', 'add_column_action', 'delete_selected_rows_action',
                           'delete_selected_column_action', 'sort_asc_action', 'sort_desc_action',
                           'clear_sort_action', 'select_all_action', 'search_action',
                           'price_calculator_action', 'save_format_action', 'shortcuts_action',
                           'view_toggle_action', 'test_action', 'prev_record_button', 'next_record_button',
                           'edit_menu', 'tools_menu', 'csv_format_menu', 'view_stack',
                           'card_view_container', 'welcome_label',
                           'text_processing_action', 'diagnose_action', 'force_show_action',
                           'remove_duplicates_action'
                           ]

        missing_attrs = []
        for attr in essential_attrs:
            if not hasattr(self, attr):
                missing_attrs.append(attr)

        if missing_attrs:
            print(f"警告: 以下の必須UI要素がui_main_window.pyで定義されていません: {missing_attrs}")
            print("これは予期しない挙動を引き起こす可能性があります。ui_main_window.pyを確認してください。")
            # 最低限のフォールバック (ただし、ui_main_window.pyの完全な定義が推奨される)
            if not hasattr(self, 'table_view'): self.table_view = QTableView()
            if not hasattr(self, 'status_label'): self.status_label = QLabel("準備完了")
            if not hasattr(self, 'progress_bar'): self.progress_bar = QProgressBar()
            if not hasattr(self, 'card_scroll_area'): self.card_scroll_area = QScrollArea()
            if not hasattr(self, 'operation_label'): self.operation_label = QLabel()
            if not hasattr(self, 'view_stack'):
                self.view_stack = QWidget()
                self.setCentralWidget(self.view_stack)
                self.view_stack_layout = QVBoxLayout(self.view_stack)
                self.view_stack_layout.setContentsMargins(0,0,0,0)
                self.view_stack_layout.addWidget(self.table_view)
                if not hasattr(self, 'card_scroll_area'):
                    self.view_stack_layout.addWidget(self.card_scroll_area)
            if not hasattr(self, 'welcome_widget'):
                self.welcome_widget = QWidget()
                self.main_layout.addWidget(self.welcome_widget)
            if not hasattr(self, 'card_view_container'):
                self.card_view_container = QWidget()
                self.card_view_container.setLayout(QFormLayout())
                self.card_scroll_area.setWidget(self.card_view_container)
                self.card_scroll_area.setWidgetResizable(True)
            if not hasattr(self, 'welcome_label'): self.welcome_label = QLabel("Welcome")
            for attr in [a for a in essential_attrs if 'action' in a or 'menu' in a]:
                if not hasattr(self, attr):
                    setattr(self, attr, QAction(self) if 'action' in attr else QMenu(self))
            for attr in ['new_file_button_welcome', 'open_file_button_welcome', 'sample_data_button_welcome']:
                if not hasattr(self, attr):
                    setattr(self, attr, QPushButton(self))

        self.main_window_is_initialized = True # setupUi 完了フラグを設定
        
        self.lazy_loader = None
        self.db_backend = None
        self.performance_mode = False

        self.sort_info = {'column_index': -1, 'order': Qt.AscendingOrder}

        # コントローラーの初期化 (これらは CsvTableModel の後で初期化する必要がある)
        self.file_controller = FileIOController(self)
        self.view_controller = ViewController(self)
        self.search_controller = SearchController(self)
        self.async_manager = AsyncDataManager(self)
        self.table_operations = TableOperationsManager(self)
        
        # 子ウィンドウ管理用のリストを初期化
        self.child_windows = []

        self.undo_manager = UndoRedoManager(self)
        self.parent_child_manager = ParentChildManager()

        self.search_dock_widget = None
        self.search_panel = None

        self.pulse_timer = QTimer(self)
        self.pulse_timer.setSingleShot(True)
        self.pulsing_cells = set()

        # card_mapper の初期化は table_model の後
        self.card_mapper = QDataWidgetMapper(self)
        self.card_mapper.setModel(self.table_model) # table_model がここで確実に存在する
        self.card_fields_widgets = {}

        self.settings_manager = SettingsManager()

        self.operation_timer = None
        self.progress_dialog = None

        # ローディングオーバーレイの作成と初期非表示
        self.loading_overlay = LoadingOverlay(self)
        self.loading_overlay.hide()

        # table_view にモデルを設定（これも table_model 初期化後）
        self.table_view.setModel(self.table_model)
        self.table_view.verticalHeader().setDefaultSectionSize(self.density['row_height'])

        self.last_selected_index = QModelIndex()
        self.active_index = QModelIndex()
        self.dragging = False

        # キーイベントフィルターをインストール
        self.installEventFilter(self)

        self._connect_signals()
        self._connect_controller_signals()
        self._create_search_dock_widget()
        self.search_dock_widget.hide()

        self.apply_theme()
        self._set_application_icon()
        self._set_default_font()

        # アプリケーションの起動時の状態に応じて初期表示を決定
        if dataframe is not None:
            # 新規データとして初期化された場合 (open_new_window_with_new_data から呼ばれる)
            self.view_stack.show()
            self.welcome_widget.hide()
            self.view_controller.show_main_view() # メインビューを表示
            self.table_model.set_dataframe(dataframe) # データフレームを設定
            self.status_label.setText(f"新規ファイル ({len(dataframe):,}行, {len(dataframe.columns)}列)") # ステータスバーを更新
            self.setWindowTitle(f"高機能CSVエディタ (PySide6) - 無題") # ウィンドウタイトルを更新
            self.table_view.resizeColumnsToContents() # 列幅を調整
            self._set_ui_state('normal') # UI状態を設定
            self.view_controller.recreate_card_view_fields() # カードビューを再作成
        elif self.filepath and os.path.exists(self.filepath):
            # コマンドライン引数でファイルが指定された場合 (メインウィンドウで開く)
            print(f"DEBUG: ファイル自動読み込みを開始: {self.filepath}")

            self.view_stack.hide()
            self.welcome_widget.hide() # Welcome画面は隠す

            self.status_label.setText(f"ファイル読み込み中: {os.path.basename(self.filepath)}")
            self.setWindowTitle(f"高機能CSVエディタ (PySide6) - {os.path.basename(self.filepath)} 読み込み中...")

            # コマンドライン引数での自動読み込み時にもモード選択ダイアログを表示する
            # file_io_controller._start_file_loading_process がファイルロードを処理
            QTimer.singleShot(100, lambda: self._auto_open_file_with_dialog(self.filepath))
        else:
            # 通常起動でファイルが指定されていない場合 (ウェルカム画面表示)
            self.view_stack.hide()
            self.welcome_widget.show()
            self.setWindowTitle("高機能CSVエディタ (PySide6) - ファイルを開いてください。") # ウィンドウタイトルを更新

        self.settings_manager.load_window_settings(self)
        self.settings_manager.load_toolbar_state(self)

        # 🔥 修正5: 初期化検証の追加
        if not self._validate_initialization():
            print("ERROR: アプリケーションの初期化に失敗しました。終了します。")
            sys.exit(1)

        print(f"DEBUG: 初期化完了後の状態:")
        print(f"  - view_stack.isVisible(): {self.view_stack.isVisible()}")
        print(f"  - welcome_widget.isVisible(): {self.welcome_widget.isVisible()}")
        print(f"  - table_view.isVisible(): {self.table_view.isVisible()}")

    def _set_application_icon(self):
        """アプリケーションアイコンを設定"""
        if getattr(sys, 'frozen', False):
            base_path = sys._MEIPASS
        else:
            base_path = os.path.dirname(os.path.abspath(__file__))
        
        icon_path = os.path.join(base_path, 'icon_256x256.ico')
        
        if os.path.exists(icon_path):
            app_icon = QIcon(icon_path)
            self.setWindowIcon(app_icon)
            print(f"DEBUG: アイコンを設定しました: {icon_path}")
        else:
            print(f"WARNING: アイコンファイルが見つかりません: {icon_path}")

    # ドラッグ＆ドロップイベントハンドラの追加
    def dragEnterEvent(self, event):
        """ドラッグされたアイテムがCSVファイルかチェック"""
        if event.mimeData().hasUrls():
            urls = event.mimeData().urls()
            if urls:
                first_file = urls[0].toLocalFile()
                if first_file.lower().endswith(('.csv', '.txt')):
                    event.acceptProposedAction()
                    # 🔥 改善: ドラッグ中のウェルカム画面のスタイルを変化させる
                    if self.welcome_widget.isVisible():
                        self.welcome_label.setStyleSheet("""
                            QLabel {
                                background-color: #E8F4FD;
                                border: 2px dashed #2196F3;
                                border-radius: 8px;
                                padding: 20px;
                            }
                        """)
                else:
                    event.ignore()
        else:
            event.ignore()

    def dragLeaveEvent(self, event):
        """ドラッグがウィンドウから離れたときの処理"""
        # 🔥 改善: ドラッグ終了時のウェルカム画面のスタイルを元に戻す
        if self.welcome_widget.isVisible():
            self.welcome_label.setStyleSheet("")
        event.accept()

    def dropEvent(self, event):
        """CSVファイルがドロップされたときの処理（ウェルカム画面考慮版）"""
        # 🔥 改善: ドロップ後のウェルカム画面スタイルを元に戻す
        if self.welcome_widget.isVisible():
            self.welcome_label.setStyleSheet("")

        if event.mimeData().hasUrls():
            urls = event.mimeData().urls()
            if urls:
                filepath = urls[0].toLocalFile()
                
                # ファイル存在チェックはfile_controller._start_file_loading_process内で行われるためここでは不要だが、
                # エラーメッセージを即座に出したい場合はここに含める。
                # ただし、open_file / _start_file_loading_process 側で一元的に行う方が良い。
                # ここでは file_io_controller.open_file に委譲するため、そちらでエラーハンドリングされる。

                if filepath.lower().endswith(('.csv', '.txt')):
                    print(f"DEBUG: ファイルがドロップされました: {filepath}")
                    
                    # 🔥 修正のポイント：ウェルカム画面の状態を考慮して FileIOController に委譲
                    # FileIOController.open_file は引数つきで呼ばれた場合、現在のウィンドウで開く
                    # 引数なしで呼ばれた場合（ダイアログ選択時）は、FileIOController内でウェルカム画面考慮の分岐が行われる
                    self.file_controller.open_file(filepath) # filepath を引数として渡す
                    
                    event.acceptProposedAction()
                else:
                    QMessageBox.warning(
                        self, "無効なファイル",
                        "CSVファイル(.csv)またはテキストファイル(.txt)をドロップしてください。"
                    )
                    event.ignore()
        else:
            event.ignore()
    # ドラッグ＆ドロップイベントハンドラの追加ここまで

    def _connect_controller_signals(self):
        self.file_controller.file_loaded.connect(self._on_file_loaded)
        self.file_controller.file_saved.connect(self._on_file_saved)
        self.file_controller.load_mode_changed.connect(self._on_load_mode_changed)
        self.view_controller.view_changed.connect(self._on_view_changed)
        self.view_controller.context_hint_changed.connect(self._on_context_hint_changed)

        self.async_manager.search_results_ready.connect(self.search_controller.handle_search_results_ready)
        self.async_manager.analysis_results_ready.connect(self._on_parent_child_analysis_ready)
        self.async_manager.replace_from_file_completed.connect(self._on_replace_from_file_completed)
        self.async_manager.product_discount_completed.connect(self._on_product_discount_completed)
        self.async_manager.bulk_extract_completed.connect(self._on_bulk_extract_completed)

    def _connect_signals(self):
        # QActionの接続
        self.new_action.triggered.connect(self.file_controller.create_new_file)
        self.open_action.triggered.connect(self.file_controller.open_file)
        self.save_action.triggered.connect(lambda: self.file_controller.save_file(filepath=self.filepath, is_save_as=False))
        self.save_as_action.triggered.connect(self.file_controller.save_as_with_dialog)
        self.exit_action.triggered.connect(self.close)

        # ウェルカム画面のQPushButtonの接続
        if hasattr(self, 'new_file_button_welcome') and self.new_file_button_welcome is not None:
            # 🔥 修正のポイント：ウェルカム画面の新規作成ボタンも file_io_controller に委譲
            self.new_file_button_welcome.clicked.connect(self.file_controller.create_new_file)
        if hasattr(self, 'open_file_button_welcome') and self.open_file_button_welcome is not None:
            # 🔥 修正のポイント：ウェルカム画面の開くボタンも file_io_controller に委譲
            self.open_file_button_welcome.clicked.connect(self.file_controller.open_file)
        if hasattr(self, 'sample_data_button_welcome') and self.sample_data_button_welcome is not None:
            self.sample_data_button_welcome.clicked.connect(self.test_data)

        self.async_manager.data_ready.connect(self._on_async_data_ready)
        self.async_manager.task_progress.connect(self._update_progress_dialog)

        self.create_extract_window_signal.connect(self._create_extract_window_in_ui_thread)
        self.pulse_timer.timeout.connect(self._end_pulse)
        self.progress_bar_update_signal.connect(lambda v: self.progress_bar.setValue(v))

        self.table_view.horizontalHeader().sectionResized.connect(self._on_column_resized)

        self.undo_action.triggered.connect(self.undo_manager.undo)
        self.redo_action.triggered.connect(self.undo_manager.redo)
        # 変更: TableOperationsManager に処理を委譲
        self.cut_action.triggered.connect(self.table_operations.cut)
        self.copy_action.triggered.connect(self.table_operations.copy)
        self.paste_action.triggered.connect(self.table_operations.paste)
        self.delete_action.triggered.connect(self.table_operations.delete)
        self.cell_concatenate_action.triggered.connect(lambda: self.table_operations.concatenate_cells(is_column_merge=False))
        self.column_concatenate_action.triggered.connect(lambda: self.table_operations.concatenate_cells(is_column_merge=True))
        self.copy_column_action.triggered.connect(self.table_operations.copy_columns)
        self.paste_column_action.triggered.connect(self.table_operations.paste_columns)
        # 修正2: アクションを直接接続する代わりに、イベントフィルターで処理するように変更されたため、以下の行はそのままにしておくか、必要であればコメントアウトまたは削除を検討する。しかし、QActionがセットされている場合は、ここでの接続は残しておくのが適切。
        self.add_row_action.triggered.connect(self.table_operations.add_row)
        self.add_column_action.triggered.connect(self.table_operations.add_column)
        self.delete_selected_rows_action.triggered.connect(self.table_operations.delete_selected_rows)
        self.delete_selected_column_action.triggered.connect(self.table_operations.delete_selected_columns)
        self.sort_asc_action.triggered.connect(lambda: self._sort_by_column(Qt.AscendingOrder))
        self.sort_desc_action.triggered.connect(lambda: self._sort_by_column(Qt.DescendingOrder))
        self.clear_sort_action.triggered.connect(self._clear_sort)
        self.select_all_action.triggered.connect(self.table_operations.select_all)
        self.search_action.triggered.connect(self._toggle_search_panel)
        self.remove_duplicates_action.triggered.connect(self.table_operations.remove_duplicate_rows)

        self.price_calculator_action.triggered.connect(self._open_price_calculator)
        self.text_processing_action.triggered.connect(self._open_text_processing_tool)

        self.save_format_action.triggered.connect(self.file_controller.save_as_with_dialog)

        self.shortcuts_action.triggered.connect(self._show_shortcuts)

        self.diagnose_action.triggered.connect(self._diagnose_display_issue)

        self.view_toggle_action.triggered.connect(self.view_controller.toggle_view)
        self.test_action.triggered.connect(self.test_data)

        self.force_show_action.triggered.connect(self._emergency_show_table)

        self.table_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self._show_table_context_menu)
        self.table_view.horizontalHeader().setContextMenuPolicy(Qt.CustomContextMenu)
        self.table_view.horizontalHeader().customContextMenuRequested.connect(self._show_header_context_menu)
        self.table_view.selectionModel().currentChanged.connect(self._on_current_changed)
        self.table_view.horizontalHeader().sectionClicked.connect(self._on_column_header_clicked)
        self.table_view.verticalHeader().sectionClicked.connect(self._on_row_header_clicked)

        self.prev_record_button.clicked.connect(self.view_controller.go_to_prev_record)
        self.next_record_button.clicked.connect(self.view_controller.go_to_next_record)

        self.table_model.dataChanged.connect(self._on_model_data_changed)
        self.table_model.layoutChanged.connect(self._on_model_layout_changed)

        if hasattr(self, 'test_save_as_action'):
            self.test_save_as_action.triggered.connect(self._test_save_as_menu)

        # ファイル読み込み専用ローディングシグナルとスロットの接続
        self.file_loading_started.connect(self._show_loading_overlay)
        self.file_loading_progress.connect(self._update_loading_progress)
        self.file_loading_finished.connect(self._hide_loading_overlay)

    # 修正4: main_qt.pyのeventFilter調整
    def eventFilter(self, obj, event):
        """グローバルキーイベントの処理（カードビュー処理をview_controllerに移譲後）"""
        if obj == self and event.type() == QEvent.KeyPress:
            # カードビューでの矢印キー処理はview_controllerに移譲
            # （この部分を削除またはコメントアウト）
            # if self.view_controller.current_view == 'card':
            #     if event.modifiers() & Qt.ControlModifier:
            #         if event.key() == Qt.Key_Left:
            #             self.view_controller.go_to_prev_record()
            #             return True
            #         elif event.key() == Qt.Key_Right:
            #             self.view_controller.go_to_next_record()
            #             return True
            
            # その他のグローバルショートカット処理
            if event.modifiers() & Qt.ControlModifier:
                if event.key() == Qt.Key_Tab:
                    self.view_controller.toggle_view()
                    return True
                elif event.key() == Qt.Key_Plus or event.key() == Qt.Key_Equal:
                    if event.modifiers() & Qt.ShiftModifier:
                        self.table_operations.add_column()
                        return True
                    else:
                        self.table_operations.add_row()
                        return True
                elif event.key() == Qt.Key_Minus:
                    if event.modifiers() & Qt.ShiftModifier:
                        self.table_operations.delete_selected_columns()
                        return True
                    else:
                        self.table_operations.delete_selected_rows()
                        return True
                elif event.key() == Qt.Key_Up:
                    self._sort_by_column(Qt.AscendingOrder)
                    return True
                elif event.key() == Qt.Key_Down:
                    self._sort_by_column(Qt.DescendingOrder)
                    return True
                elif event.key() == Qt.Key_Backspace:
                    self._clear_sort()
                    return True
            
        return super().eventFilter(obj, event)

    def _create_search_dock_widget(self):
        if self.search_dock_widget is None:
            self.search_dock_widget = QDockWidget("検索・置換・抽出", self)
            self.search_dock_widget.setObjectName("SearchDockWidget")
            self.search_dock_widget.setAllowedAreas(Qt.LeftDockWidgetArea | Qt.RightDockWidgetArea)

            self.search_panel = SearchWidget(self.table_model._headers, self)
            self.search_dock_widget.setWidget(self.search_panel)
            self.addDockWidget(Qt.RightDockWidgetArea, self.search_dock_widget)

            self.search_panel.find_next_clicked.connect(self.search_controller.find_next)
            self.search_panel.find_prev_clicked.connect(self.search_controller.find_prev)
            self.search_panel.replace_one_clicked.connect(self.search_controller.replace_current)
            self.search_panel.replace_all_clicked.connect(self.search_controller.replace_all)
            self.search_panel.extract_clicked.connect(self.search_controller.execute_extract)

            self.search_panel.analysis_requested.connect(self._analyze_parent_child_from_widget)
            self.search_panel.replace_from_file_requested.connect(self._apply_replace_from_file)
            self.search_panel.product_discount_requested.connect(self._apply_product_discount)
            self.search_panel.bulk_extract_requested.connect(self._execute_bulk_extract)

    def _show_progress_dialog(self, title, on_cancel_slot):
        """
        既存のQProgressDialogを表示するメソッド。
        主にファイル読み込み以外の、AsyncDataManagerからの進捗表示に使用。
        """
        self._close_progress_dialog()
        self.progress_dialog = QProgressDialog(title, "キャンセル", 0, 100, self)
        self.progress_dialog.setWindowModality(Qt.WindowModal)
        self.progress_dialog.setMinimumDuration(0)
        self.progress_dialog.setValue(0)
        self.progress_dialog.setAutoClose(False)
        self.progress_dialog.setAutoReset(True)
        if on_cancel_slot:
            def handle_cancel():
                print("DEBUG: プログレスダイアログがキャンセルされました")
                if hasattr(self.async_manager, 'is_cancelled'):
                    self.async_manager.is_cancelled = True
                on_cancel_slot()
            self.progress_dialog.canceled.connect(handle_cancel)
        else:
            self.progress_dialog.setCancelButton(None)
        self.progress_dialog.show()
        QApplication.processEvents()

    @Slot(str, int, int)
    def _update_progress_dialog(self, status, current, total):
        """
        既存のQProgressDialogの進捗を更新するメソッド。
        AsyncDataManagerのtask_progressシグナルに接続される。
        ファイル読み込み時以外（検索、分析、保存など）の進捗表示に使用。
        """
        print(f"DEBUG: Progress update (QProgressDialog) - Status: {status}, Current: {current}, Total: {total}")

        if self.progress_dialog is None: return
        self.progress_dialog.setLabelText(status)
        if total == 0:
            self.progress_dialog.setMaximum(0)
            self.progress_dialog.setValue(0)
        else:
            if self.progress_dialog.maximum() != total:
                self.progress_dialog.setMaximum(total)
            self.progress_dialog.setValue(current)

        if current >= total and total > 0:
            print("DEBUG: Progress 100% - closing dialog")
            QTimer.singleShot(100, self._close_progress_dialog)
        QApplication.processEvents()

    def _close_progress_dialog(self):
        """
        既存のQProgressDialogを閉じるメソッド。
        AsyncDataManagerからのclose_progress_requestedシグナルに接続される。
        """
        if hasattr(self, 'loading_overlay') and self.loading_overlay is not None:
            try:
                self.loading_overlay.hide()
            except Exception as e:
                print(f"Warning: ローディングオーバーレイ非表示エラー: {e}")

        if hasattr(self, 'progress_dialog') and self.progress_dialog is not None:
            try:
                self.progress_dialog.close()
                self.progress_dialog = None
            except Exception as e:
                print(f"Warning: プログレスダイアログクローズエラー: {e}")

    @Slot()
    def _show_loading_overlay(self):
        """
        ローディングオーバーレイを表示する（ファイル読み込み専用）。
        file_loading_startedシグナルに接続。
        """
        self.loading_overlay.resize(self.size())
        self.loading_overlay.raise_()
        self.loading_overlay.show()
        QApplication.processEvents()

    @Slot()
    def _hide_loading_overlay(self):
        """
        ローディングオーバーレイを非表示にする（ファイル読み込み専用）。
        file_loading_finishedシグナルに接続。
        """
        if not hasattr(self, 'loading_overlay') or not self.loading_overlay.isVisible():
            return

        try:
            fade_out = QPropertyAnimation(self.loading_overlay, b"windowOpacity")
            fade_out.setDuration(300)
            fade_out.setStartValue(1.0)
            fade_out.setEndValue(0.0)
            fade_out.finished.connect(self.loading_overlay.hide)
            fade_out.start()
        except Exception as e:
            print(f"Warning: フェードアウトアニメーションエラー: {e}")
            self.loading_overlay.hide()

    @Slot(str, int, int)
    def _update_loading_progress(self, status, current, total):
        """
        ローディングオーバーレイの進捗を更新する（ファイル読み込み専用）。
        file_loading_progressシグナルに接続。
        """
        print(f"DEBUG: Progress update (LoadingOverlay) - Status: {status}, Current: {current}, Total: {total}")

        self.loading_overlay.set_status(status)
        if total > 0:
            self.loading_overlay.show_progress(True)
            self.loading_overlay.set_progress(current, total)
        else:
            self.loading_overlay.show_progress(False)

    @Slot(object, str, str)
    def _on_file_loaded(self, data_object, filepath, encoding):
        """
        file_io_controller.file_loadedシグナルから呼び出される。
        データの読み込みとモデルへの設定、UIの初期化を行う。
        """
        print(f"DEBUG: _on_file_loaded: ファイル読み込み完了: {filepath}")

        if hasattr(self, 'loading_overlay'):
            self.loading_overlay.hide()

        if isinstance(data_object, pd.DataFrame):
            self._df = data_object
            self.table_model.set_dataframe(data_object)
            self.performance_mode = False
            total_rows = len(data_object)
        else:
            self.table_model.set_backend(data_object)
            self.performance_mode = True
            total_rows = data_object.get_total_rows()

            if hasattr(data_object, 'table_name'):
                self.db_backend = data_object
                self.lazy_loader = None
            else:
                self.lazy_loader = data_object
                self.db_backend = None

        self.filepath = filepath
        self.encoding = encoding
        self.header = list(data_object.columns) if isinstance(data_object, pd.DataFrame) else data_object.header

        self._set_ui_state('normal')

        self.welcome_widget.hide()
        self.view_controller.show_main_view()

        mode_text = "通常モード"
        if self.performance_mode:
            if self.db_backend:
                mode_text = "SQLiteモード"
            elif self.lazy_loader:
                mode_text = "遅延読み込みモード"
        
        status_text = f"{os.path.basename(filepath)} ({total_rows:,}行, {len(self.header)}列, {encoding}, {mode_text})"
        self.status_label.setText(status_text)
        if hasattr(self, 'multi_file_list') and self.multi_file_list:
            self._set_multi_file_title(self.multi_file_list)
            self.multi_file_list = []
        else:
            self.setWindowTitle(f"高機能CSVエディタ (PySide6) - {os.path.basename(filepath)}")

        if self.search_panel:
            self.search_panel.update_headers(self.header)

        self.view_controller.recreate_card_view_fields()
        self._clear_sort()

        if self.table_model.columnCount() < 50:
            self.table_view.resizeColumnsToContents()
        else:
            for i in range(min(10, self.table_model.columnCount())):
                self.table_view.resizeColumnToContents(i)

        if self.table_model.rowCount() > 0 and self.table_model.columnCount() > 0:
            first_index = self.table_model.index(0, 0)
            self.table_view.setCurrentIndex(first_index)
            self.table_view.scrollTo(first_index)

        self.show_operation_status("ファイルを読み込みました", 2000)

    @Slot(str)
    def _on_file_saved(self, filepath):
        print(f"DEBUG: _on_file_saved: ファイル保存完了: {filepath}")
        self.filepath = filepath
        if not self.windowTitle().startswith("楽天CSV編集ツール ("):
            self.setWindowTitle(f"高機能CSVエディタ (PySide6) - {os.path.basename(filepath)}")
        self.undo_manager.clear()
        self.update_menu_states()
        self.show_operation_status("ファイルを保存しました", 2000)

    @Slot(str)
    def _on_load_mode_changed(self, mode):
        """ロードモード変更時の処理"""
        print(f"DEBUG: _on_load_mode_changed: ロードモードが '{mode}' に変更されました。")

    @Slot(str)
    def _on_view_changed(self, view_type):
        """ビュー（テーブル/カード）が切り替わった時の処理"""
        print(f"DEBUG: _on_view_changed: ビューが {view_type} に切り替わりました")
        self._update_action_button_states()

    @Slot(str)
    def _on_context_hint_changed(self, hint_type):
        """コンテキストヒント変更時の処理"""
        print(f"DEBUG: _on_context_hint_changed: ヒントタイプが {hint_type} に変更されました。")

    @Slot(object)
    def _on_async_data_ready(self, df):
        """
        AsyncDataManagerからデータが準備完了したときに呼び出される。
        ファイル読み込み時のnormal modeでの最終処理、またはその他のデータ操作完了時に使用。
        """
        print(f"WARNING: _on_async_data_ready が呼ばれました（AsyncDataManagerからの直接データ受信）")
        print(f"DEBUG: DataFrame shape: {df.shape if df is not None else 'None'}")

        self._close_progress_dialog()
        if hasattr(self, 'loading_overlay') and self.loading_overlay.isVisible():
            self.loading_overlay.hide()
        self.progress_bar.hide()

        if hasattr(self.async_manager, 'is_cancelled') and self.async_manager.is_cancelled:
            self.show_operation_status("操作がキャンセルされました。", 3000)
            self.view_controller.show_welcome_screen()
            return

        if df is None or df.empty:
            error_msg = "読み込みに失敗したか、データが空です。"
            if hasattr(self.async_manager, 'last_error'):
                error_msg += f"\n詳細: {self.async_manager.last_error}"
            self.show_operation_status(error_msg, 5000, True)
            self.view_controller.show_welcome_screen()
            return

        load_mode = self.async_manager.current_load_mode
        self.performance_mode = (load_mode == 'sqlite' or load_mode == 'lazy')

        if load_mode == 'sqlite' and self.async_manager.backend_instance:
            self.db_backend = self.async_manager.get_backend_instance()
            self.table_model.set_backend(self.db_backend)
            self.header = self.db_backend.header
            total_rows = self.db_backend.get_total_rows()
        elif load_mode == 'lazy' and self.async_manager.backend_instance:
            self.lazy_loader = self.async_manager.get_backend_instance()
            self.table_model.set_backend(self.lazy_loader)
            self.header = self.lazy_loader.header
            total_rows = self.lazy_loader.get_total_rows()
        elif load_mode == 'normal':
            self._df = df
            self.table_model.set_dataframe(df)
            self.header = list(df.columns) if df is not None else []
            total_rows = len(df) if df is not None else 0
            self.performance_mode = False

        if self.search_panel: self.search_panel.update_headers(self.header)

        self.view_controller.recreate_card_view_fields()
        self._clear_sort()

        current_filepath = self.async_manager.current_filepath if hasattr(self.async_manager, 'current_filepath') else self.filepath or "不明なファイル"
        current_encoding = self.async_manager.current_encoding if hasattr(self.async_manager, 'current_encoding') else self.encoding or "不明"

        self.filepath = current_filepath
        self.encoding = current_encoding

        mode_text = "通常モード"
        if self.performance_mode:
            if self.db_backend:
                mode_text = "SQLiteモード"
            elif self.lazy_loader:
                mode_text = "遅延読み込みモード"
        
        status_text = f"{os.path.basename(self.filepath)} ({total_rows:,}行, {len(self.header)}列, {self.encoding}, {mode_text})"
        self.status_label.setText(status_text)
        if hasattr(self, 'multi_file_list') and self.multi_file_list:
            self._set_multi_file_title(self.multi_file_list)
            self.multi_file_list = []
        else:
            self.setWindowTitle(f"高機能CSVエディタ (PySide6) - {os.path.basename(self.filepath)}")

        self._set_ui_state('normal')
        self.view_controller.show_main_view()
        print("DEBUG: _on_async_data_ready finished.")

    def _set_multi_file_title(self, file_list):
        """複数ファイル時のウィンドウタイトル設定"""
        current_file = os.path.basename(self.filepath) if self.filepath else "不明"
        try:
            file_index = file_list.index(self.filepath) + 1 if self.filepath in file_list else 1
        except ValueError:
            file_index = 1
        total_files = len(file_list)

        self.setWindowTitle(f"楽天CSV編集ツール ({file_index}/{total_files}) - {current_file}")

    def test_data(self):
        """サンプルデータを作成して表示する（安全版）"""
        print("DEBUG: test_data button clicked.起動確認用")
        print("サンプルデータを作成中...")
        
        header = ["商品名", "価格", "在庫数", "カテゴリ", "商品説明"]
        sample_data = []

        for i in range(100):
            sample_data.append({
                "商品名": f"テスト商品{i+1:03d}",
                "価格": str(1000 + i * 100),
                "在庫数": str(50 - i % 10),
                "カテゴリ": "テストカテゴリ",
                "商品説明": f"<p>これはテスト商品{i+1}の説明文です。</p><br>HTMLタグも含まれています。"
            })

        df = pd.DataFrame(sample_data, columns=header)
        print(f"DEBUG: 作成したデータ: {len(df)}行, {len(df.columns)}列")

        # 🔥 修正のポイント：ウェルカム画面の状態を考慮して FileIOController に委譲
        if self.file_controller._is_welcome_screen_active():
            # ウェルカム画面の場合 → 既存ウィンドウで新規作成と同じフロー
            print("DEBUG: ウェルカム画面状態のため、既存ウィンドウでサンプルデータをロードします")
            self.file_controller._create_new_file_in_current_window(df)
        else:
            # 既存データがある場合 → 新しいウィンドウで新規作成として開く
            print("DEBUG: 既存データがあるため、新しいウィンドウでサンプルデータをロードします")
            # open_new_window_with_new_data は新規ファイル作成フローを模倣している
            self.open_new_window_with_new_data(df)


    def _set_ui_state(self, state):
        is_data_loaded = (state == 'normal')
        self.save_action.setEnabled(is_data_loaded)
        self.save_as_action.setEnabled(is_data_loaded)
        self.edit_menu.setEnabled(is_data_loaded)
        self.tools_menu.setEnabled(is_data_loaded)
        self.csv_format_menu.setEnabled(is_data_loaded)
        self.new_action.setEnabled(True) # 新規作成は常に可能
        self.open_action.setEnabled(True) # 開くは常に可能
        if is_data_loaded: self._update_action_button_states()

    def _set_default_font(self):
        font = QApplication.font()
        font_families = ["Yu Gothic UI", "Meiryo UI", "MS UI Gothic", "Segoe UI", "sans-serif"]
        for family in font_families:
            font.setFamily(family)
            if font.exactMatch():
                break
        font.setPointSize(self.density['font_size'])
        QApplication.setFont(font)

    def apply_theme(self):
        self.setStyleSheet(f"""
            * {{
                font-family: "Yu Gothic UI", "Meiryo UI", "MS UI Gothic", "Segoe UI", sans-serif;
            }}
            QMainWindow {{ background-color: {self.theme.BG_LEVEL_1}; }}
            QMenuBar {{
                background-color: {self.theme.BG_LEVEL_1};
                color: {self.theme.TEXT_PRIMARY};
            }}
            QMenuBar::item {{
                padding: 4px 8px;
                background: transparent;
            }}
            QHeaderView::section {{ background-color: {self.theme.BG_LEVEL_2}; color: {self.theme.TEXT_PRIMARY}; padding: 5px; font-weight: bold; }}
            QTableView {{ background-color: {self.theme.BG_LEVEL_0}; alternate-background-color: {self.theme.BG_LEVEL_1}; color: {self.theme.TEXT_PRIMARY}; gridline-color: {self.theme.BG_LEVEL_3}; border: 1px solid {self.theme.BG_LEVEL_3}; }}
            QTableView::item:selected {{ background-color: {self.theme.CELL_SELECT_START}; color: white; }}
            QStatusBar {{ background-color: {self.theme.BG_LEVEL_1}; color: {self.theme.TEXT_PRIMARY}; }}
            QLabel {{ color: {self.theme.TEXT_PRIMARY}; }}
            QPushButton {{ background-color: {self.theme.PRIMARY}; color: {self.theme.BG_LEVEL_0}; border: 1px solid {self.theme.PRIMARY}; padding: {self.density['padding']}px {self.density['padding'] * 2}px; border-radius: 4px; }}
            QPushButton:hover {{ background-color: {self.theme.PRIMARY_HOVER}; }}
            QPushButton:pressed {{ background-color: {self.theme.PRIMARY_ACTIVE}; }}
            QPushButton:disabled {{ background-color: {self.theme.BG_LEVEL_3}; color: {self.theme.TEXT_MUTED}; }}
            QToolBar {{
                background-color: {self.theme.BG_LEVEL_1};
                spacing: 5px;
                padding: 2px;
            }}
            QToolButton {{
                color: {self.theme.TEXT_PRIMARY};
                padding: 4px 8px;
                border: 1px solid transparent;
            }}
            QToolButton:hover {{
                background-color: {self.theme.BG_LEVEL_2};
                border: 1px solid {self.theme.BG_LEVEL_3};
            }}
            QToolButton:pressed {{
                background-color: {self.theme.PRIMARY_ACTIVE};
            }}
            QLineEdit, QPlainTextEdit {{ background-color: {self.theme.BG_LEVEL_0}; color: {self.theme.TEXT_PRIMARY}; border: 1px solid {self.theme.BG_LEVEL_3}; padding: 2px; }}
            QDockWidget {{ background-color: {self.theme.BG_LEVEL_1}; color: {self.theme.TEXT_PRIMARY}; }}
            QTextEdit {{ background-color: {self.theme.BG_LEVEL_0}; color: {self.theme.TEXT_PRIMARY}; border: 1px solid {self.theme.BG_LEVEL_3}; padding: 2px; }}
            QGroupBox {{ color: {self.theme.TEXT_PRIMARY}; }}
            QRadioButton {{ color: {self.theme.TEXT_PRIMARY}; }}
            QCheckBox {{ color: {self.theme.TEXT_PRIMARY}; }}
            QComboBox {{ background-color: {self.theme.BG_LEVEL_0}; color: {self.theme.TEXT_PRIMARY}; border: 1px solid {self.theme.BG_LEVEL_3}; padding: 2px; }}
            QListWidget {{ background-color: {self.theme.BG_LEVEL_0}; color: {self.theme.TEXT_PRIMARY}; border: 1px solid {self.theme.BG_LEVEL_3}; }}
            QListWidget::item:selected {{ background-color: {self.theme.CELL_SELECT_START}; color: white; }}
            QScrollArea {{ border: none; }}
            /* ⭐ ウェルカム画面のスタイルを追加 */
            QWidget#welcome_widget {{
                background-color: {self.theme.BG_LEVEL_0};
            }}

            QWidget#welcome_widget QPushButton {{
                background-color: {self.theme.PRIMARY};
                color: white;
                border: none;
                border-radius: 8px;
                font-weight: bold;
                min-height: 50px;
                min-width: 150px;
            }}

            QWidget#welcome_widget QPushButton:hover {{
                background-color: {self.theme.PRIMARY_HOVER};
            }}

            QWidget#welcome_widget QPushButton:pressed {{
                background-color: {self.theme.PRIMARY_ACTIVE};
            }}
        """)

    def is_readonly_mode(self, for_edit=False):
        is_lazy = self.lazy_loader is not None
        if for_edit and is_lazy:
            return True
        return is_lazy

    def show_operation_status(self, message, duration=2000, is_error=False):
        self.operation_label.setText(message)
        palette = self.operation_label.palette()
        color = self.theme.DANGER_QCOLOR if is_error else self.theme.TEXT_PRIMARY_QCOLOR
        palette.setColor(QPalette.WindowText, color)
        self.operation_label.setPalette(palette)
        if self.operation_timer: self.operation_timer.stop()
        self.operation_timer = QTimer(self)
        self.operation_timer.setSingleShot(True)
        self.operation_timer.timeout.connect(lambda: self.operation_label.setText(""))
        self.operation_timer.start(duration)

    def _create_extract_window_in_ui_thread(self, df):
        """抽出結果を新しいウィンドウで表示"""
        print(f"DEBUG: 新しいウィンドウを作成 - DataFrame shape: {df.shape}")

        if df.empty:
            QMessageBox.warning(self, "警告", "抽出結果が空です。")
            return

        # 🔥 修正のポイント：抽出結果も新しいウィンドウで開く open_new_window_with_new_data を使用
        self.open_new_window_with_new_data(dataframe=df)

    def _pulse_cells(self, indexes):
        self.pulsing_cells = set(indexes)
        for idx in indexes:
            self.table_model.dataChanged.emit(idx, idx, [Qt.BackgroundRole])
        self.pulse_timer.start(700)

    def _end_pulse(self):
        old_pulsing_cells = self.pulsing_cells
        self.pulsing_cells = set()
        for idx in old_pulsing_cells:
            self.table_model.dataChanged.emit(idx, idx, [Qt.BackgroundRole])

    def closeEvent(self, event):
        """アプリケーション終了時の処理（子ウィンドウ管理強化版）"""
        # 設定の保存
        self.settings_manager.save_window_settings(self)
        self.settings_manager.save_toolbar_state(self)
        
        # 未保存の変更確認
        if self.undo_manager.can_undo():
            reply = QMessageBox.question(
                self, 
                "確認",
                "未保存の変更があります。終了しますか？",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if reply == QMessageBox.No:
                event.ignore()
                return
        
        # 🔥 改善：子ウィンドウの適切な管理
        # QMainWindowの親子の関係はQtが自動的に管理しますが、
        # Python側で明示的にopen_new_window_with_file / open_new_window_with_new_data で
        # child_windows リストに追加しているため、親ウィンドウが閉じる際に
        # これらの子ウィンドウも閉じるように明示的に処理します。
        # ただし、現在のウィンドウが子ウィンドウである場合は、親に閉じるよう指示する代わりに
        # そのまま閉じさせます。
        if hasattr(self, 'child_windows') and self.child_windows:
            # 現在のウィンドウが親ウィンドウ（アプリケーション起動時に最初に開いたウィンドウ）である場合のみ
            # 子ウィンドウのリストを走査して閉じる
            if self.parent() is None: # 親がNoneの場合、このウィンドウがrootウィンドウ
                print("DEBUG: Rootウィンドウのクローズイベント。子ウィンドウもクローズします。")
                for window in list(self.child_windows): # リストのコピーを作成
                    try:
                        # ウィンドウがまだ存在し、かつ非表示でない場合にのみ閉じる
                        if window and window.isWindow() and not window.isHidden():
                            print(f"DEBUG: 子ウィンドウをクローズ中: {window.windowTitle()}")
                            window.close()
                        # 子ウィンドウが正常に閉じられたか、または存在しなくなった場合はリストから削除
                        if window in self.child_windows:
                            self.child_windows.remove(window)
                    except Exception as e:
                        print(f"WARNING: 子ウィンドウのクローズでエラー: {e}")
            else: # このウィンドウ自体が子ウィンドウである場合
                print("DEBUG: 子ウィンドウのクローズイベント。バックエンドをクリーンアップします。")
                # 親ウィンドウの `child_windows` リストから自身を削除
                if hasattr(self.parent(), 'child_windows') and self in self.parent().child_windows:
                    self.parent().child_windows.remove(self)
        
        # バックエンドのクリーンアップ (現在のウィンドウのバックエンドを閉じる)
        self._cleanup_backend()
        
        event.accept()

    def resizeEvent(self, event):
        """ウィンドウサイズ変更時の自動最適化"""
        super().resizeEvent(event)
        self._adjust_toolbar_for_width()

    def _adjust_toolbar_for_width(self):
        """画面幅に応じたツールバー最適化"""
        width = self.width()
        toolbar = self.findChild(QToolBar, "MainToolbar")

        if not toolbar:
            return

        if width < 1200:
            toolbar.setToolButtonStyle(Qt.ToolButtonIconOnly)
            toolbar.setIconSize(QSize(16, 16))
            toolbar.setStyleSheet(toolbar.styleSheet() + """
                QToolButton {
                    min-width: 24px;
                    font-size: 12px;
                    padding: 1px 2px;
                }
            """)
        elif width < 1600:
            toolbar.setToolButtonStyle(Qt.ToolButtonTextUnderIcon)
            toolbar.setIconSize(QSize(16, 16))
            toolbar.setStyleSheet(toolbar.styleSheet() + """
                QToolButton {
                    min-width: 35px;
                    font-size: 13px;
                    padding: 2px 3px;
                }
            """)
        else:
            toolbar.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
            toolbar.setIconSize(QSize(20, 20))
            toolbar.setStyleSheet(toolbar.styleSheet() + """
                QToolButton {
                    min-width: 50px;
                    font-size: 14px;
                    padding: 2px 4px;
                }
            """)

    def _update_action_button_states(self):
        self._debug_selection_state()

        selection = self.table_view.selectionModel()

        if not selection:
            return

        selected_indexes = selection.selectedIndexes()
        has_cell_selection = len(selected_indexes) > 0
        has_active_cell = self.table_view.currentIndex().isValid()

        has_column_selection = False
        has_row_selection = False

        selected_columns = selection.selectedColumns()
        has_column_selection = len(selected_columns) > 0

        selected_rows = selection.selectedRows()
        has_row_selection = len(selected_rows) > 0

        if not has_column_selection and selected_indexes:
            rows = set(idx.row() for idx in selected_indexes)
            cols = set(idx.column() for idx in selected_indexes)

            total_rows = self.table_model.rowCount()
            total_cols = self.table_model.columnCount()

            for col in cols:
                col_indexes = [idx for idx in selected_indexes if idx.column() == col]
                if len(col_indexes) == total_rows:
                    has_column_selection = True
                    break

            for row in rows:
                row_indexes = [idx for idx in selected_indexes if idx.row() == row]
                if len(row_indexes) == total_cols:
                    has_row_selection = True
                    break

        is_readonly_for_edit = self.is_readonly_mode(for_edit=True)

        self.copy_action.setEnabled(has_cell_selection)
        self.cut_action.setEnabled(has_cell_selection and not is_readonly_for_edit)
        self.delete_action.setEnabled(has_cell_selection and not is_readonly_for_edit)
        self.paste_action.setEnabled(QApplication.clipboard().text() != "" and not is_readonly_for_edit and has_active_cell)

        self.copy_column_action.setEnabled(has_column_selection)
        self.paste_column_action.setEnabled(has_column_selection and self.table_operations.column_clipboard is not None and not is_readonly_for_edit)

        self.delete_selected_rows_action.setEnabled(has_row_selection and not is_readonly_for_edit)
        self.delete_selected_column_action.setEnabled(has_column_selection and not is_readonly_for_edit)

        self.sort_asc_action.setEnabled(has_active_cell and not self.lazy_loader)
        self.sort_desc_action.setEnabled(has_active_cell and not self.lazy_loader)
        self.clear_sort_action.setEnabled(self.sort_info['column_index'] != -1 and not self.lazy_loader)

        self.add_row_action.setEnabled(not is_readonly_for_edit)
        self.add_column_action.setEnabled(not is_readonly_for_edit)

        self.cell_concatenate_action.setEnabled(has_active_cell and not is_readonly_for_edit)
        self.column_concatenate_action.setEnabled(has_active_cell and not is_readonly_for_edit)
        self.remove_duplicates_action.setEnabled(not is_readonly_for_edit and self.table_model.rowCount() > 0)

        self.view_controller.show_context_hint(
            'column_selected' if has_column_selection else
            'row_selected' if has_row_selection else
            'cell_selected' if has_cell_selection else ''
        )

        self.update_menu_states()

        print(f"DEBUG: 選択状態 - 列選択={has_column_selection}, 行選択={has_row_selection}, セル選択={has_cell_selection}")
        print(f"DEBUG: アクション状態 - 列コピー={self.copy_column_action.isEnabled()}, 行削除={self.delete_selected_rows_action.isEnabled()}, 列削除={self.delete_selected_column_action.isEnabled()}")

    def update_menu_states(self):
        undo_action = self.undo_action
        redo_action = self.redo_action

        is_readonly_for_edit = self.is_readonly_mode(for_edit=True)
        
        # 防御的プログラミング：メソッドの存在を確認
        if hasattr(self.undo_manager, 'can_undo'):
            undo_action.setEnabled(self.undo_manager.can_undo() and not is_readonly_for_edit)
        else:
            undo_action.setEnabled(False)
        
        if hasattr(self.undo_manager, 'can_redo'):
            redo_action.setEnabled(self.undo_manager.can_redo() and not is_readonly_for_edit)
        else:
            redo_action.setEnabled(False)
        
        # 🔥 追加: メニューアクションのツールチップを更新する（常に有効化されているアクションのため）
        if hasattr(self, 'open_action') and hasattr(self, 'new_action'):
             self._update_menu_tooltips()

    @Slot(QModelIndex, QModelIndex)
    def _on_current_changed(self, current: QModelIndex, previous: QModelIndex):
        if current.isValid():
            self._pulse_cells([current])
            self.active_index = current
        self._update_action_button_states()
        if self.card_mapper and not self.table_view.isHidden():
            self.card_mapper.setCurrentIndex(current.row())

    @Slot(int, int, int)
    def _on_column_resized(self, logicalIndex, oldSize, newSize):
        if self.card_scroll_area.isVisible():
            col_name = self.table_model.headerData(logicalIndex, Qt.Horizontal)
            if col_name in self.view_controller.card_fields_widgets:
                self.view_controller._adjust_text_edit_height(self.view_controller.card_fields_widgets[col_name])

    @Slot()
    def _on_model_layout_changed(self):
        """モデルの構造（行数、列数、ヘッダーなど）が変更されたときに呼び出されるスロット。UIを更新する。"""
        self.view_controller.recreate_card_view_fields()
        self._update_action_button_states()
        if self.search_panel:
            self.search_panel.update_headers(self.table_model._headers)
        self.card_mapper.toFirst()

    @Slot(QModelIndex, QModelIndex, list)
    def _on_model_data_changed(self, top_left: QModelIndex, bottom_right: QModelIndex, roles=None):
        """モデルのデータが変更されたときの処理"""
        if self.card_scroll_area.isVisible():
            current_card_row = self.card_mapper.currentIndex()
            if top_left.row() <= current_card_row <= bottom_right.row():
                self.card_mapper.setCurrentIndex(current_card_row)
        self.update_menu_states()

    def _show_table_context_menu(self, pos):
        index = self.table_view.indexAt(pos)
        if not index.isValid():
            return
        menu = QMenu(self)
        selection = self.table_view.selectionModel()

        menu.addAction(self.cut_action)
        menu.addAction(self.copy_action)
        menu.addAction(self.paste_action)
        menu.addAction(self.delete_action)
        menu.addSeparator()

        # ソートメニュー
        sort_menu = menu.addMenu("現在の列をソート")
        sort_menu.setEnabled(not self.is_readonly_mode())
        sort_menu.addAction(self.sort_asc_action)
        sort_menu.addAction(self.sort_desc_action)

        if self.sort_info['column_index'] != -1:
            menu.addAction(self.clear_sort_action)
        
        menu.addSeparator()
        
        # 連結メニュー - 新しいアクションを作成
        merge_menu = menu.addMenu("連結")
        merge_menu.setEnabled(not self.is_readonly_mode(for_edit=True))
        
        # サブメニュー用の新しいアクションを作成し、table_operationsに接続
        cell_merge_action = QAction("セルの値を連結...", self)
        cell_merge_action.triggered.connect(lambda: self.table_operations.concatenate_cells(is_column_merge=False))
        cell_merge_action.setEnabled(not self.is_readonly_mode(for_edit=True))
        
        column_merge_action = QAction("列の値を連結...", self)
        column_merge_action.triggered.connect(lambda: self.table_operations.concatenate_cells(is_column_merge=True))
        column_merge_action.setEnabled(not self.is_readonly_mode(for_edit=True))
        
        merge_menu.addAction(cell_merge_action)
        merge_menu.addAction(column_merge_action)
        
        menu.addSeparator()
        
        # 行削除の処理
        selected_rows = selection.selectedRows()
        selected_columns = selection.selectedColumns()

        if len(selected_rows) > 0 and len(selected_columns) == 0:
            delete_rows_action = QAction(f"{len(selected_rows)}行を削除", self)
            delete_rows_action.triggered.connect(self.table_operations.delete_selected_rows)
            delete_rows_action.setEnabled(not self.is_readonly_mode(for_edit=True))
            menu.addAction(delete_rows_action)

        menu.exec(self.table_view.viewport().mapToGlobal(pos))

    def _show_header_context_menu(self, pos):
        logical_index = self.table_view.horizontalHeader().logicalIndexAt(pos)
        if logical_index == -1:
            return

        menu = QMenu(self)
        col_name = self.table_model.headerData(logical_index, Qt.Horizontal)
        selection = self.table_view.selectionModel()

        sort_asc_action = QAction(f"列「{col_name}」を昇順でソート", self)
        sort_asc_action.triggered.connect(lambda: self._sort_by_column(Qt.AscendingOrder, logical_index))
        sort_asc_action.setEnabled(not self.is_readonly_mode())
        menu.addAction(sort_asc_action)

        sort_desc_action = QAction(f"列「{col_name}」を降順でソート", self)
        sort_desc_action.triggered.connect(lambda: self._sort_by_column(Qt.DescendingOrder, logical_index))
        sort_desc_action.setEnabled(not self.is_readonly_mode())
        menu.addAction(sort_desc_action)

        if self.sort_info['column_index'] != -1:
            clear_sort_action = QAction("ソートをクリア", self)
            clear_sort_action.triggered.connect(self._clear_sort)
            clear_sort_action.setEnabled(not self.is_readonly_mode())
            menu.addAction(clear_sort_action)

        menu.addSeparator()

        menu.addAction(self.copy_column_action)
        menu.addAction(self.paste_column_action)
        menu.addSeparator()

        selected_columns = selection.selectedColumns()
        if len(selected_columns) > 0:
            is_column_selected = any(idx.column() == logical_index for idx in selected_columns)
            if is_column_selected:
                delete_column_action = QAction(f"列「{col_name}」を削除", self)
                delete_column_action.triggered.connect(self.table_operations.delete_selected_columns)
                delete_column_action.setEnabled(not self.is_readonly_mode(for_edit=True))
                menu.addAction(delete_column_action)

        menu.exec(self.table_view.horizontalHeader().mapToGlobal(pos))

    def _on_column_header_clicked(self, logical_index):
        """列ヘッダーがクリックされたときの処理"""
        modifiers = QApplication.keyboardModifiers()

        if not (modifiers & Qt.ControlModifier):
            self.table_view.clearSelection()

        self.table_view.selectColumn(logical_index)

        selection_model = self.table_view.selectionModel()
        if selection_model:
            top_index = self.table_model.index(0, logical_index)
            bottom_index = self.table_model.index(self.table_model.rowCount() - 1, logical_index)
            column_selection = QItemSelection(top_index, bottom_index)

            if modifiers & Qt.ControlModifier:
                selection_model.select(column_selection, QItemSelectionModel.Select | QItemSelectionModel.Columns)
            else:
                selection_model.select(column_selection, QItemSelectionModel.ClearAndSelect | QItemSelectionModel.Columns)

        self._update_action_button_states()
        print(f"DEBUG: 列{logical_index}がクリックされました - 選択完了")

    def _on_row_header_clicked(self, logical_index):
        """行ヘッダーがクリックされたときの処理"""
        modifiers = QApplication.keyboardModifiers()

        if not (modifiers & Qt.ControlModifier):
            self.table_view.clearSelection()

        self.table_view.selectRow(logical_index)

        selection_model = self.table_view.selectionModel()
        if selection_model:
            left_index = self.table_model.index(logical_index, 0)
            right_index = self.table_model.index(logical_index, self.table_model.columnCount() - 1)
            row_selection = QItemSelection(left_index, right_index)

            if modifiers & Qt.ControlModifier:
                selection_model.select(row_selection, QItemSelectionModel.Select | QItemSelectionModel.Rows)
            else:
                selection_model.select(row_selection, QItemSelectionModel.ClearAndSelect | QItemSelectionModel.Rows)

        self._update_action_button_states()
        print(f"DEBUG: 行{logical_index}がクリックされました - 選択完了")

    # _custom_key_press_event は eventFilterに統合されたため削除

    def _sort_by_column(self, order, logical_index=None):
        if self.lazy_loader:
            self.show_operation_status("遅延読み込みモードではソートできません。", is_error=True); return

        if logical_index is None:
            current_index = self.table_view.currentIndex()
            if not current_index.isValid(): return
            logical_index = current_index.column()

        self.sort_info = {'column_index': logical_index, 'order': order}

        self.table_view.horizontalHeader().setSortIndicator(logical_index, order)
        self.table_model.sort(logical_index, order)
        col_name = self.table_model.headerData(logical_index, Qt.Horizontal)
        self.show_operation_status(f"列「{col_name}」でソートしました。")
        self._update_action_button_states()

    def _clear_sort(self):
        """ソートをクリア"""
        self.sort_info = {'column_index': -1, 'order': Qt.AscendingOrder}
        self.table_view.horizontalHeader().setSortIndicator(-1, Qt.AscendingOrder)
        if not self.lazy_loader:
            self.table_model.sort(-1, Qt.AscendingOrder)
            self.show_operation_status("ソートをクリアしました。")
        self._update_action_button_states()

    def _open_price_calculator(self):
        if self.table_model.rowCount() == 0:
            self.show_operation_status("操作対象のデータがありません。", 3000, is_error=True); return
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードでは金額計算ツールを実行できません。", is_error=True); return

        # ① 現在選択中の列名を取得
        current_col = self._get_current_selected_column_name()
        # ② ダイアログに初期列名を渡す
        dialog = PriceCalculatorDialog(self, self.table_model._headers, initial_column_name=current_col)

        if dialog.exec() == QDialog.Accepted:
            settings = dialog.result
            self._apply_price_calculation(settings)

    def _open_text_processing_tool(self):
        """テキスト処理ツールを開く"""
        if self.table_model.rowCount() == 0:
            self.show_operation_status("操作対象のデータがありません。", 3000, is_error=True)
            return

        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードではテキスト処理ツールを実行できません。", is_error=True)
            return

        dialog = TextProcessingDialog(self, self.table_model._headers)

        if dialog.exec() == QDialog.Accepted:
            settings = dialog.getSettings()
            self._apply_text_processing(settings)

    def _apply_price_calculation(self, settings):
        """金額計算を実行"""
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードでは計算を実行できません。", is_error=True)
            return

        target_col = settings['column']
        tax_status = settings['tax_status']
        discount = settings['discount']
        round_mode = settings['round_mode']

        print(f"DEBUG: 金額計算開始")
        print(f"  - 対象列: {target_col}")
        print(f"  - 税の状態: {tax_status}")
        print(f"  - 割引率: {discount}%")
        print(f"  - 丸め方: {round_mode}")

        try:
            target_col_index = self.table_model._headers.index(target_col)
        except ValueError:
            self.show_operation_status(f"列 '{target_col}' が見つかりません。", is_error=True)
            return

        print(f"DEBUG: 対象列のインデックス: {target_col_index}")

        changes = []
        processed_count = 0
        error_count = 0

        tax_rate = 1.10
        discount_multiplier = 1.0 - (discount / 100.0)

        for i in range(self.table_model.rowCount()):
            index = self.table_model.index(i, target_col_index)
            original_value_str = self.table_model.data(index, Qt.DisplayRole)

            if not original_value_str:
                continue

            try:
                cleaned_value = re.sub(r'[^\d.]', '', str(original_value_str))
                if not cleaned_value:
                    continue

                price = float(cleaned_value)
                processed_count += 1

                if tax_status == 'exclusive':
                    price_with_tax = math.floor(price * tax_rate)
                    discounted_price_with_tax = math.floor(price_with_tax * discount_multiplier)
                    new_price_float = discounted_price_with_tax / tax_rate

                    epsilon = 0.0001
                    new_price_float = new_price_float + epsilon

                    if i < 5:
                        print(f"DEBUG: 行{i} - 元の価格: {price}")
                        print(f"  → 税込価格: {price * tax_rate} → 切り捨て: {price_with_tax}")
                        print(f"  → 割引後税込: {price_with_tax * discount_multiplier} → 切り捨て: {discounted_price_with_tax}")
                        print(f"  → 税抜に戻す（補正前）: {discounted_price_with_tax / tax_rate}")
                        print(f"  → 税抜に戻す（補正後）: {new_price_float}")

                else:
                    new_price_float = price * discount_multiplier

                if round_mode == 'truncate':
                    new_price = math.floor(new_price_float)
                elif round_mode == 'round':
                    new_price = round(new_price_float)
                elif round_mode == 'ceil':
                    new_price = math.ceil(new_price_float)
                else:
                    new_price = math.floor(new_price_float)

                new_value_str = str(int(new_price))

                if i < 5:
                    print(f"  → 最終価格: {new_price_float} → 丸め後: {new_price}")

                if new_value_str != str(original_value_str):
                    changes.append({
                        'item': str(i),
                        'column': target_col,
                        'old': original_value_str,
                        'new': new_value_str
                    })

            except (ValueError, TypeError) as e:
                error_count += 1
                if error_count <= 5:
                    print(f"Warning: Row {i}, Column '{target_col}' value '{original_value_str}' cannot be converted to number. Error: {e}")
                continue

        print(f"DEBUG: 処理完了 - 処理行数: {processed_count}, 変更数: {len(changes)}, エラー数: {error_count}")

        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action)
            self.apply_action(action, is_undo=False)
            self.show_operation_status(f"{len(changes)}件の金額を更新しました")
        else:
            self.show_operation_status("金額の更新はありませんでした", 2000)

    def _apply_text_processing(self, settings):
        """テキスト処理を実行"""
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードではテキスト処理を実行できません。", is_error=True)
            return

        target_col = settings['column']

        print(f"DEBUG: テキスト処理開始")
        print(f"  - 対象列: {target_col}")
        print(f"  - 接頭辞追加: {settings['add_prefix']}")
        print(f"  - 接頭辞: {settings['prefix']}")
        print(f"  - バイト数制限: {settings['apply_limit']}")
        print(f"  - 最大バイト数: {settings['max_bytes']}")

        try:
            target_col_index = self.table_model._headers.index(target_col)
        except ValueError:
            self.show_operation_status(f"列 '{target_col}' が見つかりません。", is_error=True)
            return

        changes = []
        processed_count = 0

        for i in range(self.table_model.rowCount()):
            index = self.table_model.index(i, target_col_index)
            original_text = str(self.table_model.data(index, Qt.DisplayRole) or "")

            if not original_text.strip() and not settings['add_prefix']:
                continue

            processed_count += 1

            new_text = self._process_single_text(original_text, settings)

            if original_text != new_text:
                changes.append({
                    'item': str(i),
                    'column': target_col,
                    'old': original_text,
                    'new': new_text
                })

        print(f"DEBUG: 処理完了 - 処理行数: {processed_count}, 変更数: {len(changes)}")

        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action)
            self.apply_action(action, is_undo=False)
            self.show_operation_status(f"{len(changes)}件のテキストを処理しました")
        else:
            self.show_operation_status("テキストの変更はありませんでした", 2000)

    def _process_single_text(self, text, settings):
        """単一テキストの処理"""
        result = text

        if settings['add_prefix'] and settings['prefix']:
            result = settings['prefix'] + result

        if settings['apply_limit']:
            max_bytes = settings['max_bytes']
            result = self._limit_text_by_bytes(result, max_bytes)

        if settings['trim_end']:
            result = result.rstrip()

        if settings['remove_partial_word']:
            result = self._remove_partial_word(result)

        return result

    def _limit_text_by_bytes(self, text, max_bytes):
        """バイト数制限"""
        if self._get_byte_length(text) <= max_bytes:
            return text

        result = text
        while len(result) > 0 and self._get_byte_length(result) > max_bytes:
            result = result[:-1]

        return result

    def _remove_partial_word(self, text):
        """行末の不完全な単語を削除"""
        return re.sub(r'\s+[^\s]*$', '', text)

    def _get_byte_length(self, text):
        """Shift-JIS相当のバイト数計算"""
        byte_length = 0
        for char in text:
            char_code = ord(char)
            if ((0x0020 <= char_code <= 0x007e) or
                (0xff61 <= char_code <= 0xff9f)):
                byte_length += 1
            else:
                byte_length += 2
        return byte_length

    # 修正3: _show_shortcuts メソッドを修正
    def _show_shortcuts(self):
        """実際に機能するショートカットキーのみを表示"""
        shortcuts_text = """
### ファイル操作:
Ctrl+N: 新規作成
Ctrl+O: ファイルを開く
Ctrl+S: ファイルを保存
Ctrl+Q: アプリケーションを終了

### 編集操作:
Ctrl+Z: 元に戻す (Undo)
Ctrl+Y: やり直し (Redo)
Ctrl+X: 切り取り
Ctrl+C: コピー
Ctrl+V: 貼り付け
Delete: 選択セルをクリア
Ctrl+A: 全選択
Ctrl+F: 検索パネル表示/非表示
Ctrl+Shift+D: 重複行を削除

### 行/列操作:
Ctrl++ : 行を追加
Ctrl+- : 選択行を削除
Ctrl+Shift++ : 列を追加
Ctrl+Shift+- : 選択列を削除
Ctrl+Shift+C: 列をコピー
Ctrl+Shift+V: 列に貼り付け

### ソート:
Ctrl+↑: 現在の列を昇順ソート
Ctrl+↓: 現在の列を降順ソート
Ctrl+Backspace: ソートをクリア

### ビュー切り替え:
Ctrl+Tab: テーブルビュー/カードビュー切り替え

### カードビュー内移動:
Ctrl+←: 前のレコード
Ctrl+→: 次のレコード
Ctrl+↑: 前のレコード (カードビュー)
Ctrl+↓: 次のレコード (カードビュー)

### セル編集:
F2またはEnter: セルの編集開始
Tab: 次のセルへ移動
Shift+Tab: 前のセルへ移動
"""
        
        dialog = QMessageBox(self)
        dialog.setWindowTitle("ショートカットキー一覧")
        dialog.setText(shortcuts_text)
        dialog.setStandardButtons(QMessageBox.Ok)
        
        # ダイアログサイズを大きく
        dialog.setStyleSheet("QLabel{min-width: 500px; min-height: 600px;}")
        dialog.exec()

    def _get_current_selected_column_name(self):
        """現在選択中の列名を安全に取得する"""
        try:
            # テーブルビューの場合
            if hasattr(self, 'view_controller') and self.view_controller.current_view == 'table':
                current_index = self.table_view.currentIndex()
                if current_index.isValid():
                    col_idx = current_index.column()
                    if 0 <= col_idx < len(self.header):
                        column_name = self.table_model.headerData(col_idx, Qt.Horizontal)
                        print(f"DEBUG: テーブルビューの選択列: {column_name}")
                        return column_name
            
            # カードビューの場合
            elif hasattr(self, 'view_controller') and self.view_controller.current_view == 'card':
                focused_widget = QApplication.focusWidget()
                if isinstance(focused_widget, QPlainTextEdit):
                    # card_fields_widgetsから列名を逆引き
                    for col_name, widget in self.view_controller.card_fields_widgets.items():
                        if widget == focused_widget:
                            print(f"DEBUG: カードビューのフォーカス列: {col_name}")
                            return col_name
                
                # フォーカスが特定できない場合、現在のレコードの最初の列
                if hasattr(self, 'card_mapper') and self.card_mapper and self.header:
                    print(f"DEBUG: カードビューでフォーカス不明、最初の列を使用: {self.header[0]}")
                    return self.header[0]
            
        except Exception as e:
            print(f"DEBUG: 現在列の取得でエラー: {e}")
        
        return None

    def _toggle_search_panel(self):
        """検索パネルの表示/非表示を切り替える（自動スクロール対応版）"""
        if self.table_model.rowCount() == 0:
            self.show_operation_status("操作対象のデータがありません。", 3000, is_error=True)
            return

        if self.search_dock_widget is None:
            self._create_search_dock_widget()
            
        self.search_panel.update_headers(self.table_model._headers)

        if self.search_dock_widget.isHidden():
            # 現在選択中の列を取得
            current_column = self._get_current_selected_column_name()
            
            if current_column and current_column in self.table_model._headers:
                # 現在の列を選択し、自動スクロール
                success = self.search_panel.set_target_column(current_column)
                
                if success:
                    # 関連列の提案を表示
                    if hasattr(self.search_panel, '_suggest_related_columns'):
                        self.search_panel._suggest_related_columns(current_column)
                    
                    self.show_operation_status(
                        f"🔍 検索対象: 「{current_column}」列が選択されました（関連列も確認してください）", 4000
                    )
                else:
                    self.search_panel.reset_to_default_column()
                    self.show_operation_status("検索対象: 最初の列が選択されました", 2000)
            else:
                self.search_panel.reset_to_default_column()
                self.show_operation_status("検索対象: 最初の列が選択されました", 2000)

            self.search_dock_widget.show()
            
            # 検索入力欄にフォーカス（スクロール完了後）
            QTimer.singleShot(300, lambda: self.search_panel.search_entry.setFocus())
        else:
            self.search_dock_widget.hide()

    @Slot(str)
    def _on_parent_child_analysis_ready(self, summary_text):
        """親子関係分析結果の受信処理"""
        self._close_progress_dialog()
        self.progress_bar.hide()

        if self.search_panel:
            if "分析エラー" in summary_text:
                self.search_panel.analysis_text.setText(summary_text)
                self.show_operation_status("親子関係の分析に失敗しました。", is_error=True)
            else:
                self.search_panel.analysis_text.setText(summary_text)
                self.show_operation_status("親子関係を分析しました。")

    @Slot(list, str)
    def _on_replace_from_file_completed(self, changes: list, status_message: str):
        """ファイル参照置換完了の処理"""
        self._close_progress_dialog()
        QApplication.restoreOverrideCursor()
        self.progress_bar.hide()

        if "エラー" in status_message or "失敗" in status_message:
            self.show_operation_status(status_message, is_error=True)
            QMessageBox.critical(self, "エラー", status_message)
        elif not changes:
            if self.db_backend:
                self.table_model.layoutChanged.emit()
            self.show_operation_status(status_message, 3000)
        else:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action)
            self.apply_action(action, is_undo=False)
            self.show_operation_status(status_message)

    @Slot(list, str)
    def _on_product_discount_completed(self, changes: list, status_message: str):
        """商品別割引適用完了の処理"""
        self._close_progress_dialog()
        QApplication.restoreOverrideCursor()
        self.progress_bar.hide()

        if "エラー" in status_message:
            self.show_operation_status(status_message, is_error=True)
            QMessageBox.critical(self, "エラー", status_message)
        elif not changes:
            if self.db_backend:
                self.table_model.layoutChanged.emit()
            self.show_operation_status(status_message, 3000)
        else:
            undo_data = []
            for change in changes:
                undo_data.append({
                    'item': str(change['row_idx']) if 'row_idx' in change else change['item'],
                    'column': change['col_name'] if 'col_name' in change else change['column'],
                    'old': change['old_value'] if 'old_value' in change else change['old'],
                    'new': change['new_value'] if 'new_value' in change else change['new']
                })

            action = {'type': 'edit', 'data': undo_data}
            self.undo_manager.add_action(action)
            self.apply_action(action, is_undo=False)
            self.show_operation_status(status_message)

    @Slot(object, str)
    def _on_bulk_extract_completed(self, result_df, status_message):
        """商品リスト一括抽出完了の処理"""
        self._close_progress_dialog()
        QApplication.restoreOverrideCursor()
        
        if "エラー" in status_message:
            self.show_operation_status(status_message, is_error=True)
            QMessageBox.critical(self, "エラー", status_message)
            if self.search_panel:
                self.search_panel.bulk_result_label.setText(f"<font color='red'>{status_message}</font>")
            return
            
        if self.search_panel:
            self.search_panel.bulk_result_label.setText(status_message)
        
        if result_df.empty:
            self.show_operation_status(status_message, 3000)
            QMessageBox.information(self, "抽出結果", status_message)
            return
        
        # 🔥 修正のポイント：抽出結果も新しいウィンドウで開く
        self.open_new_window_with_new_data(result_df.copy())
        self.show_operation_status(status_message)

    def _apply_replace_from_file(self, params: dict):
        """ファイル参照置換の実行処理"""
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードではファイル参照置換を実行できません。", 3000, is_error=True)
            return

        self._show_progress_dialog("ファイル参照置換を実行中...", self.async_manager.cancel_current_task)
        data_source = self.db_backend if self.db_backend else self.table_model.get_dataframe()
        self.async_manager.replace_from_file_async(self.db_backend, data_source, params)


    def _apply_product_discount(self, params):
        """商品別割引適用の実行処理"""
        if self.is_readonly_mode(for_edit=True):
            self.main_window.show_operation_status("このモードでは商品別割引適用を実行できません。", 3000, is_error=True)
            return

        if not params['current_product_col'] or not params['current_product_col'] in self.header:
            self.show_operation_status("現在ファイルの商品番号列と金額列を選択してください。", is_error=True)
            return

        if not params['discount_filepath']:
            self.show_operation_status("参照ファイルを選択してください。", is_error=True)
            return

        if not params['ref_product_col'] or not self.search_panel.ref_product_col_combo.currentText():
            self.show_operation_status("参照ファイルの商品番号列と割引率列を選択してください。", is_error=True)
            return

        if params.get('preview', False):
            pass

        self._show_progress_dialog("商品別割引適用を実行中...", self.async_manager.cancel_current_task)
        self.async_manager.product_discount_async(self.db_backend, self.table_model, params)

    def _execute_bulk_extract(self, settings: dict):
        """商品リスト一括抽出の実行処理"""
        if self.table_model.rowCount() == 0:
            self.show_operation_status("操作対象のデータがありません。", 3000, is_error=True)
            return
        
        if not settings['product_list']:
            self.show_operation_status("商品番号リストが空です。", 3000, is_error=True)
            return
            
        self._show_progress_dialog("商品リストを抽出中...", self.async_manager.cancel_current_task)

        data_source = None
        load_mode = self.async_manager.current_load_mode
        
        if load_mode == 'sqlite':
            data_source = self.db_backend
        elif load_mode == 'lazy':
            data_source = self.lazy_loader
        else:
            data_source = self.table_model.get_dataframe()
            
        self.async_manager.bulk_extract_async(data_source, settings, load_mode)

    def _analyze_parent_child_from_widget(self):
        """検索パネルからの親子関係分析要求処理"""
        settings = self.search_panel.get_settings()
        column_name = settings.get("key_column")
        analysis_mode = settings.get("analysis_mode", "consecutive")

        if not column_name:
            self.show_operation_status("親子関係分析のキー列を選択してください。", is_error=True)
            return

        if self.lazy_loader:
            QMessageBox.warning(self, "機能制限", "遅延読み込みモードでは親子関係の分析はできません。")
            if self.search_panel:
                self.search_panel.analysis_text.setText("遅延読み込みモードでは親子関係の分析はできません。")
            return

        self._show_progress_dialog("親子関係を分析中...", self.async_manager.cancel_current_task)

        if self.db_backend:
            self.async_manager.analyze_parent_child_async(self.db_backend, column_name, analysis_mode)
        else:
            df_to_analyze = self.table_model.get_dataframe()

            if df_to_analyze is None or df_to_analyze.empty:
                self._close_progress_dialog()
                if self.search_panel:
                    self.search_panel.analysis_text.setText("分析対象のデータがありません。")
                self.show_operation_status("分析対象のデータがありません。", is_error=True)
                return
            success, msg, total_rows = self.parent_child_manager.analyze_relationships(df_to_analyze, column_name, analysis_mode)
            self._close_progress_dialog()
            
            if success:
                if self.search_panel:
                    self.search_panel.analysis_text.setText(self.parent_child_manager.get_groups_summary())
                self.show_operation_status("親子関係を分析しました。")
            else:
                if self.search_panel:
                    self.search_panel.analysis_text.setText(f"分析エラー:\n{msg}")
                self.show_operation_status("親子関係の分析に失敗しました。", is_error=True)

    def _toggle_view(self):
        self.view_controller.toggle_view()

    def _adjust_text_edit_height(self, text_edit_widget):
        print("WARNING: _adjust_text_edit_height は ViewController に移譲されました。")
        doc = text_edit_widget.document()
        text_edit_widget.setUpdatesEnabled(False)
        new_height = int(doc.size().height() + text_edit_widget.contentsMargins().top() + text_edit_widget.contentsMargins().bottom() + 5)
        min_height = int(self.density['row_height'] * 1.5)
        max_height = int(self.density['row_height'] * 8)
        final_height = max(min_height, min(new_height, max_height))
        if text_edit_widget.height() != final_height:
            text_edit_widget.setFixedHeight(final_height)
        text_edit_widget.setUpdatesEnabled(True)

    def _recreate_card_view_fields(self):
        self.view_controller.recreate_card_view_fields()

    def _handle_card_view_tab_navigation(self, event: QEvent):
        return False

    def _go_to_prev_record(self):
        self.view_controller.go_to_prev_record()

    def _go_to_next_record(self):
        self.view_controller.go_to_next_record()

    def _move_card_record(self, new_row: int):
        self.view_controller._move_card_record(new_row)

    def _prepare_dataframe_for_save(self, df):
        """CSV保存前にPandas DataFrameを調整するためのフック"""
        print(f"DEBUG: _prepare_dataframe_for_save - 入力DataFrame: {df.shape if df is not None else 'None'}")

        if df is None or df.empty:
            print("WARNING: _prepare_dataframe_for_save - DataFrameが空です")
            return pd.DataFrame()

        df_copy = df.copy()
        for col in df_copy.columns:
            df_copy[col] = df_copy[col].astype(str)

        print(f"DEBUG: _prepare_dataframe_for_save - 出力DataFrame: {df_copy.shape}")
        return df_copy

    def show_context_hint(self, hint_type=''):
        self.view_controller.show_context_hint(hint_type)

    def _debug_selection_state(self):
        """現在の選択状態をデバッグ出力"""
        selection = self.table_view.selectionModel()

        if not selection:
            print("DEBUG: 選択モデルがありません")
            return

        selected_indexes = selection.selectedIndexes()
        selected_columns = selection.selectedColumns()
        selected_rows = selection.selectedRows()

        print(f"DEBUG: 選択状態詳細:")
        print(f"  - selectedIndexes: {len(selected_indexes)}個")
        print(f"  - selectedColumns: {[idx.column() for idx in selected_columns]}")
        print(f"  - selectedRows: {[idx.row() for idx in selected_rows]}")

        if selected_columns:
            for col_idx_model_index in selected_columns:
                col_num = col_idx_model_index.column()
                cells_in_column = [idx for idx in selected_indexes if idx.column() == col_num]
                print(f"  - 列{col_num}: {len(cells_in_column)}/{self.table_model.rowCount()}セル選択")

    def apply_action(self, action, is_undo):
        action_type, data = action['type'], action['data']

        if action_type in ['add_column', 'delete_column'] and self.db_backend:
            pass

        if action_type == 'edit':
            if self.db_backend:
                changes_for_db = []
                for c in data:
                    row_idx = int(c['item'])
                    col_name = c['column']
                    new_value = c['old'] if is_undo else c['new']
                    changes_for_db.append({'row_idx': row_idx, 'col_name': col_name, 'new_value': new_value})

                self.db_backend.update_cells(changes_for_db)

                self.table_model._row_cache.clear()
                self.table_model._cache_queue.clear()

                self.table_model.beginResetModel()
                self.table_model.endResetModel()

                if self.card_scroll_area.isVisible():
                    current_row = self.card_mapper.currentIndex()
                    self.card_mapper.setCurrentIndex(current_row)
            else:
                for change in data:
                    try:
                        row_idx = int(change['item'])
                        col_idx = self.table_model._headers.index(change['column'])
                        target_value = change['old'] if is_undo else change['new']
                        self.table_model.setData(self.table_model.index(row_idx, col_idx), target_value, Qt.EditRole)
                    except (ValueError, IndexError):
                        print(f"Warning: Column '{change['column']}' not found during apply_action edit.")
                        self.show_operation_status(f"一部の変更が適用できませんでした: 列'{change['column']}'が見つかりません。", is_error=True)
        elif action_type == 'delete_column':
            if is_undo:
                if self.db_backend and hasattr(self.db_backend, 'recreate_table_with_new_columns'):
                    old_headers_from_data = data['col_names_before']
                    current_headers = list(self.table_model._headers)

                    self.show_operation_status("列のUndo: テーブルを再構築中...", duration=0)
                    QApplication.setOverrideCursor(Qt.WaitCursor)
                    try:
                        success = self.db_backend.recreate_table_with_new_columns(old_headers_from_data, current_headers)
                        if success:
                            self.table_model.beginResetModel()
                            self.table_model._headers = old_headers_from_data
                            self.table_model.endResetModel()
                            self.progress_bar.hide()
                            QApplication.restoreOverrideCursor()
                        else:
                            self.progress_bar.hide()
                            QApplication.restoreOverrideCursor()
                            self.show_operation_status("列のUndoに失敗しました。", is_error=True)
                            return
                    except Exception as e:
                        self.progress_bar.hide()
                        QApplication.restoreOverrideCursor()
                        self.show_operation_status(f"列のUndo中にエラー: {e}", is_error=True)
                        return

                else:
                    self.table_model.insertColumns(data['col_idx'], 1)
                    self.table_model.setHeaderData(data['col_idx'], Qt.Horizontal, data['col_name'])

                    for row_idx, value in enumerate(data['col_data']):
                        if row_idx < self.table_model.rowCount():
                            self.table_model.setData(self.table_model.index(row_idx, data['col_idx']), value, Qt.EditRole)
            else:
                if self.db_backend and hasattr(self.db_backend, 'recreate_table_with_new_columns'):
                    new_headers_from_data = data['col_names_after']
                    current_headers = list(self.table_model._headers)

                    self.show_operation_status("列のRedo: テーブルを再構築中...", duration=0)
                    QApplication.setOverrideCursor(Qt.WaitCursor)
                    try:
                        success = self.db_backend.recreate_table_with_new_columns(new_headers_from_data, current_headers)
                        if success:
                            self.table_model.beginResetModel()
                            self.table_model._headers = new_headers_from_data
                            self.table_model.endResetModel()
                            self.progress_bar.hide()
                            QApplication.restoreOverrideCursor()
                        else:
                            self.progress_bar.hide()
                            QApplication.restoreOverrideCursor()
                            self.show_operation_status("列のRedoに失敗しました。", is_error=True)
                            return
                    except Exception as e:
                        self.progress_bar.hide()
                        QApplication.restoreOverrideCursor()
                        self.show_operation_status(f"列のRedo中にエラー: {e}", is_error=True)
                        return
                else:
                    self.table_model.removeColumns(data['col_idx'], 1)

        elif action_type == 'add_row':
            if is_undo:
                if self.db_backend and hasattr(self.db_backend, 'remove_rows'):
                    self.db_backend.remove_rows([data['row_pos']])
                self.table_model.removeRows(data['row_pos'], 1)
            else:
                if self.db_backend and hasattr(self.db_backend, 'insert_rows'):
                    self.db_backend.insert_rows(data['row_pos'], 1, self.header)
                self.table_model.insertRows(data['row_pos'], 1)

        elif action_type == 'add_column':
            if is_undo:
                if self.db_backend and hasattr(self.db_backend, 'recreate_table_with_new_columns'):
                    old_headers_from_data = data['col_names_before']
                    current_headers = list(self.table_model._headers)

                    self.show_operation_status("列のUndo: テーブルを再構築中...", duration=0)
                    QApplication.setOverrideCursor(Qt.WaitCursor)
                    try:
                        success = self.db_backend.recreate_table_with_new_columns(old_headers_from_data, current_headers)
                        if success:
                            self.table_model.beginResetModel()
                            self.table_model._headers = old_headers_from_data
                            self.table_model.endResetModel()
                            self.progress_bar.hide()
                            QApplication.restoreOverrideCursor()
                        else:
                            self.progress_bar.hide()
                            QApplication.restoreOverrideCursor()
                            self.show_operation_status("列のUndoに失敗しました。", is_error=True)
                            return
                    except Exception as e:
                        self.progress_bar.hide()
                        QApplication.restoreOverrideCursor()
                        self.show_operation_status(f"列のUndo中にエラー: {e}", is_error=True)
                        return
                else:
                    self.table_model.removeColumns(data['col_pos'], 1)
            else:
                self.table_model.insertColumns(data['col_pos'], 1, names=[data['col_name']])

        elif action_type == 'column_merge':
            pass

        self.show_operation_status(f"操作を{'元に戻しました' if is_undo else '実行しました'}"); self._update_action_button_states()

    def _create_menu_bar(self):
        pass

    def _test_save_as_menu(self):
        """名前を付けて保存メニューのテスト (file_controllerに委譲)"""
        print("DEBUG: _test_save_as_menu called")
        self.file_controller.save_as_with_dialog()

    def emergency_reset_toolbar(self):
        """ツールバー緊急復旧"""
        try: 
            # 既存のツールバーを削除
            toolbar = self.findChild(QToolBar, "MainToolbar")
            if toolbar: 
                self.removeToolBar(toolbar)
            
            # 新しいツールバーを再作成し、オブジェクト名を同じにする
            emergency_toolbar = self.addToolBar("MainToolbar") 
            emergency_toolbar.setObjectName("MainToolbar") 
            
            # 最小限のアクションを追加
            emergency_toolbar.addAction(self.new_action)
            emergency_toolbar.addAction(self.open_action)
            emergency_toolbar.addAction(self.save_action)
            emergency_toolbar.addSeparator()
            emergency_toolbar.addAction(self.search_action)
            
            # ツールバーのスタイルとサイズをデフォルトにリセット（必要に応じて）
            emergency_toolbar.setIconSize(QSize(20, 20)) 
            emergency_toolbar.setToolButtonStyle(Qt.ToolButtonTextBesideIcon) 
            emergency_toolbar.setStyleSheet("") 
            
            QMessageBox.information(self, "復旧完了", 
                "ツールバーを緊急復旧しました。\n" 
                "アプリを再起動して正常なツールバーを復元してください。") 
        except Exception as e: 
            QMessageBox.critical(self, "復旧失敗", 
                f"緊急復旧に失敗: {e}\n" 
                f"バックアップから復元してください。") 

    def open_new_window_with_file(self, filepath):
        """
        新しいウィンドウで指定されたCSVファイルを開く
        作業中のウィンドウを保護するための重要な機能
        """
        print(f"DEBUG: 新しいウィンドウでファイルを開く: {filepath}")

        try:
            # ファイルの存在確認は file_io_controller._start_file_loading_process で行われるためここでは不要
            
            # 🔥 改善1: 循環インポートを避ける
            # 関数スコープ内でインポートすることで、初期化時の循環参照を防ぐ
            from main_qt import CsvEditorAppQt 
            
            # 新しいウィンドウの作成 (filepathを渡す)
            new_window = CsvEditorAppQt(filepath=filepath)
            
            # 🔥 改善2: 子ウィンドウリストの管理
            # 新しく開いたウィンドウを親ウィンドウのリストに追加して管理
            if not hasattr(self, 'child_windows'):
                self.child_windows = []
            self.child_windows.append(new_window)
            
            # 🔥 改善3: 新しいウィンドウの位置をずらす (カスケード表示)
            # config.OPEN_FILE_BEHAVIOR['offset_new_windows'] 設定に従う
            if config.OPEN_FILE_BEHAVIOR.get('offset_new_windows', True):
                current_pos = self.pos()
                new_window.move(current_pos.x() + 30, current_pos.y() + 30)
            
            # ウィンドウタイトルの設定
            # 複数ウィンドウがあることを示すため、ウィンドウ数を追加
            base_title = f"高機能CSVエディタ (PySide6) - {os.path.basename(filepath)}"
            window_count = len(self.child_windows) # 親ウィンドウの child_windows リストの数を数える
            new_window.setWindowTitle(f"{base_title} ({window_count})") # 便宜上、子の数でタイトルに番号付け
            
            # ウィンドウを表示し、最前面に持ってくる
            new_window.show()
            new_window.raise_()  # 最前面に表示
            new_window.activateWindow()  # アクティブにする
            
            print(f"DEBUG: 新しいウィンドウ作成完了")
            self.show_operation_status(f"新しいウィンドウで '{os.path.basename(filepath)}' を開きました")
            
            return new_window
            
        except Exception as e:
            print(f"ERROR: 新しいウィンドウ作成エラー: {e}")
            import traceback
            traceback.print_exc() # 詳細なトレースバックを出力
            
            QMessageBox.critical(
                self,
                "新しいウィンドウ作成エラー",
                f"新しいウィンドウでファイルを開けませんでした。\n\n"
                f"ファイル: {filepath}\n"
                f"エラー: {str(e)}"
            )
            return None

    # 🔥 新規追加メソッド：新しいウィンドウで新規データを開くための汎用関数
    def open_new_window_with_new_data(self, dataframe):
        """新しいウィンドウで新規データ（DataFrame）を開く"""
        print(f"DEBUG: 新しいウィンドウで新規データを開く: {dataframe.shape}")
        
        try:
            # 循環インポートを避けるため、関数スコープ内でインポート
            from main_qt import CsvEditorAppQt
            
            # 新しいウィンドウの作成 (dataframe を直接渡して初期化)
            new_window = CsvEditorAppQt(dataframe=dataframe)
            
            # 子ウィンドウリストの管理
            if not hasattr(self, 'child_windows'):
                self.child_windows = []
            self.child_windows.append(new_window)
            
            # ウィンドウの位置をずらす
            if config.OPEN_FILE_BEHAVIOR.get('offset_new_windows', True):
                current_pos = self.pos()
                new_window.move(current_pos.x() + 30, current_pos.y() + 30)
            
            # ウィンドウタイトルの設定
            window_count = len(self.child_windows)
            new_window.setWindowTitle(f"高機能CSVエディタ (PySide6) - 無題 ({window_count})")
            
            # 🔥 重要：新規データの場合は直接メインビューを表示
            # CsvEditorAppQt の __init__ が dataframe が渡された場合に適切に UI を設定するようになっているため、
            # ここではその後の操作ステータスを更新するだけで十分です。
            # new_window.view_controller.show_main_view()
            # new_window.welcome_widget.hide()
            # new_window.view_stack.show()
            
            # ウィンドウを表示し、最前面に持ってくる
            new_window.show()
            new_window.raise_()
            new_window.activateWindow()
            
            print(f"DEBUG: 新規データウィンドウ作成完了")
            self.show_operation_status("新しいウィンドウで新規ファイルを作成しました")
            
            return new_window
            
        except Exception as e:
            print(f"ERROR: 新規データウィンドウ作成エラー: {e}")
            import traceback
            traceback.print_exc()
            
            QMessageBox.critical(
                self,
                "新しいウィンドウ作成エラー",
                f"新しいウィンドウで新規ファイルを作成できませんでした。\n\n"
                f"エラー: {str(e)}"
            )
            return None

    def _diagnose_display_issue(self):
        """表示問題を診断する"""
        info = []
        info.append("=== 表示診断情報 ===\n")

        info.append("ウィジェットの存在:")
        info.append(f"  - view_stack: {hasattr(self, 'view_stack')}")
        info.append(f"  - table_view: {hasattr(self, 'table_view')}")
        info.append(f"  - welcome_widget: {hasattr(self, 'welcome_widget')}")
        info.append(f"  - card_scroll_area: {hasattr(self, 'card_scroll_area')}")

        info.append("\n表示状態:")
        if hasattr(self, 'view_stack'):
            info.append(f"  - view_stack.isVisible(): {self.view_stack.isVisible()}")
            info.append(f"  - view_stack.isHidden(): {self.view_stack.isHidden()}")

        if hasattr(self, 'table_view'):
            info.append(f"  - table_view.isVisible(): {self.table_view.isVisible()}")
            info.append(f"  - table_view.isHidden(): {self.table_view.isHidden()}")

        if hasattr(self, 'welcome_widget'):
            info.append(f"  - welcome_widget.isVisible(): {self.welcome_widget.isVisible()}")

        if hasattr(self, 'card_scroll_area'):
            info.append(f"  - card_scroll_area.isVisible(): {self.card_scroll_area.isVisible()}")

        info.append("\nデータ状態:")
        info.append(f"  - table_model.rowCount(): {self.table_model.rowCount()}")
        info.append(f"  - table_model.columnCount(): {self.table_model.columnCount()}")
        info.append(f"  - _df is None: {self._df is None}")
        if self._df is not None:
            info.append(f"  - _df.shape: {self._df.shape}")

        info.append("\n親子関係:")
        if hasattr(self, 'table_view'):
            info.append(f"  - table_view.parent(): {self.table_view.parent()}")
        if hasattr(self, 'view_stack'):
            info.append(f"  - view_stack.parent(): {self.view_stack.parent()}")
            info.append(f"  - view_stack.layout(): {self.view_stack.layout()}")

        info.append("\nサイズ情報:")
        if hasattr(self, 'view_stack'):
            info.append(f"  - view_stack.size(): {self.view_stack.size()}")
        if hasattr(self, 'table_view'):
            info.append(f"  - table_view.size(): {self.table_view.size()}")

        result = "\n".join(info)
        print(result)

        dialog = QDialog(self)
        dialog.setWindowTitle("表示診断結果")
        dialog.setMinimumSize(600, 400)

        layout = QVBoxLayout(dialog)
        text_edit = QTextEdit()
        text_edit.setPlainText(result)
        text_edit.setReadOnly(True)
        layout.addWidget(text_edit)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        dialog.exec()

    def _emergency_show_table(self):
        """緊急: テーブルを強制的に表示"""
        print("DEBUG: 緊急表示実行")

        if self.table_model.rowCount() == 0:
            QMessageBox.warning(self, "警告", "表示するデータがありません"); return

        widgets_to_hide = [self.welcome_widget, self.card_scroll_area]
        for widget in widgets_to_hide:
            if widget is not None:
                widget.hide()

        if not hasattr(self, 'view_stack') or self.view_stack.layout() is None:
            self.view_stack = QWidget()
            self.setCentralWidget(self.view_stack)
            self.view_stack_layout = QVBoxLayout(self.view_stack)
            self.view_stack_layout.setContentsMargins(0,0,0,0)
            self.view_stack_layout.addWidget(self.table_view)
            self.view_stack_layout.addWidget(self.card_scroll_area)

        self.view_stack.show()
        self.table_view.show()

        self.view_stack.repaint()
        self.table_view.viewport().repaint()
        self.update()
        QApplication.processEvents()

        QMessageBox.information(self, "完了", "強制表示を実行しました。\nテーブルが表示されているか確認してください。")

    def _cleanup_backend(self):
        """バックエンドのクリーンアップ（強化版）"""
        print("DEBUG: _cleanup_backend called.")
        
        # バックエンドのクローズ
        if hasattr(self, 'db_backend') and self.db_backend:
            self.db_backend.close()
            self.db_backend = None
            print("DEBUG: SQLiteBackend closed and cleared.")
        
        if hasattr(self, 'lazy_loader') and self.lazy_loader:
            self.lazy_loader.close()
            self.lazy_loader = None
            print("DEBUG: LazyCSVLoader cleared.")
        
        # 🔥 重要：DataFrameの参照を完全にクリア
        self._df = None
        
        # 🔥 重要：モデルを空の状態にリセット
        if hasattr(self.table_model, 'reset_to_empty'):
            self.table_model.reset_to_empty()
        else:
            # reset_to_emptyメソッドがない場合のフォールバック
            self.table_model.set_dataframe(pd.DataFrame())
        
        # パフォーマンスモードをリセット
        self.performance_mode = False
        
        # ソート情報をクリア
        self._clear_sort()
        
        # 検索ハイライトをクリア
        if hasattr(self, 'search_controller'):
            self.search_controller.clear_search_highlight()
        
        print("DEBUG: Backend cleanup completed.")

    def _auto_open_file_with_dialog(self, filepath):
        """コマンドライン引数で指定されたファイルを自動で開く際に、モード選択ダイアログを表示する"""
        print(f"DEBUG: _auto_open_file_with_dialog called with: {filepath}")

        try:
            # ファイル存在確認とパーミッションエラーは file_io_controller._start_file_loading_process で行われるためここでは不要
            
            encoding = self.file_controller._detect_encoding(filepath)
            if not encoding:
                encoding = 'shift_jis'
            
            file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
            print(f"DEBUG: 自動読み込み時のファイルサイズ: {file_size_mb:.2f} MB")

            selected_mode = 'normal'
            if file_size_mb > config.FILE_SIZE_MODE_SELECTION_THRESHOLD_MB:
                print(f"DEBUG: ファイルサイズ({file_size_mb:.2f}MB)が閾値({config.FILE_SIZE_MODE_SELECTION_THRESHOLD_MB}MB)を超えたため、モード選択ダイアログを表示")
                
                mode_dialog = QDialog(self)
                mode_dialog.setWindowTitle("読み込みモード選択")
                layout = QVBoxLayout(mode_dialog)
                
                info_label = QLabel(f"ファイルサイズが {file_size_mb:.1f} MB と大きいため、\n"
                                   f"適切な読み込みモードを選択してください。")
                layout.addWidget(info_label)
                
                normal_radio = QRadioButton("通常モード (高速だがメモリ使用量大)")
                sqlite_radio = QRadioButton("SQLiteモード (推奨：メモリ効率的)")
                lazy_radio = QRadioButton("遅延読み込みモード (巨大ファイル用)")
                
                memory_ok, memory_msg = self.file_controller._check_memory_feasibility(file_size_mb)

                if file_size_mb > 100 or not memory_ok:
                    sqlite_radio.setChecked(True)
                    if not memory_ok:
                        QMessageBox.warning(self, "メモリ不足",
                                            f"{memory_msg}\nSQLiteモードを推奨します。")
                else:
                    normal_radio.setChecked(True)
                    
                layout.addWidget(normal_radio)
                layout.addWidget(sqlite_radio)
                layout.addWidget(lazy_radio)
                
                button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
                button_box.accepted.connect(mode_dialog.accept)
                button_box.rejected.connect(mode_dialog.reject)
                layout.addWidget(button_box)
                
                if mode_dialog.exec() == QDialog.Accepted:
                    if sqlite_radio.isChecked():
                        selected_mode = 'sqlite'
                    elif lazy_radio.isChecked():
                        selected_mode = 'lazy'
                    else:
                        selected_mode = 'normal'
                else:
                    self.show_operation_status("ファイルの読み込みをキャンセルしました。", 3000)
                    self.view_controller.show_welcome_screen()
                    self.async_manager.cleanup_backend_requested.emit()
                    return

            # FileIOController._start_file_loading_process は async_manager.load_full_dataframe_async を呼び出している
            self.async_manager.load_full_dataframe_async(filepath, encoding, selected_mode)

            if hasattr(self, 'multi_file_list') and len(self.multi_file_list) > 1:
                # 🔥 修正のポイント：複数ファイルも open_new_window_with_file で新しいウィンドウで開く
                remaining_files = [f for f in self.multi_file_list if f != filepath]
                # 遅延実行により、最初のファイルがロードされてから順次開く
                for i, extra_filepath in enumerate(remaining_files):
                    QTimer.singleShot(500 * (i + 1), lambda fp=extra_filepath: self.open_new_window_with_file(fp))
                self.multi_file_list = []

            print(f"DEBUG: ファイル読み込み処理を開始しました（Selected Mode: {selected_mode}）")

        except Exception as e:
            print(f"ERROR: 自動ファイル読み込みでエラー: {e}")
            traceback.print_exc()

            self.view_stack.hide()
            self.welcome_widget.show()
            self.status_label.setText("ファイルを開いてください。")
            self.setWindowTitle("高機能CSVエディタ (PySide6)")

            QMessageBox.critical(
                self,
                "ファイル読み込みエラー",
                f"指定されたファイルを開けませんでした。\n\n"
                f"ファイル: {filepath}\n"
                f"エラー: {str(e)}"
            )

    # 修正4: デバッグ用ショートカットキー確認機能を追加
    def check_shortcuts_status(self):
        """すべてのショートカットキーの動作状態を確認"""
        print("=== ショートカットキー動作確認 ===")
        
        actions = [
            (self.new_action, "新規作成"),
            (self.open_action, "開く"),
            (self.save_action, "保存"),
            (self.undo_action, "元に戻す"),
            (self.redo_action, "やり直し"),
            (self.copy_action, "コピー"),
            (self.paste_action, "貼り付け"),
            (self.search_action, "検索"),
            (self.sort_asc_action, "昇順ソート"),
            (self.sort_desc_action, "降順ソート"),
            (self.view_toggle_action, "ビュー切り替え"),
            (self.add_row_action, "行追加"), 
            (self.add_column_action, "列追加"), 
            (self.delete_selected_rows_action, "行削除"), 
            (self.delete_selected_column_action, "列削除"), 
            (self.copy_column_action, "列コピー"), 
            (self.paste_column_action, "列貼り付け"), 
            (self.remove_duplicates_action, "重複行削除"), 
            (self.cell_concatenate_action, "セル連結"), 
            (self.column_concatenate_action, "列連結"), 
            (self.price_calculator_action, "金額計算"), 
            (self.text_processing_action, "テキスト処理"), 
            (self.save_as_action, "名前を付けて保存"), 
            (self.exit_action, "終了"), 
            (self.select_all_action, "全選択"), 
            (self.clear_sort_action, "ソートクリア"), 
        ]
        
        for action, name in actions:
            shortcut = action.shortcut().toString() if action.shortcut() else "なし"
            enabled = action.isEnabled()
            print(f"{name}: {shortcut} - {'有効' if enabled else '無効'}")
    
    # 🔥 新規追加メソッド：メニューアクションのツールチップを現在の状態に応じて更新
    def _update_menu_tooltips(self):
        """メニューアクションのツールチップを現在の状態に応じて更新"""
        # FileIOController の _is_welcome_screen_active メソッドを利用
        is_welcome_screen = self.file_controller._is_welcome_screen_active()

        if is_welcome_screen:
            # ウェルカム画面の場合
            self.open_action.setToolTip("CSVファイルをこのウィンドウで開きます (Ctrl+O)")
            self.open_action.setStatusTip("このウィンドウでCSVファイルを開きます")
            self.new_action.setToolTip("新規CSVファイルをこのウィンドウで作成します (Ctrl+N)")
            self.new_action.setStatusTip("このウィンドウで新規CSVファイルを作成します")
        else:
            # 既存データがある場合
            self.open_action.setToolTip("CSVファイルを新しいウィンドウで開きます (Ctrl+O)")
            self.open_action.setStatusTip("新しいウィンドウでCSVファイルを開きます")
            self.new_action.setToolTip("新規CSVファイルを新しいウィンドウで作成します (Ctrl+N)")
            self.new_action.setStatusTip("新しいウィンドウで新規CSVファイルを作成します")
        
        # ツールバーのツールチップも更新されるように強制的に再設定
        # QAction のツールチップが変更された際に TooltipEventFilter が自動で拾うはずだが、念のため
        if hasattr(self, 'tooltip_filters'):
            for f in self.tooltip_filters:
                if f.target_widget and f.text_callback: # target_widget と text_callback が存在することを確認
                    f.target_widget.setToolTip(f.text_callback()) # 直接ツールチップを更新
                    # QAction に statusTip を設定する
                    if isinstance(f.target_widget, QAction):
                        f.target_widget.setStatusTip(f.text_callback())
                    elif hasattr(f.target_widget, 'setStatusTip'): # QToolButton など
                        f.target_widget.setStatusTip(f.text_callback())


    # 🔥 修正5: 初期化が正常に完了したかを検証するメソッド
    def _validate_initialization(self):
        """アプリケーションの初期化が正常に完了したかを検証"""
        required_attrs = [
            'table_model', 'table_view', 'card_mapper', 'file_controller', 
            'view_controller', 'search_controller', 'async_manager', 
            'table_operations', 'undo_manager', 'parent_child_manager', 
            'search_dock_widget', 'search_panel', 'loading_overlay', 
            'settings_manager'
        ]
        
        missing = []
        for attr in required_attrs:
            if not hasattr(self, attr) or getattr(self, attr) is None:
                missing.append(attr)
        
        if missing:
            print(f"ERROR: 初期化エラー - 以下の必須属性が作成されていないか、Noneです: {missing}")
            return False
        
        # table_view のモデルが正しく設定されているか
        if self.table_view.model() is None:
            print("ERROR: 初期化エラー - table_view にモデルが設定されていません。")
            return False

        # card_mapper のモデルが正しく設定されているか
        if self.card_mapper.model() is None:
            print("ERROR: 初期化エラー - card_mapper にモデルが設定されていません。")
            return False
            
        print("DEBUG: 初期化検証完了 - すべての必須属性が作成され、正しく設定されています。")
        return True


if __name__ == "__main__":
    print(f"DEBUG: アプリケーション開始")
    print(f"DEBUG: コマンドライン引数: {sys.argv}")

    app = QApplication(sys.argv)
    editor = CsvEditorAppQt()
    editor.show()

    print(f"DEBUG: メインウィンドウ表示完了")

    sys.exit(app.exec())