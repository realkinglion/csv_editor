# main_qt.py (コントローラー分割後の最終修正版)

import sys
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QDialog,
    QVBoxLayout, QHBoxLayout, QGridLayout, QFormLayout,
    QLineEdit, QTextEdit, QPlainTextEdit, QComboBox, QCheckBox, QRadioButton,
    QSpinBox, QDoubleSpinBox, QPushButton,
    QLabel, QProgressBar, QTableView, QListWidget,
    QGroupBox, QScrollArea, QDockWidget, QButtonGroup,
    QFileDialog, QMessageBox, QInputDialog, QProgressDialog, QDialogButtonBox,
    QHeaderView, QAbstractItemView, QStyle, QMenu, QSizePolicy,
    QDataWidgetMapper
)
from PySide6.QtGui import QKeySequence, QGuiApplication, QTextOption, QFont, QAction, QPalette
from PySide6.QtCore import Qt, Signal, Slot, QTimer, QModelIndex, QEvent, QItemSelectionModel, QObject, QItemSelection, QSize, QUrl # QUrlを追加

import config
import pandas as pd
import os # osを追加
import csv
import re
import traceback
import math
from io import StringIO

from themes_qt import ThemeQt, DarkThemeQt
from data_model import CsvTableModel
from db_backend import SQLiteBackend
from lazy_loader import LazyCSVLoader
from features import (
    AsyncDataManager,
    UndoRedoManager,
    ParentChildManager
)
from search_widget import SearchWidget

# コントローラーのインポート
from file_io_controller import FileIOController
from view_controller import ViewController
from search_controller import SearchController

# dialogs.py から必要なダイアログクラスをインポート
from dialogs import (
    MergeSeparatorDialog, PriceCalculatorDialog, PasteOptionDialog,
    CSVSaveFormatDialog, TooltipEventFilter, EncodingSaveDialog,
    TextProcessingDialog
)

from ui_main_window import Ui_MainWindow

# 既存のimport文の後に追加
from settings_manager import SettingsManager


class CsvEditorAppQt(QMainWindow):
    """
    アプリケーションのメインロジックを担当するクラス。
    UIの定義はUi_MainWindowクラスから継承する。
    """
    data_fetched = Signal(pd.DataFrame)
    create_extract_window_signal = Signal(pd.DataFrame)
    progress_bar_update_signal = Signal(int)

    def __init__(self, dataframe=None, parent=None, filepath=None, encoding='shift_jis'):
        super().__init__(parent)

        print(f"DEBUG: CsvEditorAppQt 初期化開始")
        print(f"DEBUG: parent = {parent}")
        print(f"DEBUG: self.parent() = {self.parent()}")
        print(f"  - dataframe: {dataframe.shape if dataframe is not None else 'None'}")
        print(f"  - filepath: {filepath}")
        print(f"  - encoding: {encoding}")

        self.tooltip_filters = []
        self.filepath = filepath
        self.encoding = encoding

        # ui_main_window.pyからUIをセットアップ
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
                           'card_view_container', 'welcome_label', # welcome_labelを追加
                           'text_processing_action', 'diagnose_action', 'force_show_action'
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
            if not hasattr(self, 'welcome_label'): self.welcome_label = QLabel("Welcome") # フォールバックを追加
            
            for attr in [a for a in essential_attrs if 'action' in a or 'menu' in a]:
                if not hasattr(self, attr):
                    setattr(self, attr, QAction(self) if 'action' in attr else QMenu(self))
            for attr in ['new_file_button_welcome', 'open_file_button_welcome', 'sample_data_button_welcome']:
                if not hasattr(self, attr):
                    setattr(self, attr, QPushButton(self))


        self.theme = config.CURRENT_THEME
        self.density = config.CURRENT_DENSITY
        
        self._df = dataframe
        self.header = list(self._df.columns) if self._df is not None and not self._df.empty else []
        
        self.lazy_loader = None
        self.db_backend = None
        self.performance_mode = False
        
        self.sort_info = {'column_index': -1, 'order': Qt.AscendingOrder}
        self.column_clipboard = None
        
        # コントローラーの初期化
        self.file_controller = FileIOController(self)
        self.view_controller = ViewController(self)
        self.search_controller = SearchController(self)
        
        # 修正箇所: async_manager の初期化をコントローラーの後に移動
        self.async_manager = AsyncDataManager(self)

        self.table_model = CsvTableModel(self._df, self.theme)
        self.table_model.set_app_instance(self)
        
        self.undo_manager = UndoRedoManager(self)
        self.parent_child_manager = ParentChildManager()

        self.search_dock_widget = None
        self.search_panel = None
        
        self.pulse_timer = QTimer(self)
        self.pulse_timer.setSingleShot(True)
        self.pulsing_cells = set()
        
        self.card_mapper = QDataWidgetMapper(self)
        self.card_mapper.setModel(self.table_model)
        self.card_fields_widgets = {}

        self.settings_manager = SettingsManager()
        
        self.operation_timer = None
        self.progress_dialog = None
        
        self.table_view.setModel(self.table_model)
        self.table_view.verticalHeader().setDefaultSectionSize(self.density['row_height'])

        self.last_selected_index = QModelIndex()
        self.active_index = QModelIndex()
        self.dragging = False

        self._connect_signals()
        self._connect_controller_signals()
        self._create_search_dock_widget()
        self.search_dock_widget.hide()

        self.apply_theme()
        self._set_default_font()
        
        if dataframe is not None:
            self.view_stack.show()
            self.welcome_widget.hide()
            self.view_controller.show_main_view()
            self.table_model.set_dataframe(dataframe)
            self.status_label.setText(f"抽出結果 ({len(dataframe):,}行)")
            self.setWindowTitle(f"高機能CSVエディタ (PySide6) - 抽出結果")
            self.table_view.resizeColumnsToContents()
            self._set_ui_state('normal')
            self.view_controller.recreate_card_view_fields()
        else:
            self.view_stack.hide()
            self.welcome_widget.show()
            self.view_controller.show_welcome_screen()

        self.settings_manager.load_window_settings(self)

        print(f"DEBUG: 初期化完了後の状態:")
        print(f"  - view_stack.isVisible(): {self.view_stack.isVisible()}")
        print(f"  - welcome_widget.isVisible(): {self.welcome_widget.isVisible()}")
        print(f"  - table_view.isVisible(): {self.table_view.isVisible()}")

    # ドラッグ＆ドロップイベントハンドラの追加
    def dragEnterEvent(self, event):
        """ドラッグされたアイテムがCSVファイルかチェック"""
        if event.mimeData().hasUrls():
            urls = event.mimeData().urls()
            if urls:
                first_file = urls[0].toLocalFile()
                if first_file.lower().endswith(('.csv', '.txt')):
                    event.acceptProposedAction()
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
        if self.welcome_widget.isVisible():
            self.welcome_label.setStyleSheet("") # スタイルシートをリセット
        event.accept()

    def dropEvent(self, event):
        """CSVファイルがドロップされたときの処理"""
        if self.welcome_widget.isVisible():
            self.welcome_label.setStyleSheet("") # スタイルシートをリセット
            
        if event.mimeData().hasUrls():
            urls = event.mimeData().urls()
            if urls:
                filepath = urls[0].toLocalFile()
                
                if not os.path.exists(filepath): # ファイルの存在確認
                    QMessageBox.warning(self, "ファイルエラー", 
                                        f"ファイルが見つかりません:\n{filepath}")
                    event.ignore()
                    return
                
                if filepath.lower().endswith(('.csv', '.txt')):
                    print(f"DEBUG: ファイルがドロップされました: {filepath}")
                    
                    if self.undo_manager.can_undo(): # 未保存の変更確認
                        reply = QMessageBox.question(
                            self, "確認", 
                            "未保存の変更があります。\n新しいファイルを開きますか？",
                            QMessageBox.Yes | QMessageBox.No,
                            QMessageBox.No
                        )
                        if reply == QMessageBox.No:
                            event.ignore()
                            return
                    
                    self.file_controller.open_file(filepath) # file_controllerに委譲
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


    def _connect_signals(self):
        # QActionの接続
        self.new_action.triggered.connect(self.file_controller.create_new_file)
        self.open_action.triggered.connect(self.file_controller.open_file)
        self.save_action.triggered.connect(lambda: self.file_controller.save_file(filepath=self.filepath, is_save_as=False))
        self.save_as_action.triggered.connect(self.file_controller.save_as_with_dialog)
        self.exit_action.triggered.connect(self.close)

        # ウェルカム画面のQPushButtonの接続
        if hasattr(self, 'new_file_button_welcome') and self.new_file_button_welcome is not None:
            self.new_file_button_welcome.clicked.connect(self.file_controller.create_new_file)
        if hasattr(self, 'open_file_button_welcome') and self.open_file_button_welcome is not None:
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
        self.cut_action.triggered.connect(self._cut)
        self.copy_action.triggered.connect(self._copy)
        self.paste_action.triggered.connect(self._paste)
        self.delete_action.triggered.connect(self._delete)
        self.cell_concatenate_action.triggered.connect(lambda: self._concatenate_cells(is_column_merge=False))
        self.column_concatenate_action.triggered.connect(lambda: self._concatenate_cells(is_column_merge=True))
        self.copy_column_action.triggered.connect(self._copy_columns)
        self.paste_column_action.triggered.connect(self._paste_columns)
        self.add_row_action.triggered.connect(self._add_row)
        self.add_column_action.triggered.connect(self._add_column)
        self.delete_selected_rows_action.triggered.connect(self._delete_selected_rows)
        self.delete_selected_column_action.triggered.connect(self._delete_selected_columns)
        self.sort_asc_action.triggered.connect(lambda: self._sort_by_column(Qt.AscendingOrder))
        self.sort_desc_action.triggered.connect(lambda: self._sort_by_column(Qt.DescendingOrder))
        self.clear_sort_action.triggered.connect(self._clear_sort)
        self.select_all_action.triggered.connect(self._select_all)
        self.search_action.triggered.connect(self._toggle_search_panel)

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

    def _create_search_dock_widget(self):
        if self.search_dock_widget is None:
            self.search_dock_widget = QDockWidget("検索・置換・抽出", self)
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

    def _show_progress_dialog(self, title, on_cancel_slot):
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
            self._close_progress_dialog()
        QApplication.processEvents()

    def _close_progress_dialog(self):
        if self.progress_dialog:
            self.progress_dialog.close()
            self.progress_dialog = None
            
    @Slot(object, str, str)
    def _on_file_loaded(self, data_object, filepath, encoding):
        print(f"DEBUG: _on_file_loaded: ファイル読み込み完了: {filepath}")
        
        if isinstance(data_object, pd.DataFrame):
            self._df = data_object
            self.table_model.set_dataframe(data_object)
            self.performance_mode = False
            total_rows = len(data_object)
        else:
            self.table_model.set_backend(data_object)
            self.performance_mode = True
            total_rows = data_object.get_total_rows()

        self.filepath = filepath
        self.encoding = encoding
        self.header = list(data_object.columns) if isinstance(data_object, pd.DataFrame) else data_object.header
        
        self._set_ui_state('normal')
        self.view_controller.show_main_view()
        
        status_text = f"{os.path.basename(filepath)} ({total_rows:,}行, {len(self.header)}列, {encoding})"
        self.status_label.setText(status_text)
        self.setWindowTitle(f"高機能CSVエディタ (PySide6) - {os.path.basename(filepath)}")
        
        if self.search_panel:
            self.search_panel.update_headers(self.header)
        
        self.view_controller.recreate_card_view_fields()
        
        self._clear_sort()
        
        self.table_view.resizeColumnsToContents()
        
        if self.table_model.rowCount() > 0 and self.table_model.columnCount() > 0:
            first_index = self.table_model.index(0, 0)
            self.table_view.setCurrentIndex(first_index)
            self.table_view.scrollTo(first_index)

        self.show_operation_status("ファイルを読み込みました", 2000)

    @Slot(str)
    def _on_file_saved(self, filepath):
        print(f"DEBUG: _on_file_saved: ファイル保存完了: {filepath}")
        self.filepath = filepath
        self.setWindowTitle(f"高機能CSVエディタ (PySide6) - {os.path.basename(filepath)}")
        self.undo_manager.clear()
        self.update_menu_states()
        self.show_operation_status("ファイルを保存しました", 2000)

    @Slot(str)
    def _on_load_mode_changed(self, mode):
        print(f"DEBUG: _on_load_mode_changed: ロードモードが '{mode}' に変更されました。")

    @Slot(str)
    def _on_view_changed(self, view_type):
        print(f"DEBUG: _on_view_changed: ビューが {view_type} に切り替わりました")
        self._update_action_button_states()

    @Slot(str)
    def _on_context_hint_changed(self, hint_type):
        print(f"DEBUG: _on_context_hint_changed: ヒントタイプが {hint_type} に変更されました。")

    @Slot(int)
    def _on_replace_completed(self, count):
        print(f"DEBUG: _on_replace_completed: 置換完了: {count}件")

    @Slot(object)
    def _on_extract_completed(self, df):
        print(f"DEBUG: _on_extract_completed: 抽出完了: {df.shape if df is not None else 'None'}")


    @Slot(pd.DataFrame)
    def _on_async_data_ready(self, df):
        print(f"WARNING: _on_async_data_ready が呼ばれました（AsyncDataManagerからの直接データ受信）")
        print(f"DEBUG: DataFrame shape: {df.shape if df is not None else 'None'}")
        self._close_progress_dialog()
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

        if load_mode == 'sqlite':
            self.db_backend = self.async_manager.get_backend_instance()
            self.table_model.set_backend(self.db_backend)
            self.header = self.db_backend.header
            total_rows = self.db_backend.get_total_rows()
        elif load_mode == 'lazy':
            self.lazy_loader = self.async_manager.get_backend_instance()
            self.table_model.set_backend(self.lazy_loader)
            self.header = self.lazy_loader.header
            total_rows = self.lazy_loader.get_total_rows()
        else:
            self._df = df
            self.table_model.set_dataframe(df)
            self.header = list(df.columns) if df is not None else []
            total_rows = len(df) if df is not None else 0
            self.performance_mode = False

        if self.search_panel: self.search_panel.update_headers(self.header)
        
        self.view_controller.recreate_card_view_fields()
        self._clear_sort()
        
        current_filepath = self.async_manager.current_filepath if hasattr(self.async_manager, 'current_filepath') else "不明なファイル"
        current_encoding = self.async_manager.current_encoding if hasattr(self.async_manager, 'current_encoding') else "不明"
        
        self.filepath = current_filepath
        self.encoding = current_encoding

        status_text = f"{os.path.basename(self.filepath)} ({total_rows:,}行, {len(self.header)}列, {self.encoding})"
        self.status_label.setText(status_text)
        self.setWindowTitle(f"高機能CSVエディタ (PySide6) - {os.path.basename(self.filepath)}")
        
        self._set_ui_state('normal')
        self.view_controller.show_main_view()
        self.table_view.resizeColumnsToContents()
        print("DEBUG: _on_async_data_ready finished.")

    def test_data(self):
        """サンプルデータを作成して表示する"""
        print("DEBUG: test_data button clicked.起動確認用")
        print("サンプルデータを作成中...")
        
        self._cleanup_backend()
        self.undo_manager.clear()
        
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
        
        self._df = df
        self.header = list(df.columns)
        self.filepath = "test_data.csv"
        self.encoding = 'shift_jis'
        self.performance_mode = False
        
        self.table_model.set_dataframe(df)
        
        if self.search_panel:
            self.search_panel.update_headers(self.header)
        
        self.view_controller.recreate_card_view_fields()
        self._clear_sort()
        
        self.view_controller.show_main_view()
        
        status_text = f"テストデータ ({len(df):,}行, {len(df.columns)}列)"
        self.status_label.setText(status_text)
        self.setWindowTitle("高機能CSVエディタ (PySide6) - テストデータ")
        self.show_operation_status("テストデータを表示しました")
        self.view_toggle_action.setEnabled(True)
    
    def _set_ui_state(self, state):
        is_data_loaded = (state == 'normal')
        self.save_action.setEnabled(is_data_loaded)
        self.save_as_action.setEnabled(is_data_loaded)
        self.edit_menu.setEnabled(is_data_loaded)
        self.tools_menu.setEnabled(is_data_loaded)
        self.csv_format_menu.setEnabled(is_data_loaded)
        self.new_action.setEnabled(True)
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

    @Slot(pd.DataFrame)
    def _create_extract_window_in_ui_thread(self, df):
        """抽出結果を新しいウィンドウで表示"""
        print(f"DEBUG: 新しいウィンドウを作成 - DataFrame shape: {df.shape}")
        
        if df.empty:
            QMessageBox.warning(self, "警告", "抽出結果が空です。")
            return
        
        if not hasattr(self, 'open_windows'):
            self.open_windows = []
        
        parent_encoding = self.encoding if self.encoding else 'shift_jis'
        print(f"DEBUG: 抽出ウィンドウに引き継ぐエンコーディング: {parent_encoding}")
        
        new_window = CsvEditorAppQt(
            dataframe=df,
            encoding=parent_encoding
        )
        self.open_windows.append(new_window)
        new_window.show()
        
        print(f"DEBUG: 新しいウィンドウが作成されました - エンコーディング: {new_window.encoding}")

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
        """アプリケーション終了時の処理"""
        
        self.settings_manager.save_window_settings(self)
        
        if self.undo_manager.can_undo():
            reply = QMessageBox.question(self, "確認", 
                                       "未保存の変更があります。終了しますか？",
                                       QMessageBox.Yes | QMessageBox.No, 
                                       QMessageBox.No)
            if reply == QMessageBox.No:
                event.ignore()
                return
        
        if hasattr(self, 'open_windows'):
            for window in list(self.open_windows):
                window.close()
                if window in self.open_windows:
                    self.open_windows.remove(window)

        self._cleanup_backend()
        event.accept()

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
        self.paste_column_action.setEnabled(has_column_selection and self.column_clipboard is not None and not is_readonly_for_edit)
        
        self.delete_selected_rows_action.setEnabled(has_row_selection and not is_readonly_for_edit)
        self.delete_selected_column_action.setEnabled(has_column_selection and not is_readonly_for_edit)
        
        self.sort_asc_action.setEnabled(has_active_cell and not self.lazy_loader)
        self.sort_desc_action.setEnabled(has_active_cell and not self.lazy_loader)
        self.clear_sort_action.setEnabled(self.sort_info['column_index'] != -1 and not self.lazy_loader)
        
        self.add_row_action.setEnabled(not is_readonly_for_edit)
        self.add_column_action.setEnabled(not is_readonly_for_edit)
        
        self.cell_concatenate_action.setEnabled(has_active_cell and not is_readonly_for_edit)
        self.column_concatenate_action.setEnabled(has_active_cell and not is_readonly_for_edit)

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
        undo_action.setEnabled(self.undo_manager.can_undo() and not is_readonly_for_edit)
        redo_action.setEnabled(self.undo_manager.can_redo() and not is_readonly_for_edit)

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

        sort_menu = menu.addMenu("現在の列をソート")
        sort_menu.setEnabled(not self.is_readonly_mode())
        sort_menu.addAction(self.sort_asc_action)
        sort_menu.addAction(self.sort_desc_action)
        
        if self.sort_info['column_index'] != -1:
            menu.addAction(self.clear_sort_action)
        menu.addSeparator()

        merge_menu = menu.addMenu("連結")
        merge_menu.setEnabled(not self.is_readonly_mode(for_edit=True))
        merge_menu.addAction(self.cell_concatenate_action)
        merge_menu.addAction(self.column_concatenate_action)
        menu.addSeparator()

        selected_rows = selection.selectedRows()
        selected_columns = selection.selectedColumns()
        selected_indexes = selection.selectedIndexes()
        
        if len(selected_rows) > 0 and len(selected_columns) == 0:
            delete_rows_action = QAction(f"{len(selected_rows)}行を削除", self)
            delete_rows_action.triggered.connect(self._delete_selected_rows)
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
                delete_column_action.triggered.connect(self._delete_selected_columns)
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

    def _cut(self):
        if self.is_readonly_mode(for_edit=True): self.show_operation_status("このモードでは切り取りはできません。", is_error=True); return
        self._copy(); self._delete()

    def _copy(self):
        selected = self.table_view.selectionModel().selectedIndexes()
        if not selected: return

        min_r = min(idx.row() for idx in selected)
        max_r = max(idx.row() for idx in selected)

        selected_col_indices = sorted(list(set(idx.column() for idx in selected)))
        selected_col_names = [self.table_model.headerData(idx, Qt.Horizontal) for idx in selected_col_indices]

        df_selected_rows = self.table_model.get_rows_as_dataframe(list(range(min_r, max_r + 1)))

        df_to_copy = df_selected_rows[selected_col_names]

        output = StringIO()
        df_to_copy.to_csv(output, sep='\t', index=False, header=False)
        QApplication.clipboard().setText(output.getvalue().strip())
        output.close()

        self.show_operation_status(f"{len(selected)}個のセルをコピーしました")


    def _paste(self):
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードでは貼り付けはできません。", is_error=True); return
        
        selection = self.table_view.selectionModel()
        clipboard_text = QApplication.clipboard().text()
        if not clipboard_text:
            self.show_operation_status("クリップボードにデータがありません。", is_error=True); return
        
        selected_indexes = selection.selectedIndexes()
        if not selected_indexes:
            self.show_operation_status("貼り付け開始位置を選択してください。", is_error=True); return
        
        start_row = min(idx.row() for idx in selected_indexes)
        start_col = min(idx.column() for idx in selected_indexes)
        
        num_model_rows = self.table_model.rowCount()
        num_model_cols = self.table_model.columnCount()

        pasted_df_raw = None
        try:
            pasted_df_raw = pd.read_csv(StringIO(clipboard_text), sep='\t', header=None, dtype=str, on_bad_lines='skip').fillna('')
        except Exception as e:
            print(f"Initial clipboard parsing failed with tab delimiter: {e}")
            pass

        is_single_value_clipboard = False
        if pasted_df_raw is None or pasted_df_raw.empty or (pasted_df_raw.shape[0] == 1 and pasted_df_raw.shape[1] == 1):
            is_single_value_clipboard = True
            pasted_df_raw = pd.DataFrame([[clipboard_text.strip()]], dtype=str)
            print(f"DEBUG: クリップボードは単一値と判定: '{pasted_df_raw.iloc[0,0]}'")

        num_pasted_rows_raw = pasted_df_raw.shape[0] if pasted_df_raw is not None else 0
        num_pasted_cols_raw = pasted_df_raw.shape[1] if pasted_df_raw is not None else 0

        paste_dialog = PasteOptionDialog(self, not is_single_value_clipboard and num_pasted_cols_raw > 1)
        if paste_dialog.exec() != QDialog.Accepted:
            return

        paste_mode = paste_dialog.get_selected_mode()
        custom_delimiter = paste_dialog.get_custom_delimiter()

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
                self.show_operation_status(f"カスタム区切り文字での解析に失敗しました: {e}", is_error=True); return
        
        if pasted_df is None: return

        num_pasted_rows, num_pasted_cols = pasted_df.shape
        print(f"DEBUG: 貼り付け対象データ形状: {num_pasted_rows}行, {num_pasted_cols}列")

        changes = []
        
        selected_rows_indices = sorted(list(set(idx.row() for idx in selected_indexes)))
        selected_cols_indices = sorted(list(set(idx.column() for idx in selected_indexes)))
        
        is_full_row_selection = (len(selected_rows_indices) == 1 and len(selected_cols_indices) == num_model_cols)
        is_full_column_selection = (len(selected_cols_indices) == 1 and len(selected_rows_indices) == num_model_rows)

        if is_single_value_clipboard:
            value_to_paste = pasted_df.iloc[0, 0]
            print(f"DEBUG: 単一値貼り付けモード: '{value_to_paste}'")

            if is_full_column_selection:
                target_col = selected_cols_indices[0]
                print(f"DEBUG: 1セルコピー → 1列全体選択 (列: {target_col})")
                for r_off in range(num_model_rows):
                    target_row = r_off
                    idx = self.table_model.index(target_row, target_col)
                    old_value = self.table_model.data(idx, Qt.EditRole)
                    if str(old_value) != value_to_paste:
                        changes.append({'item': str(target_row), 'column': self.table_model.headerData(target_col, Qt.Horizontal), 'old': str(old_value), 'new': value_to_paste})
            elif is_full_row_selection:
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
            print(f"DEBUG: 複数セル貼り付けモード")
            for r_off in range(num_pasted_rows):
                for c_off in range(num_pasted_cols):
                    r, c = start_row + r_off, start_col + c_off

                    if r < num_model_rows and c < num_model_cols:
                        idx = self.table_model.index(r, c)
                        old_value = self.table_model.data(idx, Qt.EditRole)
                        new_value = pasted_df.iloc[r_off, c_off]
                        if str(old_value) != new_value:
                            changes.append({'item': str(r), 'column': self.table_model.headerData(c, Qt.Horizontal), 'old': str(old_value), 'new': new_value})
                    elif r >= num_model_rows:
                        pass
                    elif c >= num_model_cols:
                        pass
        
        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action); self.apply_action(action, False); self.show_operation_status(f"{len(changes)}個のセルを貼り付けました。")
        else:
            self.show_operation_status("貼り付けによる変更はありませんでした。", 2000)

    def _delete(self):
        if self.is_readonly_mode(for_edit=True): self.show_operation_status("このモードでは削除はできません。", is_error=True); return
        selected = self.table_view.selectionModel().selectedIndexes()
        if not selected: return
        changes = []
        for i in selected:
            current_value = self.table_model.data(i, Qt.DisplayRole)
            if current_value:
                changes.append({'item': str(i.row()), 'column': self.table_model.headerData(i.column(), Qt.Horizontal), 'old': str(current_value), 'new': ""})
        
        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action); self.apply_action(action, False); self.show_operation_status(f"{len(changes)}個のセルをクリアしました。")
        else:
            self.show_operation_status("削除する対象のセルがありませんでした。", 2000)

    def _select_all(self):
        self.table_view.selectAll()
        self._update_action_button_states()

    def _custom_key_press_event(self, event):
        current_index = self.table_view.currentIndex()
        
        if self.view_controller.current_view == 'card':
            if event.modifiers() & Qt.ControlModifier:
                if event.key() == Qt.Key_Left:
                    self.view_controller.go_to_prev_record()
                    event.accept()
                    return
                elif event.key() == Qt.Key_Right:
                    self.view_controller.go_to_next_record()
                    event.accept()
                    return

        super(QTableView, self.table_view).keyPressEvent(event)
        
        new_index = self.table_view.currentIndex()
        if current_index != new_index: self._pulse_cells([new_index])
        
        if event.key() in [Qt.Key_Return, Qt.Key_Enter, Qt.Key_F2] and new_index.isValid():
            self.view_controller.show_context_hint('editing')
            self.table_view.edit(new_index)

        self._update_action_button_states()

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


    def _add_row(self):
        if self.is_readonly_mode(for_edit=True): self.show_operation_status("このモードでは行を追加できません。", is_error=True); return
        current_index = self.table_view.currentIndex()
        row_pos = current_index.row() + 1 if current_index.isValid() else self.table_model.rowCount()
        action = {'type': 'add_row', 'data': {'row_pos': row_pos}}
        self.undo_manager.add_action(action); self.apply_action(action, is_undo=False)
        self.show_operation_status(f"{row_pos + 1}行目に行を追加しました。")

    def _add_column(self):
        if self.is_readonly_mode(for_edit=True): self.show_operation_status("このモードでは列を追加できません。", is_error=True); return
        
        if self.db_backend:
            reply = QMessageBox.question(self, "確認",
                                       "データベースモードでの列追加は元に戻す(Undo)のに時間がかかる場合があります。\n続行しますか？",
                                       QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            if reply == QMessageBox.No:
                return
        
        col_name, ok = QInputDialog.getText(self, "新しい列の作成", "新しい列の名前を入力してください:")
        if not (ok and col_name): return
        if col_name in self.table_model._headers:
            self.show_operation_status(f"列名 '{col_name}' は既に存在します。", is_error=True)
            QMessageBox.warning(self, "エラー", f"列名 '{col_name}' は既に存在します。")
            return

        current_index = self.table_view.currentIndex()
        col_pos = current_index.column() + 1 if current_index.isValid() else self.table_model.columnCount()
        
        col_names_before = list(self.table_model._headers)
        new_headers_temp = list(self.table_model._headers)
        new_headers_temp.insert(col_pos, col_name)
        col_names_after = new_headers_temp

        action = {'type': 'add_column', 'data': {'col_pos': col_pos, 'col_name': col_name, 'col_names_before': col_names_before, 'col_names_after': col_names_after}}
        self.undo_manager.add_action(action); self.apply_action(action, is_undo=False)
        self.show_operation_status(f"列 '{col_name}' を追加しました。")
        self.view_controller.recreate_card_view_fields()

    def _delete_selected_rows(self):
        if self.is_readonly_mode(for_edit=True): self.show_operation_status("このモードでは行を削除できません。", is_error=True); return
        selected_rows = sorted(list({idx.row() for idx in self.table_view.selectionModel().selectedIndexes()}), reverse=True)
        if not selected_rows: self.show_operation_status("削除する行を選択してください。", is_error=True); return
        reply = QMessageBox.question(self, "行の削除", f"{len(selected_rows)}行を削除しますか？\nこの操作は元に戻せません。", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.No: return
        
        if self.db_backend and hasattr(self.db_backend, 'remove_rows'):
            self.db_backend.remove_rows(selected_rows)
            self.table_model.beginResetModel()
            self.table_model.endResetModel()
        else:
            for row in selected_rows:
                self.table_model.removeRows(row, 1)

        self.show_operation_status(f"{len(selected_rows)}行を削除しました。")
    
    def _delete_selected_columns(self):
        selected_columns = self.table_view.selectionModel().selectedColumns()
        if not selected_columns:
            self.show_operation_status("削除する列を選択してください。", is_error=True)
            return
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードでは列を削除できません。", is_error=True); return
        
        col_idx = selected_columns[0].column()
        col_name = self.table_model.headerData(col_idx, Qt.Horizontal)
        
        warning_message = f"列「{col_name}」を削除しますか？\nこの操作は元に戻せます。"
        if self.db_backend:
            warning_message += "\n\n注意: データベースモードでの列削除は元に戻す(Undo)のに時間がかかる場合があります。"
            
        if QMessageBox.question(self, "列の削除", warning_message, QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            col_data = []
            if not self.db_backend:
                col_data = self.table_model.get_column_data(col_idx)
            
            col_names_before = list(self.table_model._headers)
            new_headers_after_delete = [h for h in col_names_before if h != col_name]
            col_names_after = new_headers_after_delete

            action = {'type': 'delete_column', 'data': {'col_idx': col_idx, 'col_name': col_name, 'col_data': col_data, 'col_names_before': col_names_before, 'col_names_after': col_names_after}}
            self.undo_manager.add_action(action); self.apply_action(action, False)
            self.show_operation_status(f"列「{col_name}」を削除しました。")

    def _open_price_calculator(self):
        if self.table_model.rowCount() == 0:
            self.show_operation_status("操作対象のデータがありません。", 3000, is_error=True); return
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードでは金額計算ツールを実行できません。", is_error=True); return

        dialog = PriceCalculatorDialog(self, self.table_model._headers)
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
            
        tax_rate = 1.10
        discount_multiplier = 1.0 - (discount / 100.0)
            
        try:
            target_col_index = self.table_model._headers.index(target_col)
        except ValueError:
            self.show_operation_status(f"列 '{target_col}' が見つかりません。", is_error=True)
            return
            
        print(f"DEBUG: 対象列のインデックス: {target_col_index}")
            
        changes = []
        processed_count = 0
        error_count = 0
            
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
                        'old': str(original_value_str),
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

    def _concatenate_cells(self, is_column_merge=False):
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードではセル結合/列連結はできません。", is_error=True); return

        current_index = self.table_view.currentIndex()
        if not current_index.isValid():
            self.show_operation_status("連結するセルを選択してください。", is_error=True); return

        current_row = current_index.row()
        current_col = current_index.column()

        if is_column_merge:
            if current_col + 1 >= self.table_model.columnCount():
                self.show_operation_status("連結する隣の列がありません。", is_error=True); return

            dialog = MergeSeparatorDialog(self, is_column_merge=True)
            if dialog.exec() != QDialog.Accepted:
                return
            separator = dialog.get_separator()

            changes = []
            current_col_name = self.table_model.headerData(current_col, Qt.Horizontal)
            next_col_name = self.table_model.headerData(current_col + 1, Qt.Horizontal)

            for row_idx in range(self.table_model.rowCount()):
                current_cell_index = self.table_model.index(row_idx, current_col)
                current_value = str(self.table_model.data(current_cell_index, Qt.DisplayRole) or "")

                next_cell_index = self.table_model.index(row_idx, current_col + 1)
                next_value = str(self.table_model.data(next_cell_index, Qt.DisplayRole) or "")

                if current_value and next_value:
                    new_value = f"{current_value}{separator}{next_value}"
                elif current_value:
                    new_value = current_value
                elif next_value:
                    new_value = next_value
                else:
                    new_value = ""

                if current_value != new_value:
                    changes.append({
                        'item': str(row_idx),
                        'column': current_col_name,
                        'old': current_value,
                        'new': new_value
                    })

                if next_value:
                    changes.append({
                        'item': str(row_idx),
                        'column': next_col_name,
                        'old': next_value,
                        'new': ""
                    })

            if changes:
                action = {'type': 'edit', 'data': changes}
                self.undo_manager.add_action(action)
                self.apply_action(action, is_undo=False)
                self.show_operation_status(f"列「{current_col_name}」と「{next_col_name}」を連結し、「{next_col_name}」をクリアしました（{len([c for c in changes if c['column'] == current_col_name])}行）。")
            else:
                self.show_operation_status("連結による変更はありませんでした。", 2000)

        else:
            if current_col + 1 >= self.table_model.columnCount():
                self.show_operation_status("連結する隣のセルがありません。", is_error=True); return

            dialog = MergeSeparatorDialog(self, is_column_merge=False)
            if dialog.exec() != QDialog.Accepted:
                return
            separator = dialog.get_separator()

            current_value = str(self.table_model.data(current_index, Qt.DisplayRole) or "")

            next_index = self.table_model.index(current_row, current_col + 1)
            next_value = str(self.table_model.data(next_index, Qt.DisplayRole) or "")

            if current_value and next_value:
                new_value = f"{current_value}{separator}{next_value}"
            elif current_value:
                new_value = current_value
            elif next_value:
                new_value = next_value
            else:
                new_value = ""

            changes = []
            current_col_name = self.table_model.headerData(current_col, Qt.Horizontal)
            next_col_name = self.table_model.headerData(current_col + 1, Qt.Horizontal)

            if current_value != new_value:
                changes.append({
                    'item': str(current_row),
                    'column': current_col_name,
                    'old': current_value,
                    'new': new_value
                })

            if next_value:
                changes.append({
                    'item': str(current_row),
                    'column': next_col_name,
                    'old': next_value,
                    'new': ""
                })

            if changes:
                action = {'type': 'edit', 'data': changes}
                self.undo_manager.add_action(action)
                self.apply_action(action, is_undo=False)
                self.show_operation_status("セルを連結し、隣のセルをクリアしました。")
            else:
                self.show_operation_status("連結による変更はありませんでした。", 2000)

    def _show_shortcuts(self): QMessageBox.information(self, "ショートカットキー", 
        """
        ファイル操作:
        Ctrl+O: ファイルを開く
        Ctrl+S: ファイルを保存
        Ctrl+Shift+S: 名前を付けて保存
        Ctrl+Q: アプリケーションを終了

        編集操作:
        Ctrl+Z: 元に戻す (Undo)
        Ctrl+Y: やり直し (Redo)
        Ctrl+X: 切り取り
        Ctrl+C: コピー
        Ctrl+V: 貼り付け
        Del: 選択セルをクリア
        Ctrl+A: 全選択
        Ctrl+F: 検索パネル表示/非表示

        行/列操作:
        Ctrl++ (CtrlとShiftと=): 行を追加
        Ctrl+- (Ctrlと-): 選択行を削除
        Ctrl+Shift++: 列を追加
        Ctrl+Shift+-: 選択列を削除
        
        ソート:
        Ctrl+Up: 現在の列を昇順ソート
        Ctrl+Down: 現在の列を降順ソート
        Ctrl+Backspace: ソートをクリア
        
        ビュー切り替え:
        Ctrl+Tab: テーブルビュー/カードビュー切り替え
        
        カードビュー内移動:
        Ctrl+Left: 前のレコード
        Ctrl+Right: 次のレコード
        """)

    def _copy_columns(self):
        selected_columns = self.table_view.selectionModel().selectedColumns()
        if not selected_columns:
            self.show_operation_status("コピーする列を選択してください。", is_error=True); return
        
        col_index = selected_columns[0].column()
        if self.is_readonly_mode(for_edit=True) and self.table_model.rowCount() > 500000:
             QMessageBox.warning(self, "警告", "巨大な列データをメモリにロードします。時間がかかる場合があります。")

        self.column_clipboard = self.table_model.get_column_data(col_index)
        col_name = self.table_model.headerData(col_index, Qt.Horizontal)
        self.show_operation_status(f"列「{col_name}」({len(self.column_clipboard):,}行)をコピーしました。")
        self._update_action_button_states()

    def _paste_columns(self):
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードでは貼り付けできません。", is_error=True); return

        if self.column_clipboard is None:
            self.show_operation_status("貼り付ける列データがありません。先に列をコピーしてください。", is_error=True); return

        selected_columns = self.table_view.selectionModel().selectedColumns()
        if not selected_columns:
            self.show_operation_status("貼り付け先の列を選択してください。", is_error=True); return

        dest_col_index = selected_columns[0].column()
        dest_col_name = self.table_model.headerData(dest_col_index, Qt.Horizontal)
        
        num_rows_to_paste = len(self.column_clipboard)
        if num_rows_to_paste != self.table_model.rowCount():
            reply = QMessageBox.question(self, "行数不一致の確認",
                                       f"コピー元の行数({num_rows_to_paste:,})と現在の行数({self.table_model.rowCount():,})が異なります。\n\n可能な限り貼り付けますか？",
                                       QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            if reply == QMessageBox.No:
                return

        changes = []
        paste_limit = min(num_rows_to_paste, self.table_model.rowCount())

        for i in range(paste_limit):
            old_val = self.table_model.data(self.table_model.index(i, dest_col_index), Qt.EditRole)
            new_val = self.column_clipboard[i]
            if str(old_val) != str(new_val):
                changes.append({'item': str(i), 'column': dest_col_name, 'old': old_val, 'new': str(new_val)})
        
        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action)
            self.apply_action(action, is_undo=False)
            self.show_operation_status(f"{len(changes)}件を列「{dest_col_name}」に貼り付けました。")
        else:
            self.show_operation_status("変更はありませんでした。", 2000)

    def _toggle_search_panel(self):
        """検索パネルの表示/非表示を切り替える"""
        if self.table_model.rowCount() == 0:
            self.show_operation_status("操作対象のデータがありません。", 3000, is_error=True)
            return
        if self.search_dock_widget is None:
            self._create_search_dock_widget()

        self.search_panel.update_headers(self.table_model._headers)
        if self.search_dock_widget.isHidden():
            self.search_dock_widget.show()
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
            # Undo/Redo履歴に追加
            undo_data = []
            for change in changes:
                # changes の要素は {row_idx, col_name, new_value} または {item, column, old, new}
                # ここでは {item, column, old, new} 形式に統一する必要がある
                # ProductDiscountTaskからは changes が {row_idx, col_name, new_value} 形式で返るため、old_valueが必要
                # 一旦、ProductDiscountTask で old_value も返すように修正するか、
                # ここで再度 old_value を取得する
                # 今回は main_qt.py で old_value を取得するロジックを簡素化するため、
                # features.py の ProductDiscountTask._process_with_dataframe/_process_with_backend で
                # changes リストに 'old' を含めるように変更しました。
                undo_data.append({
                    'item': str(change['row_idx']) if 'row_idx' in change else change['item'],
                    'column': change['col_name'] if 'col_name' in change else change['column'],
                    'old': change['old_value'] if 'old_value' in change else change['old'],
                    'new': change['new_value'] if 'new_value' in change else change['new']
                })

            action = {'type': 'edit', 'data': undo_data} # 修正: undo_data を渡す
            self.undo_manager.add_action(action)
            # apply_action は db_backend の場合は内部で update_cells を呼ぶ
            self.apply_action(action, is_undo=False)
            self.show_operation_status(status_message)

    def _apply_replace_from_file(self, params: dict):
        """ファイル参照置換の実行処理"""
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードではファイル参照置換を実行できません。", 3000, is_error=True)
            return
        
        self._show_progress_dialog("ファイル参照置換を実行中...", self.async_manager.cancel_current_task)
        self.async_manager.replace_from_file_async(self.db_backend, self.table_model.get_dataframe(), params)

    def _apply_product_discount(self, params):
        """商品別割引適用の実行処理"""
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードでは商品別割引適用を実行できません。", 3000, is_error=True)
            return
            
        if not params['current_product_col'] or not params['current_price_col']:
            self.show_operation_status("現在ファイルの商品番号列と金額列を選択してください。", is_error=True)
            return
        
        if not params['discount_filepath']:
            self.show_operation_status("参照ファイルを選択してください。", is_error=True)
            return
        
        if not params['ref_product_col'] or not params['ref_discount_col']:
            self.show_operation_status("参照ファイルの商品番号列と割引率列を選択してください。", is_error=True)
            return
        
        if params.get('preview', False):
            pass
            
        self._show_progress_dialog("商品別割引適用を実行中...", self.async_manager.cancel_current_task)
        self.async_manager.product_discount_async(self.db_backend, self.table_model, params)

    def _analyze_parent_child_from_widget(self):
        """検索パネルからの親子関係分析要求処理"""
        settings = self.search_panel.get_settings()
        column_name = settings.get("key_column")
        analysis_mode = settings.get("analysis_mode", "consecutive") # デフォルト値を設定

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
            # DBバックエンドがある場合
            self.async_manager.analyze_parent_child_async(self.db_backend, column_name, analysis_mode)
        else:
            # DataFrameモードの場合
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

    def _show_card_view(self, row_idx_in_model):
        self.view_controller._show_card_view(row_idx_in_model)

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
                # DBモードでの変更の場合、data_model の setData 経由ではなく、
                # 直接 backend を更新し、model の layoutChanged を emit する
                # Undo/Redo のために old_value も保持する
                changes_for_db = []
                for c in data:
                    row_idx = int(c['item'])
                    col_name = c['column']
                    new_value = c['old'] if is_undo else c['new']
                    changes_for_db.append({'row_idx': row_idx, 'col_name': col_name, 'new_value': new_value})

                self.db_backend.update_cells(changes_for_db)
                self.table_model.layoutChanged.emit() # モデルが変更されたことを通知
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
            if is_undo:
                for delete_action_data in reversed(data['delete_actions']):
                    self.apply_action({'type': 'delete_column', 'data': delete_action_data['data']}, is_undo=True)
                
                add_col_data = data['add_column_action']['data']
                self.apply_action({'type': 'add_column', 'data': add_col_data}, is_undo=True)

                self.table_model.beginResetModel()
                self.table_model._headers = data['final_headers_at_creation']
                self.table_model.endResetModel()

            else:
                for delete_action_data in data['delete_actions']:
                    self.apply_action({'type': 'delete_column', 'data': delete_action_data['data']}, is_undo=False)
                
                add_col_data = data['add_column_action']['data']
                self.apply_action({'type': 'add_column', 'data': add_col_data}, is_undo=False)
                
                new_col_name = add_col_data['col_name']
                try:
                    new_col_idx = self.table_model._headers.index(new_col_name)
                    edit_changes = []
                    for change in data['edit_action_data']:
                        row_idx = int(change['item'])
                        old_value_current = self.table_model.data(self.table_model.index(row_idx, new_col_idx), Qt.EditRole)
                        if str(old_value_current) != change['new']:
                            edit_changes.append({
                                'item': str(row_idx),
                                'column': new_col_name,
                                'old': str(old_value_current),
                                'new': change['new']
                            })
                    if edit_changes:
                        for change_item in edit_changes:
                            r_idx = int(change_item['item'])
                            c_idx = self.table_model._headers.index(change_item['column'])
                            self.table_model.setData(self.table_model.index(r_idx, c_idx), change_item['new'], Qt.EditRole)

                except ValueError:
                    print(f"Error: Merged column '{new_col_name}' not found after re-creation.")
                    self.show_operation_status(f"列連結の適用中にエラーが発生しました: '{new_col_name}'が見つかりません。", is_error=True)


        self.show_operation_status(f"操作を{'元に戻しました' if is_undo else '実行しました'}"); self._update_action_button_states()

    def _create_menu_bar(self):
        pass

    def _test_save_as_menu(self):
        """名前を付けて保存メニューのテスト (file_controllerに委譲)"""
        print("DEBUG: _test_save_as_menu called")
        self.file_controller.save_as_with_dialog()

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
        """バックエンドのクリーンアップ"""
        print("DEBUG: _cleanup_backend called.")
        if hasattr(self, 'db_backend') and self.db_backend:
            self.db_backend.close()
            self.db_backend = None
            print("DEBUG: SQLiteBackend closed and cleared.")
        if hasattr(self, 'lazy_loader') and self.lazy_loader:
            self.lazy_loader.close() # 追加: LazyCSVLoader の close メソッドを呼び出す
            self.lazy_loader = None
            print("DEBUG: LazyCSVLoader cleared.")
        self._df = None
        if hasattr(self.table_model, 'set_dataframe'):
            self.table_model.set_dataframe(pd.DataFrame())
        self.performance_mode = False
        self._clear_sort()
        self.search_controller.clear_search_highlight()
        print("DEBUG: Backend cleanup completed.")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    editor = CsvEditorAppQt()
    editor.show()
    sys.exit(app.exec())