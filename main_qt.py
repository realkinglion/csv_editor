# main_qt.py

import sys
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QMenu, QFileDialog, QMessageBox,
    QProgressBar, QTableView, QHeaderView, QAbstractItemView, QStyle,
    QInputDialog, QDialog, QDockWidget, QFormLayout, QTextEdit, QPlainTextEdit,
    QDataWidgetMapper, QLabel
)
from PySide6.QtGui import QKeySequence, QGuiApplication, QTextOption
from PySide6.QtCore import Qt, Signal, Slot, QTimer, QModelIndex, QEvent, QItemSelectionModel, QItemSelection, QObject

import config
import pandas as pd
import os
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
from dialogs import (
    MergeSeparatorDialog, PriceCalculatorDialog, PasteOptionDialog,
    CSVSaveFormatDialog, TooltipEventFilter, EncodingSaveDialog
)
from ui_main_window import Ui_MainWindow


class CsvEditorAppQt(QMainWindow, Ui_MainWindow):
    """
    アプリケーションのメインロジックを担当するクラス。
    UIの定義はUi_MainWindowクラスから継承する。
    """
    data_fetched = Signal(pd.DataFrame)
    progress_bar_update_signal = Signal(int)
    create_extract_window_signal = Signal(pd.DataFrame)

    def __init__(self, dataframe=None, parent=None, filepath=None, encoding='utf-8'):
        super().__init__(parent)

        # setupUiで必要になるメンバ変数を先に初期化する
        self.tooltip_filters = []
        self.filepath = filepath

        # UIのセットアップ (ui_main_window.pyから)
        self.setupUi(self)
        
        # UI構築後に初期化が必要なメンバ変数
        self.theme = config.CURRENT_THEME
        self.density = config.CURRENT_DENSITY
        
        self._df = dataframe
        self.encoding = encoding
        self.header = list(self._df.columns) if self._df is not None and not self._df.empty else []
        
        self.lazy_loader = None
        self.db_backend = None
        self.performance_mode = False
        
        self.sort_info = {'column_index': -1, 'order': Qt.AscendingOrder}
        self.column_clipboard = None
        
        self.async_manager = AsyncDataManager(self)
        self.table_model = CsvTableModel(self._df, self.theme)
        self.table_model.set_app_instance(self)
        
        self.undo_manager = UndoRedoManager(self)
        self.parent_child_manager = ParentChildManager()

        self.search_results = []
        self.current_search_index = -1
        
        self.search_dock_widget = None
        self.search_panel = None
        
        self.pulse_timer = QTimer(self)
        self.pulse_timer.setSingleShot(True)
        self.pulsing_cells = set()
        
        self.card_mapper = QDataWidgetMapper(self)
        self.card_mapper.setModel(self.table_model)
        self.card_fields_widgets = {}

        self._pending_replace_all = False
        self._pending_replace_settings = None
        self._pending_extract = False
        self._pending_extract_settings = None
        self._last_search_settings = None
        
        self.operation_timer = None
        
        # モデルの設定とシグナル接続
        self.table_view.setModel(self.table_model)
        self.table_view.verticalHeader().setDefaultSectionSize(self.density['row_height'])

        self.last_selected_index = QModelIndex() 
        self.active_index = QModelIndex() 
        self.dragging = False

        self._connect_signals()

        # 初期表示処理
        self.apply_theme()
        
        if dataframe is not None:
             self.show_main_view()
             self.table_model.set_dataframe(dataframe)
             self.status_label.setText(f"抽出結果 ({len(dataframe):,}行)")
             self.setWindowTitle(f"高機能CSVエディタ (PySide6) - 抽出結果")
             self.table_view.resizeColumnsToContents()
             self._set_ui_state('normal')
        else:
            self.show_welcome_screen()


    def _connect_signals(self):
        """UIウィジェットのシグナルとロジックのスロットを接続する"""
        # ファイルメニュー
        self.open_action.triggered.connect(self.open_file)
        self.save_action.triggered.connect(self.save_file)
        self.save_as_action.triggered.connect(self.save_file_as)
        self.exit_action.triggered.connect(self.close)
        
        # 編集メニュー
        self.undo_action.triggered.connect(self._undo)
        self.redo_action.triggered.connect(self._redo)
        self.cut_action.triggered.connect(self._cut)
        self.copy_action.triggered.connect(self._copy)
        self.paste_action.triggered.connect(self._paste)
        self.delete_action.triggered.connect(self._delete_selected)
        self.cell_concatenate_action.triggered.connect(self._request_cell_concatenate)
        self.column_concatenate_action.triggered.connect(self._request_column_concatenate)
        self.copy_column_action.triggered.connect(self.copy_selected_column)
        self.paste_column_action.triggered.connect(self.paste_to_selected_column)
        self.add_row_action.triggered.connect(self._add_row)
        self.add_column_action.triggered.connect(self._add_column)
        self.delete_selected_rows_action.triggered.connect(self.delete_selected_rows)
        self.delete_selected_column_action.triggered.connect(self._delete_selected_column)
        self.sort_asc_action.triggered.connect(lambda: self._sort_by_column(self.table_view.currentIndex().column(), Qt.AscendingOrder))
        self.sort_desc_action.triggered.connect(lambda: self._sort_by_column(self.table_view.currentIndex().column(), Qt.DescendingOrder))
        self.clear_sort_action.triggered.connect(self._clear_sort)
        self.select_all_action.triggered.connect(self._select_all)
        self.search_action.triggered.connect(self._show_search_panel)

        # ツールメニュー
        self.price_calculator_action.triggered.connect(self._open_price_calculator_dialog)
        
        # CSVフォーマットメニュー
        self.save_format_action.triggered.connect(self.save_file_as) 

        # ヘルプメニュー
        self.shortcuts_action.triggered.connect(self._show_shortcuts_help)

        # ツールバー
        self.view_toggle_action.triggered.connect(self._toggle_view)
        self.test_action.triggered.connect(self.test_data)
        
        # ウェルカム画面のボタン
        self.open_file_button_welcome.clicked.connect(self.open_file)
        self.sample_data_button_welcome.clicked.connect(self.test_data)
        
        # テーブルビュー
        self.table_view.horizontalHeader().sectionClicked.connect(self._on_column_header_clicked)
        self.table_view.selectionModel().selectionChanged.connect(self._update_action_button_states)
        self.table_view.clicked.connect(self._on_cell_clicked)
        self.table_view.pressed.connect(self._on_cell_pressed)
        self.table_view.viewport().setMouseTracking(True)
        self.table_view.viewport().installEventFilter(self)
        self.table_view.customContextMenuRequested.connect(self._show_context_menu)
        self.table_view.keyPressEvent = self._custom_key_press_event
        self.table_view.activated.connect(self._start_cell_edit)
        
        # カードビュー
        self.prev_record_button.clicked.connect(lambda: self._move_card_record(-1)) 
        self.next_record_button.clicked.connect(lambda: self._move_card_record(1))

        # 非同期マネージャー
        self.async_manager.data_ready.connect(self.update_view_after_data_fetch)
        self.async_manager.search_results_ready.connect(self.handle_search_results_ready)
        self.async_manager.analysis_results_ready.connect(self.handle_parent_child_analysis_ready)
        self.async_manager.replace_from_file_completed.connect(self.handle_replace_from_file_completed)
        
        # その他
        self.create_extract_window_signal.connect(self._create_extract_window_in_ui_thread)
        self.progress_bar_update_signal.connect(self._update_save_progress_bar)
        self.pulse_timer.timeout.connect(self._end_pulse)

    def _create_search_dock_widget(self):
        self.search_dock_widget = QDockWidget("検索・置換・抽出", self)
        self.search_dock_widget.setAllowedAreas(Qt.LeftDockWidgetArea | Qt.RightDockWidgetArea)
        self.search_panel = SearchWidget(self.table_model._headers, self)
        self.search_dock_widget.setWidget(self.search_panel)
        self.addDockWidget(Qt.RightDockWidgetArea, self.search_dock_widget)
        self.search_panel.analysis_requested.connect(self._analyze_parent_child_from_widget)
        self.search_panel.find_next_clicked.connect(self._find_next)
        self.search_panel.find_prev_clicked.connect(self._find_prev)
        self.search_panel.replace_one_clicked.connect(self._replace_current)
        self.search_panel.replace_all_clicked.connect(self._replace_all)
        self.search_panel.extract_clicked.connect(self._execute_extract)
        
        self.search_panel.replace_from_file_requested.connect(self._apply_replace_from_file) 
        
    def _show_search_panel(self):
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

    def show_operation_status(self, message, duration=3000, is_error=False):
        if is_error:
            self.operation_label.setStyleSheet(f"color: {self.theme.DANGER};")
            self.operation_label.setText(f"❌ {message}")
        else:
            self.operation_label.setStyleSheet(f"color: {self.theme.SUCCESS};")
            self.operation_label.setText(f"✓ {message}")
        
        if self.operation_timer: self.operation_timer.stop()
        self.operation_timer = QTimer(self)
        self.operation_timer.setInterval(duration)
        self.operation_timer.setSingleShot(True)
        self.operation_timer.timeout.connect(self._hide_operation_status)
        self.operation_timer.start()

    def _hide_operation_status(self):
        self.operation_label.setText("")

    def show_context_hint(self, hint_key=None):
        hints = {
            'cell_selected': "💡 Enter/F2で編集 | Deleteでクリア | Ctrl+C/Vでコピー/ペースト",
            'column_selected': "💡 右クリックでメニュー | Ctrl+Shift+Cで列コピー",
            'editing': "⌨️ 編集中... Enterで確定 | Escでキャンセル",
        }
        hint_text = hints.get(hint_key, "")
        self.hint_label.setText(hint_text)

    def _set_ui_state(self, state_name):
        is_data_loaded = state_name == 'normal'
        self.save_action.setEnabled(is_data_loaded)
        self.save_as_action.setEnabled(is_data_loaded)
        self.edit_menu.setEnabled(is_data_loaded)
        self.csv_format_menu.setEnabled(is_data_loaded)
        if is_data_loaded: self._update_action_button_states()

    def show_welcome_screen(self):
        self.table_view.hide()
        self.card_scroll_area.hide()
        self.welcome_widget.show()
        self._set_ui_state('welcome')
        self.status_label.setText("ファイルを開いてください。")
        self.view_toggle_action.setEnabled(False)

    def show_main_view(self):
        self.welcome_widget.hide()
        if self.view_toggle_action.text() == "カードビュー":
            self.table_view.show()
            self.card_scroll_area.hide()
        else:
            self.table_view.hide()
            self.card_scroll_area.show()
        self._set_ui_state('normal')
        self.view_toggle_action.setEnabled(True)

    def apply_theme(self):
        self.setStyleSheet(f"""
            QMainWindow {{ background-color: {self.theme.BG_LEVEL_1}; }}
            QHeaderView::section {{ background-color: {self.theme.BG_LEVEL_2}; color: {self.theme.TEXT_PRIMARY}; padding: 5px; font-weight: bold; }}
            QTableView {{ background-color: {self.theme.BG_LEVEL_0}; alternate-background-color: {self.theme.BG_LEVEL_1}; color: {self.theme.TEXT_PRIMARY}; gridline-color: {self.theme.BG_LEVEL_3}; border: 1px solid {self.theme.BG_LEVEL_3}; }}
            QTableView::item {{ padding: {self.density['padding']}px; }}
            QTableView::item:selected {{ background-color: {self.theme.CELL_SELECT_START}; color: white; }}
            QStatusBar {{ background-color: {self.theme.BG_LEVEL_1}; color: {self.theme.TEXT_PRIMARY}; }}
            QLabel {{ color: {self.theme.TEXT_PRIMARY}; }}
            QPushButton {{ background-color: {self.theme.PRIMARY}; color: {self.theme.BG_LEVEL_0}; border: 1px solid {self.theme.PRIMARY}; padding: {self.density['padding']}px {self.density['padding'] * 2}px; border-radius: 4px; }}
            QPushButton:hover {{ background-color: {self.theme.PRIMARY_HOVER}; }}
            QPushButton:pressed {{ background-color: {self.theme.PRIMARY_ACTIVE}; }}
            QPushButton:disabled {{ background-color: {self.theme.BG_LEVEL_3}; color: {self.theme.TEXT_MUTED}; }}
            QToolBar {{ background-color: {self.theme.BG_LEVEL_1}; spacing: 5px; }}
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
        """)
        font = QApplication.font()
        font.setPointSize(self.density['font_size'])
        QApplication.setFont(font)

    def _cleanup_backend(self):
        if self.db_backend: self.db_backend.close(); self.db_backend = None
        if self.lazy_loader: self.lazy_loader = None
        self._df = None
        self.table_model.set_dataframe(pd.DataFrame())
        self.performance_mode = False
        self._clear_sort()

    def _update_action_button_states(self):
        if not hasattr(self, 'copy_action'): return
        selection = self.table_view.selectionModel()
        has_cell_selection = selection.hasSelection()
        has_column_selection = bool(selection.selectedColumns())
        is_readonly_for_edit = self.is_readonly_mode(for_edit=True)
        has_active_cell = self.table_view.currentIndex().isValid()
        
        self.copy_action.setEnabled(has_cell_selection)
        self.cut_action.setEnabled(has_cell_selection and not is_readonly_for_edit)
        self.delete_action.setEnabled(has_cell_selection and not is_readonly_for_edit)
        self.paste_action.setEnabled(QApplication.clipboard().text() != "" and not is_readonly_for_edit and has_active_cell)
        
        self.copy_column_action.setEnabled(has_column_selection)
        self.paste_column_action.setEnabled(has_column_selection and self.column_clipboard is not None and not is_readonly_for_edit)
        
        self.sort_asc_action.setEnabled(has_active_cell and not self.lazy_loader)
        self.sort_desc_action.setEnabled(has_active_cell and not self.lazy_loader)
        self.clear_sort_action.setEnabled(self.sort_info['column_index'] != -1 and not self.lazy_loader)
        
        self.add_row_action.setEnabled(not is_readonly_for_edit)
        self.add_column_action.setEnabled(not is_readonly_for_edit)
        self.delete_selected_rows_action.setEnabled(bool(selection.selectedRows()) and not is_readonly_for_edit)
        self.delete_selected_column_action.setEnabled(bool(selection.selectedColumns()) and not is_readonly_for_edit)
        
        is_single_cell_selected = len(selection.selectedIndexes()) == 1
        self.cell_concatenate_action.setEnabled(is_single_cell_selected and not is_readonly_for_edit)
        self.column_concatenate_action.setEnabled(has_cell_selection and not is_readonly_for_edit)

        if has_column_selection:
            self.show_context_hint('column_selected')
        elif has_cell_selection:
            self.show_context_hint('cell_selected')
        else:
            self.show_context_hint()

        self.update_menu_states()

    def update_view_after_data_fetch(self, df):
        if self.async_manager.current_load_mode == 'sqlite':
            self.db_backend = self.async_manager.get_backend_instance()
            self.table_model.set_backend(self.db_backend)
            if hasattr(self.db_backend, 'header'):
                self.header = self.db_backend.header
            else:
                self.header = [] 
            self.table_model.set_header(self.header)
            total_rows = self.db_backend.get_total_rows()
            self.performance_mode = True
        elif self.async_manager.current_load_mode == 'lazy':
            self.lazy_loader = self.async_manager.get_backend_instance()
            self.table_model.set_backend(self.lazy_loader)
            if hasattr(self.lazy_loader, 'header'):
                self.header = self.lazy_loader.header
            else:
                self.header = []
            self.table_model.set_header(self.header)
            total_rows = self.lazy_loader.get_total_rows()
            self.performance_mode = True
        else:
            self._df = df
            self.table_model.set_dataframe(df)
            self.header = list(df.columns) if df is not None else []
            total_rows = len(df) if df is not None else 0
            self.performance_mode = False

        if self.search_panel: self.search_panel.update_headers(self.header)
        self._recreate_card_view_fields()
        
        self._clear_sort()
        
        self.status_label.setText(f"ファイルを開きました ({total_rows:,}行)")
        if self.filepath:
            self.setWindowTitle(f"高機能CSVエディタ (PySide6) - {os.path.basename(self.filepath)}")
        
        self.progress_bar.hide()
        self.show_operation_status(f"ファイルを開きました ({total_rows:,}行)", 5000)
        self.table_view.resizeColumnsToContents()
        self._set_ui_state('normal')
        self.show_main_view()

    def open_file(self, filepath=None):
        if not filepath:
            filepath_tuple = QFileDialog.getOpenFileName(self, "CSVファイルを開く", "", "CSVファイル (*.csv);;テキストファイル (*.txt);;すべてのファイル (*.*)")
            if not filepath_tuple[0]: return
            filepath = filepath_tuple[0]

        self._cleanup_backend()
        try:
            encoding = self._detect_encoding(filepath)
            if not encoding: 
                self.show_operation_status("ファイルのエンコーディングを検出できませんでした。", is_error=True)
                QMessageBox.critical(self, "エラー", "ファイルのエンコーディングを検出できませんでした。")
                self.show_welcome_screen()
                return
            
            file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
            load_mode = 'normal'

            if file_size_mb > (config.PERFORMANCE_MODE_THRESHOLD / 1000):
                reply = QMessageBox.question(self, "大きなファイル", f"ファイルサイズが {file_size_mb:.1f} MBと大きいため、パフォーマンスモードの選択を推奨します。\n\n・「はい」: SQLiteモード（推奨：編集も可能）\n・「いいえ」: 遅延読み込みモード（閲覧のみ）\n・「キャンセル」: 読み込みを中止します", QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel, QMessageBox.Yes)
                if reply == QMessageBox.Cancel: return
                load_mode = 'sqlite' if reply == QMessageBox.Yes else 'lazy'

            self.filepath = filepath; self.encoding = encoding
            self.status_label.setText("ファイルを読み込んでいます..."); self.progress_bar.show(); self.progress_bar.setRange(0, 0)
            self.async_manager.load_full_dataframe_async(filepath, encoding, load_mode)
        except Exception as e:
            QMessageBox.critical(self, "ファイル読み込みエラー", f"ファイルの読み込み中に予期せぬエラーが発生しました。\n{e}\n{traceback.format_exc()}"); 
            self.show_welcome_screen()

    def _detect_encoding(self, filepath):
        for enc in ['utf-8-sig', 'utf-8', 'shift_jis', 'cp932', 'euc-jp', 'latin1']:
            try:
                with open(filepath, 'r', encoding=enc) as f: f.read(1024)
                return enc
            except (UnicodeDecodeError, pd.errors.ParserError): continue
        return None

    def _prepare_dataframe_for_save(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        保存前にDataFrame内のテキストデータをクリーンアップする。
        特にShift_JISで問題になる文字を互換文字に置換する。
        """
        def clean_text(text):
            if not isinstance(text, str):
                return text
            
            # 1. 改行関連の特殊文字を標準の改行(\n)に統一
            text = text.replace('\u2029', '\n')  # PARAGRAPH SEPARATOR
            text = text.replace('\ufffc', '\n')  # OBJECT REPLACEMENT CHARACTER

            # 2. その他の見えない、あるいは不要な制御文字を除去
            text = text.replace('\u200b', '')    # ZERO WIDTH SPACE
            
            # 3. Shift_JISで文字化けしやすい文字を互換文字に置換
            sjis_replace_map = {
                # '〜': '～', # 波ダッシュ
                # '−': '-',  # 全角マイナス
                '①': '(1)', '②': '(2)', '③': '(3)', '④': '(4)', '⑤': '(5)',
                '⑥': '(6)', '⑦': '(7)', '⑧': '(8)', '⑨': '(9)', '⑩': '(10)',
                'Ⅰ': 'I', 'Ⅱ': 'II', 'Ⅲ': 'III', 'Ⅳ': 'IV', 'Ⅴ': 'V',
                'Ⅵ': 'VI', 'Ⅶ': 'VII', 'Ⅷ': 'VIII', 'Ⅸ': 'IX', 'Ⅹ': 'X',
                '㈱': '(株)', '㈲': '(有)',
            }
            for k, v in sjis_replace_map.items():
                text = text.replace(k, v)
                
            return text

        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].apply(clean_text)
            
        return df

    def save_file(self):
        if self.filepath is None:
            self.save_file_as()
            return
        
        # ✅ 文字コード選択ダイアログを追加
        encoding_dialog = EncodingSaveDialog(self)
        if encoding_dialog.exec() != QDialog.Accepted:
            return
        save_encoding = encoding_dialog.result_encoding
        
        quoting_style = csv.QUOTE_MINIMAL
        
        if self.performance_mode:
            reply = QMessageBox.information(self, "情報", "現在、パフォーマンスモードです。全データをCSVファイルに直接エクスポートして保存します。\nこの処理には時間がかかる場合がありますが、メモリへの全ロードは行いません。", QMessageBox.Ok | QMessageBox.Cancel)
            if reply == QMessageBox.Cancel: return

        try:
            if self.db_backend:
                total_rows = self.db_backend.get_total_rows()
                self.progress_bar.setRange(0, total_rows)
                self.progress_bar.setValue(0)
                self.progress_bar.show()
                self.show_operation_status("ファイルを保存中...", duration=0)
                self.db_backend.export_to_csv(self.filepath, encoding=save_encoding, quoting_style=quoting_style)  # ✅ save_encodingを使用
                self.progress_bar.hide()
            elif self.lazy_loader:
                QMessageBox.warning(self, "機能制限", "遅延読み込みモードでは直接上書き保存できません。データを全てメモリにロードして保存を試みますが、非常に大きなファイルではメモリ不足になる可能性があります。名前を付けて保存を推奨します。")
                df_to_save = self.table_model.get_dataframe()
                if df_to_save is None or df_to_save.empty: QMessageBox.warning(self, "保存不可", "データが空のため保存できません。"); return
                
                df_to_save = self._prepare_dataframe_for_save(df_to_save)
                df_to_save.to_csv(self.filepath, index=False, encoding=save_encoding, quoting=quoting_style, errors='replace')  # ✅ save_encodingを使用
            else:
                df_to_save = self.table_model.get_dataframe()
                if df_to_save is None or df_to_save.empty: QMessageBox.warning(self, "保存不可", "データが空のため保存できません。"); return

                df_to_save = self._prepare_dataframe_for_save(df_to_save)
                df_to_save.to_csv(self.filepath, index=False, encoding=save_encoding, quoting=quoting_style, errors='replace')  # ✅ save_encodingを使用
            
            self.encoding = save_encoding  # ✅ 選択したエンコーディングを記憶
            self.show_operation_status("ファイルを上書き保存しました"); self.undo_manager.clear(); self.update_menu_states()
        except Exception as e: 
            self.progress_bar.hide()
            self.show_operation_status(f"ファイル保存エラー: {e}", is_error=True)
            QMessageBox.critical(self, "保存エラー", f"ファイルの上書き保存中にエラーが発生しました。\n{e}\n{traceback.format_exc()}")

    def _update_save_progress_bar(self, value):
        if self.progress_bar.maximum() > 0:
            self.progress_bar.setValue(value)
            
    def save_file_as(self):
        if self.table_model.rowCount() == 0:
            QMessageBox.warning(self, "保存不可", "データが空のため保存できません。"); return

        filepath_tuple = QFileDialog.getSaveFileName(self, "名前を付けて保存", self.filepath if self.filepath else "", "CSVファイル (*.csv);;テキストファイル (*.txt);;すべてのファイル (*.*)")
        if not filepath_tuple[0]: return
        filepath = filepath_tuple[0]
        
        encoding_dialog = EncodingSaveDialog(self)
        if encoding_dialog.exec() != QDialog.Accepted:
            return
        save_encoding = encoding_dialog.result_encoding

        format_dialog = CSVSaveFormatDialog(self)
        if format_dialog.exec() != QDialog.Accepted:
            return 
        quoting_style = format_dialog.result

        if self.performance_mode:
            reply = QMessageBox.information(self, "情報", "現在、パフォーマンスモードです。全データをCSVファイルに直接エクスポートして保存します。\nこの処理には時間がかかる場合がありますが、メモリへの全ロードは行いません。", QMessageBox.Ok | QMessageBox.Cancel)
            if reply == QMessageBox.Cancel: return

        try:
            if self.db_backend:
                total_rows = self.db_backend.get_total_rows()
                self.progress_bar.setRange(0, total_rows)
                self.progress_bar.setValue(0)
                self.progress_bar.show()
                self.show_operation_status("ファイルを保存中...", duration=0)
                self.db_backend.export_to_csv(filepath, encoding=save_encoding, quoting_style=quoting_style)
                self.progress_bar.hide()
            elif self.lazy_loader:
                QMessageBox.warning(self, "機能制限", "遅延読み込みモードでは直接名前を付けて保存できません。データを全てメモリにロードして保存を試みますが、非常に大きなファイルではメモリ不足になる可能性があります。")
                df_to_save = self.table_model.get_dataframe()
                if df_to_save is None or df_to_save.empty: QMessageBox.warning(self, "保存不可", "データが空のため保存できません。"); return

                df_to_save = self._prepare_dataframe_for_save(df_to_save)
                df_to_save.to_csv(filepath, index=False, encoding=save_encoding, quoting=quoting_style, errors='replace')
            else:
                df_to_save = self.table_model.get_dataframe()
                if df_to_save is None or df_to_save.empty: QMessageBox.warning(self, "保存不可", "データが空のため保存できません。"); return

                df_to_save = self._prepare_dataframe_for_save(df_to_save)
                df_to_save.to_csv(filepath, index=False, encoding=save_encoding, quoting=quoting_style, errors='replace')

            self.filepath = filepath; self.encoding = save_encoding
            self.setWindowTitle(f"高機能CSVエディタ (PySide6) - {os.path.basename(self.filepath)}")
            self.show_operation_status("ファイルを名前を付けて保存しました"); self.undo_manager.clear(); self.update_menu_states()
        except Exception as e: 
            self.progress_bar.hide()
            self.show_operation_status(f"ファイル保存エラー: {e}", is_error=True)
            QMessageBox.critical(self, "保存エラー", f"ファイルの保存中にエラーが発生しました。\n{e}\n{traceback.format_exc()}")

    def test_data(self):
        self._cleanup_backend(); self.undo_manager.clear()
        header = ["商品名", "価格", "在庫数", "カテゴリ", "商品説明"]
        large_data = []
        for i in range(20):
            large_data.extend([
                {"商品名": f"商品A_{i}", "価格": "100", "在庫数": "50", "カテゴリ": "カテゴリX", "商品説明": f"<b>これは商品A_{i}です。</b><br>送料無料！"},
                {"商品名": f"商品B_{i}", "価格": "120", "在庫数": "30", "カテゴリ": "カテゴリX", "商品説明": f"<i>これは商品B_{i}です。</i>\n改行もOK。"},
                {"商品名": f"商品C_{i}", "価格": "80", "在庫数": "70", "カテゴリ": "カテゴリY", "商品説明": f"<p>これは商品C_{i}です。</p>"},
                {"商品名": f"商品D_{i}", "価格": "150", "在庫数": "20", "カテゴリ": "カテゴリY", "商品説明": f"〜波ダッシュや−マイナス、①などの文字もテスト〜"},
                {"商品名": f"商品E_{i}", "価格": "90", "在庫数": "40", "カテゴリ": "カテゴリZ", "商品説明": ""},
            ])
        df_large = pd.DataFrame(large_data, columns=header)

        self.filepath = "test_data.csv"
        self.encoding = 'shift_jis'  # ✅ この行を追加
        self.update_view_after_data_fetch(df_large)
        self.show_operation_status(f"テストデータをロードしました ({df_large.shape[0]:,}行)")

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            event.ignore()

    def dropEvent(self, event):
        if event.mimeData().hasUrls():
            urls = event.mimeData().urls()
            if urls:
                filepath = urls[0].toLocalFile()
                if filepath.lower().endswith(('.csv', '.txt', '.tsv')):
                    self.open_file(filepath=filepath)
                else:
                    self.show_operation_status("対応しているファイル形式は .csv, .txt, .tsv です。", is_error=True)
                    QMessageBox.warning(self, "非対応ファイル", "対応しているファイル形式は .csv, .txt, .tsv です。")

    def eventFilter(self, source: QObject, event: QEvent) -> bool:
        if self.card_scroll_area.isVisible() and source in self.card_fields_widgets.values():
            if event.type() == QEvent.Type.KeyPress:
                key_event = event
                if key_event.modifiers() & Qt.ControlModifier: 
                    if key_event.key() == Qt.Key_Left:
                        self._move_card_record(-1)
                        return True
                    elif key_event.key() == Qt.Key_Right:
                        self._move_card_record(1)
                        return True

        if source == self.table_view.viewport():
            if event.type() == QEvent.Type.MouseMove and event.buttons() & Qt.LeftButton and self.dragging:
                self._on_cell_dragged(event)
                return True
            elif event.type() == QEvent.Type.MouseButtonRelease and event.button() == Qt.LeftButton and self.dragging:
                self._on_cell_released(event)
                return True

        return super().eventFilter(source, event)

    def _on_cell_pressed(self, index):
        if QApplication.mouseButtons() == Qt.RightButton:
            return
        
        self.dragging = True; self.last_selected_index = index
        self._start_pulse(index)
        modifiers = QApplication.keyboardModifiers()
        if not (modifiers & Qt.ControlModifier or modifiers & Qt.ShiftModifier): self.table_view.selectionModel().setCurrentIndex(index, QItemSelectionModel.ClearAndSelect)
        else: self.table_view.selectionModel().setCurrentIndex(index, QItemSelectionModel.Select)
        self.active_index = index

    def _on_cell_clicked(self, index): self._update_action_button_states()
    def _on_cell_dragged(self, event):
        if not self.last_selected_index.isValid(): return
        index = self.table_view.indexAt(event.position().toPoint())
        if index.isValid(): self.table_view.selectionModel().select(QItemSelection(self.last_selected_index, index), QItemSelectionModel.Select)
    def _on_cell_released(self, event): self.dragging = False; self._update_action_button_states()
    
    def _on_column_header_clicked(self, logical_index):
        modifiers = QApplication.keyboardModifiers()
        if modifiers == Qt.ShiftModifier:
            self._sort_by_column(logical_index)
        else:
            self.table_view.clearSelection()
            self.table_view.selectColumn(logical_index)
            first_visible_row = self.table_view.rowAt(0)
            if first_visible_row != -1: self._start_pulse(self.table_model.index(first_visible_row, logical_index))
            self._update_action_button_states()

    def _start_pulse(self, index: QModelIndex):
        if not index.isValid(): return
        self._end_pulse()
        self.pulsing_cells.add(index)
        self.table_model.dataChanged.emit(index, index, [Qt.BackgroundRole])
        self.pulse_timer.start(200)

    def _end_pulse(self):
        if not self.pulsing_cells: return
        indexes_to_update = list(self.pulsing_cells)
        self.pulsing_cells.clear()
        for index in indexes_to_update:
            self.table_model.dataChanged.emit(index, index, [Qt.BackgroundRole])

    def _show_context_menu(self, pos):
        menu = QMenu(self)
        
        header = self.table_view.horizontalHeader()
        header_pos = header.mapFromGlobal(self.table_view.viewport().mapToGlobal(pos))
        logical_index = header.logicalIndexAt(header_pos)
        
        is_on_header = header.rect().contains(header_pos) and logical_index != -1

        if is_on_header:
            col_name = self.table_model.headerData(logical_index, Qt.Horizontal)
            sort_asc = QAction(f"列「{col_name}」を昇順でソート", self)
            sort_asc.triggered.connect(lambda: self._sort_by_column(logical_index, Qt.AscendingOrder))
            menu.addAction(sort_asc)
            
            sort_desc = QAction(f"列「{col_name}」を降順でソート", self)
            sort_desc.triggered.connect(lambda: self._sort_by_column(logical_index, Qt.DescendingOrder))
            menu.addAction(sort_desc)
            
            menu.addSeparator()
            menu.addAction(self.copy_column_action)
            menu.addAction(self.paste_column_action)
            menu.addSeparator()
            menu.addAction(self.delete_selected_column_action)
        else:
            menu.addAction(self.cut_action)
            menu.addAction(self.copy_action)
            menu.addAction(self.paste_action)
            menu.addAction(self.delete_action)
            menu.addSeparator()

            sort_menu = menu.addMenu("📊 現在の列をソート")
            sort_menu.setEnabled(self.table_view.currentIndex().isValid() and not self.lazy_loader)
            sort_menu.addAction(self.sort_asc_action)
            sort_menu.addAction(self.sort_desc_action)
            menu.addSeparator()

            merge_menu = menu.addMenu("🔗 連結")
            merge_menu.addAction(self.cell_concatenate_action)
            merge_menu.addAction(self.column_concatenate_action)
            
            menu.addSeparator()
            if self.table_view.selectionModel().hasSelection():
                if bool(self.table_view.selectionModel().selectedRows()):
                     menu.addAction(self.delete_selected_rows_action)
        
        menu.exec(self.table_view.viewport().mapToGlobal(pos))
    
    def _request_cell_concatenate(self):
        selected = self.table_view.selectionModel().selectedIndexes()
        if len(selected) != 1: 
            self.show_operation_status("連結する基準セルを1つ選択してください。", is_error=True)
            return
        index = selected[0]
        
        direction, ok = QInputDialog.getItem(self, "連結方向の選択", "どちらのセルと連結しますか？", ["右", "左"], 0, False)
        if ok: self._concatenate_cells('cell', index.row(), index.column(), 'right' if direction == '右' else 'left')

    def _request_column_concatenate(self):
        active_index = self.table_view.currentIndex()
        if not active_index.isValid():
             self.show_operation_status("連結する基準となるセルを1つ選択してください。", is_error=True)
             return
        col = active_index.column()
        
        direction, ok = QInputDialog.getItem(self, "連結方向の選択", "どちらの列と連結しますか？", ["右", "左"], 0, False)
        if ok: self._concatenate_cells('column', -1, col, 'right' if direction == '右' else 'left')

    def _concatenate_cells(self, merge_type, row, col, direction):
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードでは連結できません。", is_error=True); return

        other_col = col + 1 if direction == 'right' else col - 1
        if not (0 <= other_col < self.table_model.columnCount()):
            self.show_operation_status(f"{direction}方向には連結できるセル/列がありません。", is_error=True); return
            
        dialog = MergeSeparatorDialog(self, is_column_merge=(merge_type == 'column'))
        if dialog.exec() != QDialog.Accepted: return
        
        separator = dialog.result
        changes = []
        rows_to_merge = range(self.table_model.rowCount()) if merge_type == 'column' else [row]
        
        col_name_target = self.table_model.headerData(col, Qt.Horizontal)
        col_name_other = self.table_model.headerData(other_col, Qt.Horizontal)
        
        actual_changes_count = 0

        for r in rows_to_merge:
            val1 = self.table_model.data(self.table_model.index(r, col), Qt.DisplayRole) or ""
            val2 = self.table_model.data(self.table_model.index(r, other_col), Qt.DisplayRole) or ""
            new_value = f"{val1}{separator}{val2}" if val1 and val2 else (val1 or val2)
            
            if str(val1) != new_value:
                changes.append({'item': str(r), 'column': col_name_target, 'old': val1, 'new': new_value})
                actual_changes_count += 1
            if str(val2) != "":
                 changes.append({'item': str(r), 'column': col_name_other, 'old': val2, 'new': ""})
                 actual_changes_count += 1

        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action)
            self.apply_action(action, is_undo=False) 
            self.show_operation_status(f"{actual_changes_count}個のセル値を連結しました。")
        else:
            self.show_operation_status("連結による変更はありませんでした。", 2000)

    def apply_action(self, action, is_undo):
        action_type, data = action['type'], action['data']
        
        if action_type in ['add_column', 'delete_column'] and self.db_backend:
            pass
            
        if action_type == 'edit':
            if self.db_backend:
                changes_for_db = [{'row_idx': int(c['item']), 'col_name': c['column'], 'new_value': c['old'] if is_undo else c['new']} for c in data]
                self.db_backend.update_cells(changes_for_db)
                self.table_model.layoutChanged.emit() 
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

        self.show_operation_status(f"操作を{'元に戻しました' if is_undo else '実行しました'}"); self._update_action_button_states()

    def _undo(self):
        if self.undo_manager.can_undo(): self.undo_manager.undo()
        else: self.show_operation_status("元に戻せる操作はありません", is_error=True)
    def _redo(self):
        if self.undo_manager.can_redo(): self.undo_manager.redo()
        else: self.show_operation_status("やり直せる操作はありません", is_error=True)

    def _copy(self):
        selected = self.table_view.selectionModel().selectedIndexes()
        if not selected: return
        min_r, max_r = min(i.row() for i in selected), max(i.row() for i in selected)
        
        rows_to_copy_indices = list(range(min_r, max_r + 1))
        df_selected = self.table_model.get_rows_as_dataframe(rows_to_copy_indices)
        
        selected_cols_indices = sorted(list(set(i.column() for i in selected)))
        selected_col_names = [self.header[idx] for idx in selected_cols_indices]
        df_selected = df_selected[selected_col_names]

        output = StringIO()
        df_selected.to_csv(output, sep='\t', index=False, header=False)
        QApplication.clipboard().setText(output.getvalue().strip())
        output.close()

        self.show_operation_status(f"{len(selected)}個のセルをコピーしました")

    def _cut(self):
        if self.is_readonly_mode(for_edit=True): self.show_operation_status("このモードでは切り取りはできません。", is_error=True); return
        self._copy(); self._delete_selected()

    def _paste(self):
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードでは貼り付けはできません。", is_error=True); return
        selection = self.table_view.selectionModel()
        clipboard_text = QApplication.clipboard().text()
        if not clipboard_text: return
        
        pasted_df_raw = None
        try:
            pasted_df_raw = pd.read_csv(StringIO(clipboard_text), sep='\t', header=None, dtype=str, on_bad_lines='skip').fillna('')
        except Exception as e:
            print(f"Initial clipboard parsing failed with tab delimiter: {e}")
            pass

        num_pasted_rows_raw = len(pasted_df_raw) if pasted_df_raw is not None else 0
        num_pasted_cols_raw = pasted_df_raw.shape[1] if pasted_df_raw is not None else 0

        num_model_rows = self.table_model.rowCount()
        num_model_cols = self.table_model.columnCount()
        
        selected_indexes = selection.selectedIndexes()
        
        start_row, start_col = 0, 0
        if selected_indexes:
            start_row = min(idx.row() for idx in selected_indexes)
            start_col = min(idx.column() for idx in selected_indexes)
        else:
            self.show_operation_status("貼り付け開始位置を選択してください。", is_error=True); return
        
        paste_dialog = PasteOptionDialog(self, num_pasted_cols_raw > 1)
        if paste_dialog.exec() != QDialog.Accepted:
            return

        paste_mode = paste_dialog.get_selected_mode()
        custom_delimiter = paste_dialog.get_custom_delimiter()

        pasted_df = None
        if paste_mode == 'normal':
            pasted_df = pasted_df_raw
        elif paste_mode == 'single_column':
            single_column_lines = clipboard_text.split('\n')
            pasted_df = pd.DataFrame([line.strip() for line in single_column_lines], columns=[0]).fillna('')
        elif paste_mode == 'custom_delimiter':
            try:
                pasted_df = pd.read_csv(StringIO(clipboard_text), sep=custom_delimiter, header=None, dtype=str, on_bad_lines='skip').fillna('')
            except Exception as e:
                self.show_operation_status(f"カスタム区切り文字での解析に失敗しました: {e}", is_error=True); return
        
        if pasted_df is None: return

        num_pasted_rows, num_pasted_cols = pasted_df.shape
        
        changes = []
        
        is_single_column_fully_selected = False
        if num_model_rows > 0 and len(selected_indexes) == num_model_rows:
            first_selected_col = selected_indexes[0].column()
            if all(idx.column() == first_selected_col for idx in selected_indexes):
                is_single_column_fully_selected = True
                start_col = first_selected_col

        if is_single_column_fully_selected and num_pasted_cols == 1:
            for r_off in range(num_pasted_rows):
                if start_row + r_off < num_model_rows:
                    target_row = start_row + r_off
                    new_value = pasted_df.iloc[r_off, 0]
                    idx = self.table_model.index(target_row, start_col)
                    old_value = self.table_model.data(idx, Qt.DisplayRole)
                    if str(old_value) != new_value:
                        changes.append({'item': str(target_row), 'column': self.table_model.headerData(start_col, Qt.Horizontal), 'old': str(old_value), 'new': new_value})
        else:
            for r_off in range(num_pasted_rows):
                for c_off in range(num_pasted_cols):
                    r, c = start_row + r_off, start_col + c_off

                    if r < num_model_rows and c < num_model_cols:
                        idx = self.table_model.index(r, c)
                        old_value = self.table_model.data(idx, Qt.DisplayRole)
                        new_value = pasted_df.iloc[r_off, c_off]
                        if str(old_value) != new_value:
                            changes.append({'item': str(r), 'column': self.table_model.headerData(c, Qt.Horizontal), 'old': str(old_value), 'new': new_value})
        
        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action); self.apply_action(action, is_undo=False)
            self.show_operation_status("貼り付けました")
        else:
            self.show_operation_status("貼り付けによる変更はありませんでした。", 2000)

    def _delete_selected(self):
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

    def _select_all(self): self.table_view.selectAll(); self._update_action_button_states()

    def _custom_key_press_event(self, event):
        current_index = self.table_view.currentIndex()
        
        if self.card_scroll_area.isVisible():
            if event.modifiers() & Qt.ControlModifier: 
                if event.key() == Qt.Key_Left:
                    self._move_card_record(-1)
                    event.accept()
                    return
                elif event.key() == Qt.Key_Right:
                    self._move_card_record(1)
                    event.accept()
                    return
            elif event.key() == Qt.Key_Tab or event.key() == Qt.Key_Backtab:
                if self._handle_card_view_tab_navigation(event):
                    return

        super(QTableView, self.table_view).keyPressEvent(event)
        
        new_index = self.table_view.currentIndex()
        if current_index != new_index: self._start_pulse(new_index)
        
        if event.key() in [Qt.Key_Return, Qt.Key_Enter, Qt.Key_F2] and new_index.isValid(): 
            self.show_context_hint('editing')
            self._start_cell_edit(new_index)

        self._update_action_button_states()

    def _start_cell_edit(self, index):
        if index.isValid() and not self.is_readonly_mode(for_edit=True): 
            self.show_context_hint('editing')
            self.table_view.edit(index)
        else: self.show_operation_status("このモードではセルを編集できません。", is_error=True)

    def update_menu_states(self):
        if not hasattr(self, 'undo_action'): return
        is_readonly_for_edit = self.is_readonly_mode(for_edit=True)
        self.undo_action.setEnabled(self.undo_manager.can_undo() and not is_readonly_for_edit)
        self.redo_action.setEnabled(self.undo_manager.can_redo() and not is_readonly_for_edit)

    @Slot(list)
    def handle_search_results_ready(self, results):
        self.progress_bar.hide()
        self._clear_search_highlight()

        last_settings = self._pending_replace_settings or self._pending_extract_settings or self._last_search_settings
        
        final_results = []
        if last_settings and last_settings.get("is_parent_child_mode"):
            parent_child_data = self.parent_child_manager.parent_child_data
            if not parent_child_data:
                self.show_operation_status("親子関係が分析されていません。", 3000, is_error=True)
                self._pending_replace_all = False
                self._pending_extract = False
                return

            target_type = last_settings.get("target_type")
            for row, col in results:
                row_data = parent_child_data.get(row)
                
                if not row_data:
                    print(f"Warning: Row {row} not found in parent_child_data, treating as orphaned child")
                    if target_type in ['all', 'child']:
                        final_results.append((row, col))
                    continue

                is_parent = row_data.get('is_parent', False)
                if (target_type == 'all' or
                    (target_type == 'parent' and is_parent) or
                    (target_type == 'child' and not is_parent)):
                    final_results.append((row, col))
        else:
            final_results = results

        final_results_qidx = [self.table_model.index(r, c) for r, c in final_results]

        if self._pending_replace_all:
            self._pending_replace_all = False
            if final_results_qidx:
                self._execute_replace_all_with_results(self._pending_replace_settings, final_results_qidx)
            else:
                self.show_operation_status("置換対象が見つかりませんでした。", 3000)
            self._pending_replace_settings = None
            return

        if self._pending_extract:
            self._pending_extract = False
            if final_results_qidx:
                self._execute_extract_with_results(final_results_qidx)
            else:
                self.show_operation_status("抽出対象が見つかりませんでした。", 3000)
            self._pending_extract_settings = None
            return

        if not final_results_qidx:
            self.show_operation_status("一致するセルが見つかりませんでした。", 3000)
            return
        
        self.search_results = final_results_qidx
        self.current_search_index = 0
        self._highlight_current_search_result()
        self.show_operation_status(f"{len(self.search_results)}件のセルが見つかりました。")

    def _highlight_current_search_result(self):
        self.table_model.set_search_highlight_indexes(self.search_results)
        if self.current_search_index != -1 and self.search_results:
            index = self.search_results[self.current_search_index]
            self.table_model.set_current_search_index(index)
            self.table_view.scrollTo(index, QAbstractItemView.EnsureVisible)
            self.table_view.setCurrentIndex(index)
            self.table_view.selectionModel().select(index, QItemSelectionModel.ClearAndSelect)

    def _clear_search_highlight(self):
        self.table_model.set_search_highlight_indexes([]); self.table_model.set_current_search_index(QModelIndex()); self.search_results = []; self.current_search_index = -1; self.table_view.viewport().update()

    def is_readonly_mode(self, for_edit=False):
        if for_edit: return self.lazy_loader is not None
        return self.lazy_loader is not None
    
    def _show_shortcuts_help(self): QMessageBox.information(self, "ショートカットキー", "...")

    @Slot()
    def copy_selected_column(self):
        selected_columns = self.table_view.selectionModel().selectedColumns()
        if not selected_columns:
            self.show_operation_status("コピーする列を選択してください。", is_error=True)
            return
        
        col_index = selected_columns[0].left()
        if self.is_readonly_mode(for_edit=True) and self.table_model.rowCount() > 500000:
             QMessageBox.warning(self, "警告", "巨大な列データをメモリにロードします。時間がかかる場合があります。")

        self.column_clipboard = self.table_model.get_column_data(col_index)
        col_name = self.table_model.headerData(col_index, Qt.Horizontal)
        self.show_operation_status(f"列「{col_name}」({len(self.column_clipboard):,}行)をコピーしました。")
        self._update_action_button_states()

    @Slot()
    def paste_to_selected_column(self):
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードでは貼り付けできません。", is_error=True)
            return

        if self.column_clipboard is None:
            self.show_operation_status("貼り付ける列データがありません。先に列をコピーしてください。", is_error=True)
            return

        selected_columns = self.table_view.selectionModel().selectedColumns()
        if not selected_columns:
            self.show_operation_status("貼り付け先の列を選択してください。", is_error=True)
            return

        dest_col_index = selected_columns[0].left()
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

    @Slot(int, Qt.SortOrder)
    def _sort_by_column(self, logical_index, order=None):
        if self.lazy_loader:
            self.show_operation_status("遅延読み込みモードではソートできません。", is_error=True)
            return
            
        if logical_index < 0:
            return

        if order is None:
            if self.sort_info['column_index'] == logical_index:
                order = Qt.DescendingOrder if self.sort_info['order'] == Qt.AscendingOrder else Qt.AscendingOrder
            else:
                order = Qt.AscendingOrder
        
        self.sort_info = {'column_index': logical_index, 'order': order}

        self.table_view.horizontalHeader().setSortIndicator(logical_index, order)
        self.table_model.sort(logical_index, order)
        col_name = self.table_model.headerData(logical_index, Qt.Horizontal)
        self.show_operation_status(f"列「{col_name}」でソートしました。")
        self._update_action_button_states()

    @Slot()
    def _clear_sort(self):
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
        self._recreate_card_view_fields()

    def delete_selected_rows(self): 
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
    
    def _delete_selected_column(self):
        selected_columns = self.table_view.selectionModel().selectedColumns()
        if not selected_columns: 
            self.show_operation_status("削除する列を選択してください。", is_error=True)
            return
        if self.is_readonly_mode(for_edit=True): 
            self.show_operation_status("このモードでは列を削除できません。", is_error=True); return
        
        col_idx = selected_columns[0].left()
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

    def _open_price_calculator_dialog(self):
        if self.table_model.rowCount() == 0:
            self.show_operation_status("操作対象のデータがありません。", 3000, is_error=True); return
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードでは金額計算ツールを実行できません。", is_error=True); return

        dialog = PriceCalculatorDialog(self, self.table_model._headers)
        if dialog.exec() == QDialog.Accepted:
            settings = dialog.result
            self._apply_price_calculation(settings)

    def _apply_price_calculation(self, settings):
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードでは計算を実行できません。", is_error=True); return
        target_col = settings['column']
        tax_status = settings['tax_status']
        discount = settings['discount']
        round_mode = settings['round_mode']

        tax_rate = 1.10
        discount_multiplier = 1.0 - (discount / 100.0)
        
        changes = []
        for i in range(self.table_model.rowCount()):
            index = self.table_model.index(i, self.table_model._headers.index(target_col))
            original_value_str = self.table_model.data(index, Qt.DisplayRole)
            if not original_value_str: continue
            try:
                cleaned_value = re.sub(r'[^\d.]', '', original_value_str) 
                price = float(cleaned_value)
            except (ValueError, TypeError):
                print(f"Warning: Row {i}, Column '{target_col}' value '{original_value_str}' cannot be converted to number. Skipping.")
                continue
            
            new_price_float = 0.0

            if tax_status == 'exclusive':
                price_with_tax = price * tax_rate
                discounted_price_with_tax = price_with_tax * discount_multiplier
                new_price_float = discounted_price_with_tax / tax_rate
            else:
                new_price_float = price * discount_multiplier
            
            if round_mode == 'truncate':
                new_price = math.trunc(new_price_float)
            elif round_mode == 'round':
                new_price = round(new_price_float)
            elif round_mode == 'ceil':
                new_price = math.ceil(new_price_float)
            else:
                new_price = math.trunc(new_price_float)

            new_value_str = str(int(new_price))

            if new_value_str != original_value_str:
                changes.append({'item': str(i), 'column': target_col, 'old': original_value_str, 'new': new_value_str})
        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action)
            self.apply_action(action, is_undo=False)
            self.show_operation_status(f"{len(changes)}件の金額を更新しました")
        else:
            self.show_operation_status("金額の更新はありませんでした", 2000)

    @Slot(dict)
    def _apply_replace_from_file(self, params: dict):
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードではファイル参照置換を実行できません。", 3000, is_error=True); return
        
        if self.lazy_loader:
            QMessageBox.warning(self, "機能制限", "遅延読み込みモードではファイル参照置換はサポートされていません。")
            self.show_operation_status("遅延読み込みモードではファイル参照置換できません。", is_error=True)
            return

        if self.table_model.rowCount() > 50000 or \
           (os.path.exists(params['lookup_filepath']) and os.path.getsize(params['lookup_filepath']) / (1024 * 1024) > 50):
            QMessageBox.warning(self, "パフォーマンス警告", "大規模データに対するファイル参照置換は時間がかかり、メモリを大量に消費する可能性があります。")

        self.show_operation_status("ファイル参照置換中...", duration=0)
        QApplication.setOverrideCursor(Qt.WaitCursor)
        
        self.async_manager.replace_from_file_async(self.db_backend, self.table_model.get_dataframe(), params) 

    @Slot(list, str)
    def handle_replace_from_file_completed(self, changes: list, status_message: str):
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

    def _call_async_search(self, settings):
        """非同期検索を呼び出すヘルパーメソッド"""
        parent_child_data = self.parent_child_manager.parent_child_data
        selected_rows = set()
        if settings.get("in_selection_only"):
            selected_rows = {idx.row() for idx in self.table_view.selectionModel().selectedIndexes()}
        
        self.async_manager.search_data_async(
            settings, 
            self.async_manager.current_load_mode,
            parent_child_data,
            selected_rows
        )

    def _find_next(self, settings):
        if not settings["search_term"]: return

        if not self.search_results or self._last_search_settings != settings:
            self._last_search_settings = settings.copy()
            self.show_operation_status("検索中です...", duration=0)
            self._call_async_search(settings)
            return
        
        if len(self.search_results) > 0:
            self.current_search_index = (self.current_search_index + 1) % len(self.search_results)
            self._highlight_current_search_result()
            self.show_operation_status(f"検索結果 {self.current_search_index + 1}/{len(self.search_results)}件")

    def _find_prev(self, settings):
        if not settings["search_term"]: return

        if not self.search_results or self._last_search_settings != settings:
            self._last_search_settings = settings.copy()
            self.show_operation_status("検索中です...", duration=0)
            self._call_async_search(settings)
            return

        if len(self.search_results) > 0:
            self.current_search_index = (self.current_search_index - 1 + len(self.search_results)) % len(self.search_results)
            self._highlight_current_search_result()
            self.show_operation_status(f"検索結果 {self.current_search_index + 1}/{len(self.search_results)}件")
    
    @Slot(str)
    def handle_parent_child_analysis_ready(self, summary_text):
        QApplication.restoreOverrideCursor()
        self.progress_bar.hide()
        if self.search_panel:
            self.search_panel.analysis_text.setText(summary_text)
        if "分析エラー" in summary_text:
            self.show_operation_status("親子関係の分析に失敗しました。", is_error=True)
        else:
            self.show_operation_status("親子関係を分析しました。")

    def _analyze_parent_child_from_widget(self):
        settings = self.search_panel.get_settings()
        column_name = settings.get("key_column")
        analysis_mode = settings.get("analysis_mode", "consecutive")
        if not column_name: return
        
        if self.lazy_loader:
             QMessageBox.warning(self, "機能制限", "遅延読み込みモードでは親子関係の分析はできません。")
             self.search_panel.analysis_text.setText("遅延読み込みモードでは親子関係の分析はできません。")
             return

        if self.db_backend:
            self.show_operation_status("親子関係分析中... (データベース)", duration=0)
            QApplication.setOverrideCursor(Qt.WaitCursor)
            
            total_rows = self.db_backend.get_total_rows()
            self.progress_bar.setRange(0, total_rows)
            self.progress_bar.setValue(0)
            self.progress_bar.show()

            self.async_manager.analyze_parent_child_async(self.db_backend, column_name, analysis_mode)
            
        else:
            df_to_analyze = self.table_model.get_dataframe()
            
            if df_to_analyze is None or df_to_analyze.empty:
                self.search_panel.analysis_text.setText("分析対象のデータがありません。"); return

            success, msg = self.parent_child_manager.analyze_relationships(df_to_analyze, column_name, analysis_mode)
            if success:
                self.search_panel.analysis_text.setText(self.parent_child_manager.get_groups_summary())
                self.show_operation_status("親子関係を分析しました。")
            else:
                self.search_panel.analysis_text.setText(f"分析エラー:\n{msg}")
                self.show_operation_status("親子関係の分析に失敗しました。", is_error=True)

    def _replace_current(self, settings):
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードでは置換できません。", 3000, is_error=True); return
        
        if self.current_search_index == -1 or not self.search_results:
            self.show_operation_status("置換する検索結果が選択されていません。検索を実行してください。", is_error=True); return
        
        index = self.search_results[self.current_search_index]
        old_value = self.table_model.data(index, Qt.DisplayRole)
        
        try:
            pattern = re.compile(settings["search_term"] if settings["is_regex"] else re.escape(settings["search_term"]),0 if settings["is_case_sensitive"] else re.IGNORECASE)
            new_value = pattern.sub(settings["replace_term"], str(old_value))
            
            if str(old_value) != new_value:
                actual_old_value = self.table_model.data(index, Qt.DisplayRole)
                if str(actual_old_value) != new_value:
                    action = {'type': 'edit', 'data': [{'item': str(index.row()), 'column': self.table_model.headerData(index.column(), Qt.Horizontal), 'old': str(actual_old_value), 'new': new_value}]}
                    self.undo_manager.add_action(action)
                    self.apply_action(action, is_undo=False)
                    self.show_operation_status("1件のセルを置換しました。")
                else:
                    self.show_operation_status("変更がありませんでした。", 2000)
            else:
                self.show_operation_status("変更がありませんでした。", 2000)

        except re.error as e:
            self.show_operation_status(f"正規表現エラー: {e}", 3000, is_error=True); return
        except Exception as e:
            self.show_operation_status(f"置換エラー: {e}", 3000, is_error=True); return
        
        self.search_results.pop(self.current_search_index)
        if not self.search_results: 
            self._clear_search_highlight()
            self.show_operation_status("全ての検索結果を置換しました。")
        elif self.current_search_index >= len(self.search_results): 
            self.current_search_index = 0
            self._highlight_current_search_result()
        else:
            self._highlight_current_search_result()

    def _replace_all(self, settings):
        if self.is_readonly_mode(for_edit=True):
            self.show_operation_status("このモードではすべて置換できません。", 3000, is_error=True)
            return
        
        self._last_search_settings = settings.copy()
        self._pending_replace_all = True
        self._pending_replace_settings = settings
        self.show_operation_status("置換対象を検索中です...", duration=0)
        self._call_async_search(settings)

    def _execute_replace_all_with_results(self, settings, found_indices):
        if not found_indices:
            self.show_operation_status("置換対象が見つかりませんでした。", 3000)
            return

        if self.db_backend:
            success, updated_count = self.db_backend.execute_replace_all_in_db(settings)
            if success:
                self.show_operation_status(f"{updated_count}件のセルを置換しました。")
                self.table_model.layoutChanged.emit()
            else:
                self.show_operation_status("データベースでの一括置換に失敗しました。", is_error=True)
            self._clear_search_highlight()
            return

        changes = []
        try:
            pattern = re.compile(
                settings["search_term"] if settings["is_regex"] else re.escape(settings["search_term"]),
                0 if settings["is_case_sensitive"] else re.IGNORECASE
            )
        except re.error as e:
            self.show_operation_status(f"正規表現エラー: {e}", is_error=True)
            return
        
        for index in found_indices:
            old_value = str(self.table_model.data(index, Qt.DisplayRole) or "")
            new_value = pattern.sub(settings["replace_term"], old_value)
            
            if old_value != new_value:
                changes.append({
                    'item': str(index.row()),
                    'column': self.table_model.headerData(index.column(), Qt.Horizontal),
                    'old': old_value,
                    'new': new_value
                })
        
        if changes:
            action = {'type': 'edit', 'data': changes}
            self.undo_manager.add_action(action)
            self.apply_action(action, is_undo=False)
            self.show_operation_status(f"{len(changes)}件のセルを置換しました。")
            self._clear_search_highlight()
        else:
            self.show_operation_status("置換による変更はありませんでした。", 3000)

    def _execute_extract(self, settings: dict):
        if not settings["search_term"]:
            self.show_operation_status("検索条件を入力してください。", is_error=True)
            return
        
        self._last_search_settings = settings.copy()
        self._pending_extract = True
        self._pending_extract_settings = settings
        self.show_operation_status("抽出対象を検索中です...", duration=0)
        self._call_async_search(settings)

    def _execute_extract_with_results(self, found_indices: list):
        if not found_indices:
            self.show_operation_status("抽出対象が見つかりませんでした。", 3000)
            return
            
        row_indices = sorted(list({idx.row() for idx in found_indices}))
        
        extracted_df = self.table_model.get_rows_as_dataframe(row_indices).reset_index(drop=True)
        
        self.create_extract_window_signal.emit(extracted_df)

    @Slot(pd.DataFrame)
    def _create_extract_window_in_ui_thread(self, extracted_df):
        if not hasattr(self, 'child_windows'):
            self.child_windows = []
        
        new_window = CsvEditorAppQt(
            dataframe=extracted_df,
            parent=self,
            filepath=self.filepath,
            encoding=self.encoding if hasattr(self, 'encoding') else 'shift_jis'  # ✅ encodingを追加
        )
        self.child_windows.append(new_window)
        new_window.show()
        
        row_count = len(extracted_df)
        self.show_operation_status(f"{row_count}行を新しいウィンドウに抽出しました。")

    def closeEvent(self, event):
        if self.parent() is None:
            if self.undo_manager.can_undo() and not self.is_readonly_mode(for_edit=False):
                reply = QMessageBox.question(self, "確認", "未保存の変更があります。変更を保存しますか？", QMessageBox.Save | QMessageBox.Discard | QMessageBox.Cancel, QMessageBox.Save)
                if reply == QMessageBox.Save:
                    self.save_file()
                    if self.undo_manager.can_undo():
                        event.ignore(); return
                elif reply == QMessageBox.Cancel: event.ignore(); return
            
            reply = QMessageBox.question(self, "終了確認", "アプリケーションを終了しますか？", QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            if reply == QMessageBox.Yes: 
                self.async_manager.shutdown()
                self._cleanup_backend()
                event.accept()
            else: event.ignore()
        else:
            if hasattr(self.parent(), 'child_windows') and self in self.parent().child_windows: 
                self.parent().child_windows.remove(self)
            self._cleanup_backend()
            event.accept()

    def _toggle_view(self):
        if self.table_model.rowCount() == 0:
            self.show_operation_status("表示するデータがありません。", 3000, is_error=True)
            return

        current_index = self.table_view.currentIndex()
        if not current_index.isValid() and self.table_model.rowCount() > 0:
            current_index = self.table_model.index(0,0)

        if self.table_view.isVisible():
            if not current_index.isValid():
                QMessageBox.information(self, "情報", "カードビューで表示する行を選択してください。")
                return

            self._show_card_view(current_index.row())
            self.table_view.hide()
            self.card_scroll_area.show()
            self.view_toggle_action.setText("テーブルビュー")
            self.view_toggle_action.setIcon(self.style().standardIcon(QStyle.SP_FileDialogListView))
            
        else:
            self.card_scroll_area.hide()
            self.table_view.show()
            self.view_toggle_action.setText("カードビュー")
            self.view_toggle_action.setIcon(self.style().standardIcon(QStyle.SP_FileDialogDetailedView))
            
            self.table_model.layoutChanged.emit()

    def _adjust_text_edit_height(self, text_edit_widget):
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
        layout = self.card_view_container.layout
        while layout.rowCount() > 1:
            layout.removeRow(0)
        self.card_fields_widgets.clear()
        self.card_mapper.clearMapping()

        for col_idx, col_name in enumerate(self.header):
            label = QLabel(f"{col_name}:")
            
            field_widget = QPlainTextEdit()
            field_widget.setLineWrapMode(QPlainTextEdit.WidgetWidth)
            field_widget.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)

            field_widget.document().contentsChanged.connect(lambda f=field_widget: self._adjust_text_edit_height(f))
            
            field_widget.setMinimumHeight(self.density['row_height'] * 1.5)
            field_widget.setMaximumHeight(self.density['row_height'] * 8)

            self.card_fields_widgets[col_name] = field_widget
            layout.insertRow(layout.rowCount() - 1, label, field_widget)
            
            self.card_mapper.addMapping(field_widget, col_idx) 
            field_widget.installEventFilter(self)

        self.card_mapper.setModel(self.table_model) 
        if self.card_scroll_area.isVisible():
            current_index = self.table_view.currentIndex()
            row_to_show = current_index.row() if current_index.isValid() else 0
            if self.table_model.rowCount() > 0:
                self._show_card_view(row_to_show)

    def _show_card_view(self, row_idx_in_model):
        if not self.table_model.rowCount():
            self.show_operation_status("表示するデータがありません。", 3000, is_error=True)
            return

        model_index = self.table_model.index(row_idx_in_model, 0)
        if not model_index.isValid():
            model_index = self.table_model.index(0, 0)
            if not model_index.isValid():
                self.show_operation_status("表示するデータがありません。", 3000, is_error=True)
                return

        self.card_mapper.setCurrentIndex(model_index.row())
        
        for field_widget in self.card_fields_widgets.values():
            self._adjust_text_edit_height(field_widget)

        self.card_mapper.setSubmitPolicy(QDataWidgetMapper.AutoSubmit)

    def _handle_card_view_tab_navigation(self, event: QEvent):
        if not self.card_scroll_area.isVisible():
            return False
        
        current_widget = QApplication.focusWidget()

        widgets_in_order = []
        layout = self.card_view_container.layout
        for i in range(layout.rowCount()):
            layout_item = layout.itemAt(i, QFormLayout.FieldRole)
            if layout_item and layout_item.widget():
                widget = layout_item.widget()
                if isinstance(widget, QPlainTextEdit):
                    widgets_in_order.append(widget)
        
        if current_widget not in widgets_in_order:
            return False
        
        is_shift_pressed = bool(QApplication.keyboardModifiers() & Qt.ShiftModifier)

        if event.key() == Qt.Key_Tab or event.key() == Qt.Key_Backtab:
            if not is_shift_pressed: # Tab
                current_widget_index = widgets_in_order.index(current_widget)
                if current_widget_index == len(widgets_in_order) - 1:
                    self._move_card_record(1)
                    if widgets_in_order: widgets_in_order[0].setFocus()
                    return True
            else: # Shift + Tab (Backtab)
                current_widget_index = widgets_in_order.index(current_widget)
                if current_widget_index == 0:
                     self._move_card_record(-1)
                     if widgets_in_order: widgets_in_order[-1].setFocus()
                     return True
        
        return False

    def _move_card_record(self, direction: int):
        current_row = self.card_mapper.currentIndex()
        new_row = current_row + direction
        
        if 0 <= new_row < self.table_model.rowCount():
            self.card_mapper.setCurrentIndex(new_row)
            for field_widget in self.card_fields_widgets.values():
                self._adjust_text_edit_height(field_widget)

            self.table_view.setCurrentIndex(self.table_model.index(new_row, 0))
            self.table_view.scrollTo(self.table_model.index(new_row, 0), QAbstractItemView.PositionAtCenter)

        else:
            self.show_operation_status("これ以上レコードはありません。", 2000)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    editor = CsvEditorAppQt()
    editor.show()
    sys.exit(app.exec())