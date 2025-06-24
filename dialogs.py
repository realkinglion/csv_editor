# dialogs.py

import os
import pandas as pd
import csv
import re

from PySide6.QtWidgets import (
    QApplication,
    QDialog, QVBoxLayout, QGridLayout, QGroupBox, QRadioButton, QComboBox,
    QLineEdit, QCheckBox, QPushButton, QLabel, QListWidget, QHBoxLayout,
    QMessageBox, QWidget, QDialogButtonBox, QDoubleSpinBox, QButtonGroup,
    QFileDialog, QToolTip
)
from PySide6.QtCore import Signal, Qt, QTimer, QEvent, QObject
from PySide6.QtGui import QPalette

class SearchReplaceDialog(QDialog):
    analysis_requested = Signal(dict)
    find_next_clicked = Signal(dict)
    find_prev_clicked = Signal(dict)
    replace_one_clicked = Signal(dict)
    replace_all_clicked = Signal(dict)
    extract_clicked = Signal(dict)

    def __init__(self, parent, headers, theme, mode='search_replace'):
        super().__init__(parent)
        
        self.headers = headers
        self.theme = theme
        self.mode = mode 
        self.result = None

        if self.mode == 'extract':
            self.setWindowTitle("検索して抽出")
        else:
            self.setWindowTitle("検索と置換")
        
        self.setMinimumWidth(550)
        self.setLayout(QVBoxLayout())

        self._create_widgets()
        self._connect_signals()
        self._update_ui_state(self.parent_child_group.isChecked())

    def _create_widgets(self):
        main_layout=QVBoxLayout(self)

        search_group = QGroupBox("検索と置換")
        search_layout = QGridLayout(search_group)
        self.search_entry = QLineEdit()
        self.replace_entry = QLineEdit()
        search_layout.addWidget(QLabel("検索:"), 0, 0)
        search_layout.addWidget(self.search_entry, 0, 1, 1, 2)
        self.replace_label = QLabel("置換:")
        search_layout.addWidget(self.replace_label, 1, 0)
        search_layout.addWidget(self.replace_entry, 1, 1, 1, 2)
        if self.mode == 'extract':
            self.replace_label.hide()
            self.replace_entry.hide()
        main_layout.addWidget(search_group)

        self.parent_child_group = QGroupBox("親子関係モード")
        self.parent_child_group.setCheckable(True)
        self.parent_child_group.setChecked(False)
        pc_layout = QVBoxLayout(self.parent_child_group)
        
        pc_config_layout = QHBoxLayout()
        self.column_combo = QComboBox()
        self.column_combo.addItems(self.headers)
        self.analyze_button = QPushButton("分析実行")
        pc_config_layout.addWidget(QLabel("基準列:"))
        pc_config_layout.addWidget(self.column_combo)
        pc_config_layout.addWidget(self.analyze_button)
        pc_layout.addLayout(pc_config_layout)
        
        self.analysis_text = QLabel("分析されていません。")
        self.analysis_text.setWordWrap(True)
        pc_layout.addWidget(self.analysis_text)
        
        target_frame = QGroupBox("対象")
        target_layout = QHBoxLayout(target_frame)
        self.target_all_radio = QRadioButton("全て")
        self.target_parent_radio = QRadioButton("親のみ")
        self.target_child_radio = QRadioButton("子のみ")
        self.target_all_radio.setChecked(True)
        target_layout.addWidget(self.target_all_radio)
        target_layout.addWidget(self.target_parent_radio)
        target_layout.addWidget(self.target_child_radio)
        pc_layout.addWidget(target_frame)
        main_layout.addWidget(self.parent_child_group)

        col_frame = QGroupBox("対象列 (Ctrl/Shiftで複数選択可)")
        col_layout = QVBoxLayout(col_frame)
        self.col_listbox = QListWidget()
        self.col_listbox.setSelectionMode(QListWidget.ExtendedSelection)
        self.col_listbox.addItems(self.headers)
        self.col_listbox.selectAll()
        col_layout.addWidget(self.col_listbox)
        main_layout.addWidget(col_frame)
        
        option_frame_top = QHBoxLayout()
        self.regex_check = QCheckBox("正規表現")
        self.case_check = QCheckBox("大文字/小文字を区別")
        self.selection_check = QCheckBox("選択範囲のみ")
        option_frame_top.addWidget(self.regex_check)
        option_frame_top.addWidget(self.case_check)
        option_frame_top.addWidget(self.selection_check)
        main_layout.addLayout(option_frame_top)

        option_frame_bottom = QHBoxLayout()
        self.ignore_blanks_check = QCheckBox("空白セルは無視する")
        self.ignore_blanks_check.setChecked(True)
        option_frame_bottom.addWidget(self.ignore_blanks_check)
        main_layout.addLayout(option_frame_bottom)

        button_frame = QGridLayout()
        self.find_prev_button = QPushButton("前を検索")
        self.find_next_button = QPushButton("次を検索")
        self.replace_button = QPushButton("置換")
        self.replace_all_button = QPushButton("すべて置換")
        self.extract_btn = QPushButton("抽出")
        
        if self.mode == 'extract':
            button_frame.addWidget(self.extract_btn, 0, 0, 1, 3)
            self.replace_label.hide()
            self.replace_entry.hide()
            self.replace_button.hide()
            self.replace_all_button.hide()
            self.find_prev_button.hide()
            self.find_next_button.hide()
        else:
            button_frame.addWidget(self.find_prev_button, 0, 0)
            button_frame.addWidget(self.find_next_button, 0, 1)
            button_frame.addWidget(self.replace_button, 1, 0)
            button_frame.addWidget(self.replace_all_button, 1, 1)
            self.extract_btn.hide()

        main_layout.addLayout(button_frame)
        
        self.result_label = QLabel("")
        main_layout.addWidget(self.result_label)
        
        main_layout.addStretch()

    def _connect_signals(self):
        self.parent_child_group.toggled.connect(self._update_ui_state)
        
        self.analyze_button.clicked.connect(self._analyze_parent_child) 
        
        if self.mode == 'extract':
            self.extract_btn.clicked.connect(self.extract)
        else:
            self.find_prev_button.clicked.connect(self.find_all)
            self.find_next_button.clicked.connect(self.find_all)
            self.replace_button.clicked.connect(lambda: self.find_all(replace_one=True))
            self.replace_all_button.clicked.connect(self.replace_all)

    def _update_ui_state(self, is_checked):
        self.column_combo.setEnabled(is_checked)
        self.analyze_button.setEnabled(is_checked)
        self.target_all_radio.setEnabled(is_checked)
        self.target_parent_radio.setEnabled(is_checked)
        self.target_child_radio.setEnabled(is_checked)

    def update_headers(self, new_headers: list):
        self.headers = new_headers
        self.column_combo.clear()
        self.column_combo.addItems(self.headers)
        self.col_listbox.clear()
        self.col_listbox.addItems(self.headers)
        self.col_listbox.selectAll()

    def get_settings(self):
        target_type = "all"
        if self.target_parent_radio.isChecked(): target_type = "parent"
        elif self.target_child_radio.isChecked(): target_type = "child"

        selected_columns = [item.text() for item in self.col_listbox.selectedItems()]

        return {
            "search_term": self.search_entry.text(),
            "replace_term": self.replace_entry.text(),
            "target_columns": selected_columns,
            "is_regex": self.regex_check.isChecked(),
            "is_case_sensitive": self.case_check.isChecked(),
            "in_selection_only": self.selection_check.isChecked(),
            "ignore_blanks": self.ignore_blanks_check.isChecked(),
            "is_parent_child_mode": self.parent_child_group.isChecked(),
            "key_column": self.column_combo.currentText(),
            "target_type": target_type
        }
    
    def _analyze_parent_child(self):
        self.parent().search_panel.analysis_requested.emit(self.get_settings())

    def _compile_regex(self):
        try:
            flags=0 if self.case_check.isChecked() else re.IGNORECASE
            pattern=self.search_entry.text()
            if not self.regex_check.isChecked(): pattern=re.escape(pattern)
            return re.compile(pattern,flags)
        except re.error as e: 
            QMessageBox.critical(self, "正規表現エラー", f"無効な正規表現です:\n{e}", parent=self); return None

    def _get_search_scope(self):
        pass

    def find_all(self, replace_one=False):
        self.result = {'action': 'find_all', 'settings': self.get_settings(), 'replace_one': replace_one}
        self.accept()

    def replace_all(self):
        self.result = {'action': 'replace_all', 'settings': self.get_settings()}
        self.accept()

    def extract(self):
        self.result = {'action': 'extract', 'settings': self.get_settings()}
        self.accept()

class MergeSeparatorDialog(QDialog):
    def __init__(self, parent, is_column_merge=False):
        super().__init__(parent)
        self.parent = parent
        self.is_column_concatenate = is_column_merge
        self.result = None
        
        merge_type = "列" if is_column_merge else "セル"
        self.setWindowTitle(f"{merge_type}連結設定")
        self.setMinimumWidth(400)
        
        self.setLayout(QVBoxLayout())
        self._create_widgets()
        self._connect_signals()
        self.custom_entry.setText(" ")
        self._update_preview()
        
    def _create_widgets(self):
        desc_text = "隣接する列の全行を連結します。" if self.is_column_concatenate else "選択したセルを隣のセルと連結します。"
        self.layout().addWidget(QLabel(desc_text))
        
        separator_group = QGroupBox("連結時の区切り文字")
        group_layout = QVBoxLayout(separator_group)
        self.layout().addWidget(separator_group)
        
        self.radio_space = QRadioButton("スペース")
        self.radio_none = QRadioButton("なし（直接連結）")
        self.radio_comma = QRadioButton("カンマ + スペース")
        self.radio_hyphen = QRadioButton("ハイフン")
        self.radio_custom = QRadioButton("カスタム:")
        self.radio_space.setChecked(True)
        
        self.custom_entry = QLineEdit()
        self.custom_entry.setEnabled(False)
        
        custom_layout = QHBoxLayout()
        custom_layout.addWidget(self.radio_custom)
        custom_layout.addWidget(self.custom_entry)
        
        group_layout.addWidget(self.radio_space)
        group_layout.addWidget(self.radio_none)
        group_layout.addWidget(self.radio_comma)
        group_layout.addWidget(self.radio_hyphen)
        group_layout.addLayout(custom_layout)
        
        preview_group = QGroupBox("プレビュー")
        preview_layout = QVBoxLayout(preview_group)
        self.preview_label = QLabel("値1" + " " + "値2")
        preview_layout.addWidget(self.preview_label)
        self.layout().addWidget(preview_group)
        
        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.layout().addWidget(self.button_box)
        
    def _connect_signals(self):
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        self.radio_space.toggled.connect(self._update_preview)
        self.radio_none.toggled.connect(self._update_preview)
        self.radio_comma.toggled.connect(self._update_preview)
        self.radio_hyphen.toggled.connect(self._update_preview)
        self.radio_custom.toggled.connect(self._update_preview)
        self.custom_entry.textChanged.connect(self._update_preview)

    def _update_preview(self):
        self.custom_entry.setEnabled(self.radio_custom.isChecked())
        separator = self.get_separator()
        self.preview_label.setText(f"値1{separator}値2")

    def get_separator(self):
        if self.radio_space.isChecked(): return " "
        if self.radio_none.isChecked(): return ""
        if self.radio_comma.isChecked(): return ", "
        if self.radio_hyphen.isChecked(): return " - "
        if self.radio_custom.isChecked(): return self.custom_entry.text()
        return ""

    def accept(self):
        self.result = self.get_separator()
        super().accept()

class PriceCalculatorDialog(QDialog):
    def __init__(self, parent, headers):
        super().__init__(parent)
        self.parent = parent
        self.headers = headers
        self.result = None

        self.setWindowTitle("金額計算ツール")
        self.setMinimumWidth(400)
        
        main_layout = QVBoxLayout(self)
        
        col_group = QGroupBox("1. 計算対象の列")
        col_layout = QHBoxLayout(col_group)
        self.column_combo = QComboBox()
        self.column_combo.addItems(self.headers)
        col_layout.addWidget(QLabel("列を選択:"))
        col_layout.addWidget(self.column_combo)
        main_layout.addWidget(col_group)

        tax_group = QGroupBox("2. 元の金額の消費税の状態")
        tax_layout = QVBoxLayout(tax_group)
        self.tax_exclusive_radio = QRadioButton("消費税 別 （税抜価格）")
        self.tax_inclusive_radio = QRadioButton("消費税 込み （税込価格）")
        self.tax_exclusive_radio.setChecked(True)
        tax_layout.addWidget(self.tax_exclusive_radio)
        tax_layout.addWidget(self.tax_inclusive_radio)
        main_layout.addWidget(tax_group)

        discount_group = QGroupBox("3. 割引率")
        discount_layout = QHBoxLayout(discount_group)
        self.discount_spinbox = QDoubleSpinBox()
        self.discount_spinbox.setRange(0.0, 100.0)
        self.discount_spinbox.setValue(0.0)
        self.discount_spinbox.setSuffix(" %")
        discount_layout.addWidget(QLabel("割引率:"))
        discount_layout.addWidget(self.discount_spinbox)
        main_layout.addWidget(discount_group)
        
        round_group = QGroupBox("4. 計算結果の丸め方")
        round_layout = QVBoxLayout(round_group)
        self.round_truncate_radio = QRadioButton("切り捨て (例: 10.9 → 10)")
        self.round_round_radio = QRadioButton("四捨五入 (例: 10.5 → 11)")
        self.round_ceil_radio = QRadioButton("切り上げ (例: 10.1 → 11)")
        self.round_truncate_radio.setChecked(True)
        round_layout.addWidget(self.round_truncate_radio)
        round_layout.addWidget(self.round_round_radio)
        round_layout.addWidget(self.round_ceil_radio)
        self.round_button_group = QButtonGroup(self)
        self.round_button_group.addButton(self.round_truncate_radio)
        self.round_button_group.addButton(self.round_round_radio)
        self.round_button_group.addButton(self.round_ceil_radio)
        main_layout.addWidget(round_group)

        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        main_layout.addWidget(self.button_box)

    def get_settings(self):
        round_mode = 'truncate'
        if self.round_round_radio.isChecked():
            round_mode = 'round'
        elif self.round_ceil_radio.isChecked():
            round_mode = 'ceil'

        return {
            'column': self.column_combo.currentText(),
            'tax_status': 'inclusive' if self.tax_inclusive_radio.isChecked() else 'exclusive',
            'discount': self.discount_spinbox.value(),
            'round_mode': round_mode
        }

    def accept(self):
        if not self.column_combo.currentText():
            QMessageBox.warning(self, "入力エラー", "対象の列を選択してください。")
            return
        self.result = self.get_settings()
        super().accept()

class PasteOptionDialog(QDialog):
    def __init__(self, parent, is_clipboard_multi_column=False):
        super().__init__(parent)
        self.setWindowTitle("貼り付けオプション")
        self.setMinimumWidth(350)
        self.result = {}

        main_layout = QVBoxLayout(self)

        group_box = QGroupBox("貼り付け方法を選択してください")
        group_layout = QVBoxLayout(group_box)

        self.radio_normal = QRadioButton("通常貼り付け (クリップボードの内容を展開)")
        self.radio_single_column = QRadioButton("単一列にすべて貼り付け (改行区切り)")
        self.radio_custom_delimiter = QRadioButton("カスタム区切り文字で貼り付け:")
        
        self.custom_delimiter_entry = QLineEdit()
        self.custom_delimiter_entry.setEnabled(False)
        
        custom_delimiter_layout = QHBoxLayout()
        custom_delimiter_layout.addWidget(self.radio_custom_delimiter)
        custom_delimiter_layout.addWidget(self.custom_delimiter_entry)

        group_layout.addWidget(self.radio_normal)
        group_layout.addWidget(self.radio_single_column)
        group_layout.addLayout(custom_delimiter_layout)

        self.radio_normal.setChecked(True)
        self.radio_single_column.setEnabled(is_clipboard_multi_column)
        self.radio_custom_delimiter.setEnabled(is_clipboard_multi_column)

        main_layout.addWidget(group_box)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        main_layout.addWidget(button_box)

        self.radio_custom_delimiter.toggled.connect(self.custom_delimiter_entry.setEnabled)

    def get_selected_mode(self):
        if self.radio_normal.isChecked():
            return 'normal'
        elif self.radio_single_column.isChecked():
            return 'single_column'
        elif self.radio_custom_delimiter.isChecked():
            return 'custom_delimiter'
        return 'normal'

    def get_custom_delimiter(self):
        return self.custom_delimiter_entry.text()

    def accept(self):
        if self.radio_custom_delimiter.isChecked() and not self.custom_delimiter_entry.text():
            QMessageBox.warning(self, "入力エラー", "カスタム区切り文字を入力してください。")
            return
        super().accept()

class CSVSaveFormatDialog(QDialog):
    """CSV保存時のフォーマット(クォートスタイル)を選択するダイアログ"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("CSV保存フォーマットの選択")
        self.setMinimumWidth(350)
        
        self.result = None
        self.layout = QVBoxLayout(self)
        
        # オプションと対応するcsvモジュールの定数を定義
        self.format_options = {
            "必要最小限のクォート（推奨）": csv.QUOTE_MINIMAL,
            "全てのフィールドをクォートで囲む": csv.QUOTE_ALL,
            "クォートを使用しない（非推奨）": csv.QUOTE_NONE,
            "非数値フィールドのみクォートで囲む": csv.QUOTE_NONNUMERIC
        }
        
        self.button_group = QButtonGroup(self)
        
        group_box = QGroupBox("クォート（\"）の使用方法")
        group_layout = QVBoxLayout()
        
        for text, value in self.format_options.items():
            radio_button = QRadioButton(text)
            self.button_group.addButton(radio_button, value)
            group_layout.addWidget(radio_button)
            if value == csv.QUOTE_MINIMAL:
                radio_button.setChecked(True) # デフォルト選択

        group_box.setLayout(group_layout)
        self.layout.addWidget(group_box)
        
        button_box = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        self.layout.addWidget(button_box)

    def selected_format(self):
        """選択されたクォートスタイルを返す"""
        return self.button_group.checkedId()

    def accept(self):
        """保存ボタンが押されたときの処理"""
        self.result = self.selected_format()
        super().accept()

    def reject(self):
        """キャンセルボタンが押されたときの処理"""
        self.result = None
        super().reject()

class SmartTooltip(QWidget):
    """
    指定時間ホバーされた際に、詳細なツールチップを表示するヘルパークラス。
    シングルトンパターンで実装し、同時に複数のツールチップが表示されないようにする。
    """
    _instance = None 

    def __init__(self, parent=None):
        super().__init__(parent)
        # ツールチップとしてのウィンドウフラグを設定
        self.setWindowFlags(Qt.ToolTip | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint)
        self.setAttribute(Qt.WA_TranslucentBackground)
        
        layout = QVBoxLayout(self)
        self.label = QLabel(self)
        self.label.setWordWrap(True)
        layout.addWidget(self.label)
        
        # スタイルシートで外観を定義（テーマに合わせて調整可能）
        self.setStyleSheet("""
            SmartTooltip {
                background-color: #282828;
                color: white;
                border: 1px solid #505050;
                border-radius: 4px;
                padding: 6px;
                font-size: 9pt;
            }
        """)
        
    @staticmethod
    def _get_instance(parent=None):
        """シングルトンインスタンスを取得または作成する"""
        if SmartTooltip._instance is None:
            # アプリケーションのトップレベルウィンドウを親にするのが一般的
            app_window = QApplication.activeWindow()
            SmartTooltip._instance = SmartTooltip(app_window)
        return SmartTooltip._instance

    @staticmethod
    def show_tooltip(pos, text, parent=None):
        """指定された位置にツールチップを表示する"""
        tooltip = SmartTooltip._get_instance(parent)
        tooltip.label.setText(text)
        tooltip.adjustSize()
        tooltip.move(pos)
        tooltip.show()

    @staticmethod
    def hide_tooltip():
        """ツールチップを非表示にする"""
        if SmartTooltip._instance is not None:
            SmartTooltip._instance.hide()

class TooltipEventFilter(QObject):
    """ウィジェットのイベントを監視し、SmartTooltipの表示を制御する"""
    def __init__(self, parent, text_callback, delay=700):
        super().__init__(parent)
        self._parent = parent
        self.text_callback = text_callback
        self.timer = QTimer(self)
        self.timer.setSingleShot(True)
        self.timer.setInterval(delay)
        self.timer.timeout.connect(self._show_tooltip)

    def eventFilter(self, watched, event):
        if watched == self._parent:
            if event.type() == QEvent.Type.Enter:
                self.timer.start()
            elif event.type() == QEvent.Type.Leave:
                self.timer.stop()
                SmartTooltip.hide_tooltip()

        return super().eventFilter(watched, event)

    def _show_tooltip(self):
        """タイマー完了後にツールチップを表示する"""
        if not self._parent.isVisible():
            return
        
        text = self.text_callback()
        if text:
            tooltip_pos = self._parent.mapToGlobal(self._parent.rect().bottomLeft())
            tooltip_pos.setY(tooltip_pos.y() + 2)
            SmartTooltip.show_tooltip(tooltip_pos, text, self._parent.window())

class EncodingSaveDialog(QDialog):
    """保存時のエンコーディングを選択するダイアログ"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("エンコーディングの選択")
        self.setMinimumWidth(350)
        self.result_encoding = None

        self.layout = QVBoxLayout(self)
        group_box = QGroupBox("保存する文字エンコーディング")
        group_layout = QVBoxLayout(group_box)

        # エンコーディングの選択肢
        self.encoding_options = {
            "Shift_JIS (Windows標準)": "shift_jis",
            "CP932 (Windows拡張Shift_JIS)": "cp932",  # ✅ 追加
            "UTF-8": "utf-8",
            "UTF-8 (BOM付き)": "utf-8-sig",
        }
        
        self.combo_box = QComboBox()
        self.combo_box.addItems(self.encoding_options.keys())
        
        # ✅ 親ウィンドウの現在のエンコーディングを選択
        if parent and hasattr(parent, 'encoding') and parent.encoding:
            for i, (text, enc) in enumerate(self.encoding_options.items()):
                if enc == parent.encoding:
                    self.combo_box.setCurrentIndex(i)
                    break
        else:
            # デフォルトをShift_JISに設定
            self.combo_box.setCurrentIndex(0)

        group_layout.addWidget(QLabel("ファイル保存に使用するエンコーディングを選択してください:"))
        group_layout.addWidget(self.combo_box)
        self.layout.addWidget(group_box)
        
        button_box = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        button_box.button(QDialogButtonBox.Save).setText("保存")
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        self.layout.addWidget(button_box)

    def selected_encoding(self):
        """選択されたエンコーディングの値を返す"""
        selected_text = self.combo_box.currentText()
        return self.encoding_options.get(selected_text, "shift_jis")

    def accept(self):
        """保存ボタンが押されたときの処理"""
        self.result_encoding = self.selected_encoding()
        super().accept()

    def reject(self):
        """キャンセルボタンが押されたときの処理"""
        self.result_encoding = None
        super().reject()