import os
import csv
import pandas as pd
import re
import math
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP

from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton,
    QComboBox, QSpinBox, QDoubleSpinBox, QCheckBox, QRadioButton, QGroupBox,
    QDialogButtonBox, QWidget, QGridLayout, QTextEdit, QApplication, QFileDialog,
    QListWidget, QInputDialog # QListWidget, QInputDialogを追加
)
from PySide6.QtCore import Qt, QObject, Signal, QTimer, QEvent


class TooltipEventFilter(QObject):
    """
    ターゲットウィジェットのツールチップ表示直前に内容を更新するイベントフィルター。
    これにより、ツールチップのテキストを動的に変更できる。
    """
    def __init__(self, target_widget, text_callback):
        super().__init__(target_widget)
        self.target_widget = target_widget
        self.text_callback = text_callback
        self.original_tooltip = target_widget.toolTip()
        self.target_widget.setToolTip(self.text_callback())

    def eventFilter(self, obj, event):
        if obj == self.target_widget and event.type() == QEvent.ToolTip:
            new_tooltip_text = self.text_callback()
            if self.target_widget.toolTip() != new_tooltip_text:
                self.target_widget.setToolTip(new_tooltip_text)
        return super().eventFilter(obj, event)


class EncodingSaveDialog(QDialog):
    """保存時のエンコーディングを選択するダイアログ"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("エンコーディングの選択")
        self.result_encoding = 'shift_jis'

        layout = QVBoxLayout(self)

        self.label = QLabel("保存するファイルのエンコーディングを選択してください:")
        layout.addWidget(self.label)

        self.encoding_combo = QComboBox()
        self.encoding_combo.addItems([
            'shift_jis', 'cp932', 'utf-8', 'utf-8-sig', 'euc-jp'
        ])
        self.encoding_combo.setCurrentText(self.result_encoding)
        layout.addWidget(self.encoding_combo)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

        self.encoding_combo.currentTextChanged.connect(self._update_result)

    def _update_result(self, text):
        self.result_encoding = text

class CSVSaveFormatDialog(QDialog):
    """CSV保存時の形式を設定するダイアログ（楽天市場CSV対応）"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("CSV保存形式の設定")
        self.result = {
            'quoting': csv.QUOTE_NONNUMERIC,
            'line_terminator': '\r\n',
            'preserve_html': True,
            'preserve_linebreaks': False
        }
        self.setupUi()
        self._load_defaults()

    def setupUi(self):
        main_layout = QVBoxLayout(self)

        quoting_group = QGroupBox("クォート設定")
        quoting_layout = QVBoxLayout(quoting_group)
        self.quote_all_radio = QRadioButton("全てのフィールドをクォート (楽天市場推奨)")
        self.quote_nonnumeric_radio = QRadioButton("非数値フィールドをクォート")
        self.quote_minimal_radio = QRadioButton("必要なフィールドのみクォート")
        self.quote_none_radio = QRadioButton("クォートしない")
        quoting_layout.addWidget(self.quote_all_radio)
        quoting_layout.addWidget(self.quote_nonnumeric_radio)
        quoting_layout.addWidget(self.quote_minimal_radio)
        quoting_layout.addWidget(self.quote_none_radio)
        main_layout.addWidget(quoting_group)

        line_ending_group = QGroupBox("行末の改行コード")
        line_ending_layout = QVBoxLayout(line_ending_group)
        self.crlf_radio = QRadioButton("CRLF (Windows/標準) \\r\\n")
        self.lf_radio = QRadioButton("LF (Unix/macOS) \\n")
        line_ending_layout.addWidget(self.crlf_radio)
        line_ending_layout.addWidget(self.lf_radio)
        main_layout.addWidget(line_ending_group)

        rakuten_group = QGroupBox("楽天市場CSV互換オプション")
        rakuten_layout = QVBoxLayout(rakuten_group)
        self.preserve_html_checkbox = QCheckBox("セル内のHTMLタグを保持する (エスケープしない)")
        self.preserve_linebreaks_checkbox = QCheckBox("セル内の改行をそのまま保持する (自動で<br>に変換しない)")
        rakuten_layout.addWidget(self.preserve_html_checkbox)
        rakuten_layout.addWidget(self.preserve_linebreaks_checkbox)
        main_layout.addWidget(rakuten_group)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self._on_accept)
        button_box.rejected.connect(self.reject)
        main_layout.addWidget(button_box)

        self._connect_signals()

    def _connect_signals(self):
        self.quote_all_radio.toggled.connect(self._update_result)
        self.quote_nonnumeric_radio.toggled.connect(self._update_result)
        self.quote_minimal_radio.toggled.connect(self._update_result)
        self.quote_none_radio.toggled.connect(self._update_result)
        self.crlf_radio.toggled.connect(self._update_result)
        self.lf_radio.toggled.connect(self._update_result)
        self.preserve_html_checkbox.toggled.connect(self._update_result)
        self.preserve_linebreaks_checkbox.toggled.connect(self._update_result)

    def _load_defaults(self):
        self.quote_all_radio.setChecked(True)
        self.crlf_radio.setChecked(True)
        self.preserve_html_checkbox.setChecked(True)
        self.preserve_linebreaks_checkbox.setChecked(True)

    def _on_accept(self):
        self._update_result()
        self.accept()

    def _update_result(self):
        if self.quote_all_radio.isChecked():
            self.result['quoting'] = csv.QUOTE_ALL
        elif self.quote_nonnumeric_radio.isChecked():
            self.result['quoting'] = csv.QUOTE_NONNUMERIC
        elif self.quote_minimal_radio.isChecked():
            self.result['quoting'] = csv.QUOTE_MINIMAL
        elif self.quote_none_radio.isChecked():
            self.result['quoting'] = csv.QUOTE_NONE

        if self.crlf_radio.isChecked():
            self.result['line_terminator'] = '\r\n'
        elif self.lf_radio.isChecked():
            self.result['line_terminator'] = '\n'

        self.result['preserve_html'] = self.preserve_html_checkbox.isChecked()
        self.result['preserve_linebreaks'] = self.preserve_linebreaks_checkbox.isChecked()


class MergeSeparatorDialog(QDialog):
    """セルの結合、列の連結の区切り文字を設定するダイアログ"""
    def __init__(self, parent=None, is_column_merge=False):
        super().__init__(parent)
        self.is_column_merge = is_column_merge
        self.setWindowTitle("連結オプション")
        self.selected_separator = " "

        layout = QVBoxLayout(self)

        label_text = "連結する値の間に挿入する区切り文字を選択または入力してください:"
        if is_column_merge:
            label_text = "連結する列の値を統合する際に挿入する区切り文字を選択または入力してください:"
        layout.addWidget(QLabel(label_text))

        self.separator_combo = QComboBox()
        self.separator_combo.setEditable(True)
        self.separator_combo.addItems([" (スペース)", ", (カンマ)", "; (セミコロン)", ": (コロン)", "| (パイプ)", "- (ハイフン)", "_ (アンダースコア)", "なし (区切り文字なし)"])
        self.separator_combo.setCurrentIndex(0)
        layout.addWidget(self.separator_combo)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self._on_accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

        self.separator_combo.currentTextChanged.connect(self._update_separator)

    def _update_separator(self, text):
        if text == " (スペース)":
            self.selected_separator = " "
        elif text == ", (カンマ)":
            self.selected_separator = ","
        elif text == "; (セミコロン)":
            self.selected_separator = ";"
        elif text == ": (コロン)":
            self.selected_separator = ":"
        elif text == "| (パイプ)":
            self.selected_separator = "|"
        elif text == "- (ハイフン)":
            self.selected_separator = "-"
        elif text == "_ (アンダースコア)":
            self.selected_separator = "_"
        elif text == "なし (区切り文字なし)":
            self.selected_separator = ""
        else:
            self.selected_separator = text

    def _on_accept(self):
        self._update_separator(self.separator_combo.currentText())
        self.accept()

    def get_separator(self):
        return self.selected_separator

class PriceCalculatorDialog(QDialog):
    """金額計算ツールダイアログ"""
    def __init__(self, parent=None, headers=None, initial_column_name=None):
        super().__init__(parent)
        self.headers = headers if headers is not None else []
        self.initial_column_name = initial_column_name
        self.setWindowTitle("金額計算ツール")
        self.setMinimumSize(300, 200)
        self.result = {}
        self.setupUi()
        self.connectSignals()
        self._apply_initial_selection() # ⭐ 追加

    def setupUi(self):
        layout = QVBoxLayout(self)

        column_group = QGroupBox("対象列")
        column_layout = QHBoxLayout(column_group)
        column_layout.addWidget(QLabel("金額列:"))
        self.column_combo = QComboBox()
        self.column_combo.addItems(self.headers)
        column_layout.addWidget(self.column_combo)
        layout.addWidget(column_group)

        tax_group = QGroupBox("税計算")
        tax_layout = QVBoxLayout(tax_group)
        self.tax_exclusive_radio = QRadioButton("税抜価格から計算 (10%税込み)")
        self.tax_inclusive_radio = QRadioButton("税込価格から計算 (税率考慮なし)")
        self.tax_exclusive_radio.setChecked(True)
        tax_layout.addWidget(self.tax_exclusive_radio)
        tax_layout.addWidget(self.tax_inclusive_radio)
        layout.addWidget(tax_group)

        discount_group = QGroupBox("割引率")
        discount_layout = QHBoxLayout(discount_group)
        discount_layout.addWidget(QLabel("割引率 (%):"))
        self.discount_spin = QDoubleSpinBox()
        self.discount_spin.setRange(0.0, 100.0)
        self.discount_spin.setValue(0.0)
        self.discount_spin.setSuffix("%")
        discount_layout.addWidget(self.discount_spin)
        layout.addWidget(discount_group)
        
        round_group = QGroupBox("丸め方")
        round_layout = QVBoxLayout(round_group)
        self.round_truncate_radio = QRadioButton("小数点以下を切り捨て")
        self.round_round_radio = QRadioButton("四捨五入")
        self.round_ceil_radio = QRadioButton("小数点以下を切り上げ")
        self.round_truncate_radio.setChecked(True)
        round_layout.addWidget(self.round_truncate_radio)
        round_layout.addWidget(self.round_round_radio)
        round_layout.addWidget(self.round_ceil_radio)
        layout.addWidget(round_group)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self._on_accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def connectSignals(self):
        pass

    def _on_accept(self):
        self.result = {
            'column': self.column_combo.currentText(),
            'tax_status': 'exclusive' if self.tax_exclusive_radio.isChecked() else 'inclusive',
            'discount': self.discount_spin.value(),
            'round_mode': ('truncate' if self.round_truncate_radio.isChecked() else
                           'round' if self.round_round_radio.isChecked() else 'ceil')
        }
        self.accept()

    def _apply_initial_selection(self):
        """初期選択列の自動セット"""
        if self.initial_column_name and self.initial_column_name in self.headers:
            idx = self.headers.index(self.initial_column_name)
            self.column_combo.setCurrentIndex(idx)
        # else: 何もしなくてもデフォルトで先頭が選ばれる

class PasteOptionDialog(QDialog):
    """貼り付けオプションを選択するダイアログ"""
    def __init__(self, parent=None, show_delimiter_option=True):
        super().__init__(parent)
        self.setWindowTitle("貼り付けオプション")
        self.setMinimumSize(300, 150)
        self.result = {}
        self.show_delimiter_option = show_delimiter_option
        self.setupUi()
        self.connectSignals()

    def setupUi(self):
        layout = QVBoxLayout(self)

        self.normal_radio = QRadioButton("通常貼り付け (タブ区切りを維持)")
        self.normal_radio.setChecked(True)
        layout.addWidget(self.normal_radio)

        self.single_column_radio = QRadioButton("単一列として貼り付け (改行区切り)")
        layout.addWidget(self.single_column_radio)
        
        if self.show_delimiter_option:
            self.custom_delimiter_radio = QRadioButton("カスタム区切り文字で解析")
            layout.addWidget(self.custom_delimiter_radio)
            
            delimiter_layout = QHBoxLayout()
            self.custom_delimiter_entry = QLineEdit(",")
            self.custom_delimiter_entry.setEnabled(False)
            delimiter_layout.addWidget(QLabel("区切り文字:"))
            delimiter_layout.addWidget(self.custom_delimiter_entry)
            layout.addLayout(delimiter_layout)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self._on_accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def connectSignals(self):
        if self.show_delimiter_option:
            self.custom_delimiter_radio.toggled.connect(self.custom_delimiter_entry.setEnabled)

    def _on_accept(self):
        if self.normal_radio.isChecked():
            self.result['mode'] = 'normal'
            self.result['delimiter'] = '\t'
        elif self.single_column_radio.isChecked():
            self.result['mode'] = 'single_column'
            self.result['delimiter'] = '\n'
        elif self.show_delimiter_option and self.custom_delimiter_radio.isChecked():
            self.result['mode'] = 'custom_delimiter'
            self.result['delimiter'] = self.custom_delimiter_entry.text()
        self.accept()

    def get_selected_mode(self):
        return self.result.get('mode', 'normal')

    def get_custom_delimiter(self):
        return self.result.get('delimiter', '')

class TextProcessingDialog(QDialog):
    """テキスト処理ツールダイアログ"""
    
    def __init__(self, parent, headers):
        super().__init__(parent)
        self.headers = headers
        self.result = None
        self.setupUi()
        self.connectSignals()
        
    def setupUi(self):
        self.setWindowTitle("テキスト処理ツール")
        self.setMinimumSize(500, 400)
        
        layout = QVBoxLayout(self)
        
        target_group = QGroupBox("対象設定")
        target_layout = QGridLayout(target_group)
        
        target_layout.addWidget(QLabel("対象列:"), 0, 0)
        self.column_combo = QComboBox()
        self.column_combo.addItems(self.headers)
        target_layout.addWidget(self.column_combo, 0, 1)
        
        layout.addWidget(target_group)
        
        prefix_group = QGroupBox("接頭辞設定")
        prefix_layout = QVBoxLayout(prefix_group)
        
        self.add_prefix_check = QCheckBox("接頭辞を追加する")
        self.add_prefix_check.setChecked(True)
        prefix_layout.addWidget(self.add_prefix_check)
        
        prefix_input_layout = QHBoxLayout()
        prefix_input_layout.addWidget(QLabel("追加する文字:"))
        self.prefix_edit = QLineEdit("ポイントアップしてます！")
        prefix_input_layout.addWidget(self.prefix_edit)
        prefix_input_layout.addStretch() # 追加
        prefix_layout.addLayout(prefix_input_layout) #

        layout.addWidget(prefix_group)
        
        byte_group = QGroupBox("バイト数制限")
        byte_layout = QVBoxLayout(byte_group)
        
        self.apply_limit_radio = QRadioButton("制限を適用する")
        self.no_limit_radio = QRadioButton("制限しない")
        self.apply_limit_radio.setChecked(True)
        byte_layout.addWidget(self.apply_limit_radio)
        byte_layout.addWidget(self.no_limit_radio)
        
        limit_layout = QHBoxLayout()
        limit_layout.addWidget(QLabel("最大バイト数:"))
        self.byte_limit_spin = QSpinBox()
        self.byte_limit_spin.setRange(10, 1000)
        self.byte_limit_spin.setValue(150)
        self.byte_limit_spin.setSuffix(" バイト")
        limit_layout.addWidget(self.byte_limit_spin)
        limit_layout.addWidget(QLabel("(半角文字換算)"))
        limit_layout.addStretch()
        byte_layout.addLayout(limit_layout)
        
        layout.addWidget(byte_group)
        
        word_group = QGroupBox("単語境界調整")
        word_layout = QVBoxLayout(word_group)
        
        self.trim_end_check = QCheckBox("行末の空白を事前に削除")
        self.trim_end_check.setChecked(True)
        word_layout.addWidget(self.trim_end_check)
        
        self.remove_partial_word_check = QCheckBox("行末の不完全な単語を削除")
        self.remove_partial_word_check.setChecked(True)
        word_layout.addWidget(self.remove_partial_word_check)
        
        help_label = QLabel("※ 最後の半角スペース以降を削除して自然な文章にします")
        help_label.setStyleSheet("color: #666; font-size: 10px;")
        word_layout.addWidget(help_label)
        
        layout.addWidget(word_group)
        
        preview_group = QGroupBox("プレビュー")
        preview_layout = QVBoxLayout(preview_group)
        
        preview_layout.addWidget(QLabel("処理前:"))
        self.preview_before = QLineEdit()
        self.preview_before.setPlaceholderText("列を選択すると最初の行が表示されます")
        self.preview_before.setReadOnly(True)
        preview_layout.addWidget(self.preview_before)
        
        preview_layout.addWidget(QLabel("処理後:"))
        self.preview_after = QLineEdit()
        self.preview_after.setReadOnly(True)
        preview_layout.addWidget(self.preview_after)
        
        self.byte_info_label = QLabel("バイト数: -")
        preview_layout.addWidget(self.byte_info_label)
        
        self.preview_button = QPushButton("プレビュー更新")
        preview_layout.addWidget(self.preview_button)
        
        layout.addWidget(preview_group)
        
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.button(QDialogButtonBox.Ok).setText("実行")
        button_box.button(QDialogButtonBox.Cancel).setText("キャンセル")
        layout.addWidget(button_box)
        
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        
    def connectSignals(self):
        self.column_combo.currentTextChanged.connect(self.updatePreview)
        self.add_prefix_check.toggled.connect(self.updatePreview)
        self.prefix_edit.textChanged.connect(self.updatePreview)
        self.apply_limit_radio.toggled.connect(self.updatePreview)
        self.no_limit_radio.toggled.connect(self.updatePreview)
        self.byte_limit_spin.valueChanged.connect(self.updatePreview)
        self.trim_end_check.toggled.connect(self.updatePreview)
        self.remove_partial_word_check.toggled.connect(self.updatePreview)
        self.preview_button.clicked.connect(self.updatePreview)
        
        QTimer.singleShot(100, self.updatePreview)
        
    def updatePreview(self):
        try:
            parent_window = self.parent()
            if not hasattr(parent_window, 'table_model'):
                self.preview_before.setText("データモデルが見つかりません。")
                self.preview_after.setText("")
                self.byte_info_label.setText("バイト数: -")
                return
                
            column = self.column_combo.currentText()
            if not column:
                self.preview_before.setText("列が選択されていません。")
                self.preview_after.setText("")
                self.byte_info_label.setText("バイト数: -")
                return
                
            headers = parent_window.table_model._headers
            if column not in headers:
                self.preview_before.setText(f"列 '{column}' がデータモデルに見つかりません。")
                self.preview_after.setText("")
                self.byte_info_label.setText("バイト数: -")
                return
                
            col_index = headers.index(column)
            if parent_window.table_model.rowCount() == 0:
                self.preview_before.setText("データが空です。")
                self.preview_after.setText("")
                self.byte_info_label.setText("バイト数: -")
                return
                
            original_text = str(parent_window.table_model.data(
                parent_window.table_model.index(0, col_index), Qt.DisplayRole) or "")
            
            processed_text = self.processText(original_text)
            
            self.preview_before.setText(original_text)
            self.preview_after.setText(processed_text)
            
            original_bytes = self.get_byte_length(original_text)
            processed_bytes = self.get_byte_length(processed_text)
            self.byte_info_label.setText(f"バイト数: {original_bytes} → {processed_bytes}")
            
        except Exception as e:
            print(f"Preview update error: {e}")
            self.preview_before.setText(f"プレビューエラー: {e}")
            self.preview_after.setText("")
            self.byte_info_label.setText("バイト数: -")
            
    def processText(self, text):
        result = text
        
        if self.add_prefix_check.isChecked():
            prefix = self.prefix_edit.text()
            result = prefix + result
        
        if self.apply_limit_radio.isChecked():
            max_bytes = self.byte_limit_spin.value()
            result = self.limitByBytes(result, max_bytes)
        
        if self.trim_end_check.isChecked():
            result = result.rstrip()
            
        if self.remove_partial_word_check.isChecked():
            result = self.removePartialWord(result)
        
        return result
        
    def limitByBytes(self, text, max_bytes):
        if self.get_byte_length(text) <= max_bytes:
            return text
            
        result = text
        while len(result) > 0 and self.get_byte_length(result) > max_bytes:
            result = result[:-1]
        
        return result
        
    def removePartialWord(self, text):
        import re
        return re.sub(r'\s+[^\s]*$', '', text)
        
    def get_byte_length(self, text):
        byte_length = 0
        for char in text:
            char_code = ord(char)
            if ((0x0020 <= char_code <= 0x007e) or
                (0xff61 <= char_code <= 0xff9f)):
                byte_length += 1
            else:
                byte_length += 2
        return byte_length
        
    def getSettings(self):
        return {
            'column': self.column_combo.currentText(),
            'add_prefix': self.add_prefix_check.isChecked(),
            'prefix': self.prefix_edit.text(),
            'apply_limit': self.apply_limit_radio.isChecked(),
            'max_bytes': self.byte_limit_spin.value(),
            'trim_end': self.trim_end_check.isChecked(),
            'remove_partial_word': self.remove_partial_word_check.isChecked()
        }

class NewFileDialog(QDialog):
    """新規CSVファイル作成時の項目設定ダイアログ"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("新規CSVファイルの作成")
        self.setMinimumSize(500, 400)
        self.result_columns = []
        self.setupUi()
        self.connectSignals()
        
    def setupUi(self):
        layout = QVBoxLayout(self)
        
        # 説明ラベル
        info_label = QLabel("CSVファイルの項目（列）を設定してください。")
        info_label.setWordWrap(True)
        layout.addWidget(info_label)
        
        # 新規項目入力部
        input_group = QGroupBox("新規項目名")
        input_layout = QHBoxLayout(input_group)
        
        self.new_item_edit = QLineEdit()
        self.new_item_edit.setPlaceholderText("項目名を入力してEnterキーまたは追加ボタン")
        input_layout.addWidget(self.new_item_edit)
        
        self.add_button = QPushButton("追加")
        self.add_button.setMaximumWidth(80)
        input_layout.addWidget(self.add_button)
        
        layout.addWidget(input_group)
        
        # 項目リスト
        list_group = QGroupBox("項目リスト")
        list_layout = QVBoxLayout(list_group)
        
        self.items_list = QListWidget()
        self.items_list.setDragDropMode(QListWidget.InternalMove)
        list_layout.addWidget(self.items_list)
        
        # リスト操作ボタン
        button_layout = QHBoxLayout()
        
        self.edit_button = QPushButton("修正")
        self.delete_button = QPushButton("削除")
        self.up_button = QPushButton("上へ")
        self.down_button = QPushButton("下へ")
        
        button_layout.addWidget(self.edit_button)
        button_layout.addWidget(self.delete_button)
        button_layout.addStretch()
        button_layout.addWidget(self.up_button)
        button_layout.addWidget(self.down_button)
        
        list_layout.addLayout(button_layout)
        layout.addWidget(list_group)
        
        # 初期行数設定
        row_group = QGroupBox("初期設定")
        row_layout = QHBoxLayout(row_group)
        
        row_layout.addWidget(QLabel("初期行数:"))
        self.initial_rows_spin = QSpinBox()
        self.initial_rows_spin.setRange(0, 1000)
        self.initial_rows_spin.setValue(1)
        self.initial_rows_spin.setSuffix(" 行")
        row_layout.addWidget(self.initial_rows_spin)
        row_layout.addStretch()
        
        layout.addWidget(row_group)
        
        # ダイアログボタン
        button_box = QDialogButtonBox()
        self.create_button = button_box.addButton("作成", QDialogButtonBox.AcceptRole)
        self.cancel_button = button_box.addButton("キャンセル", QDialogButtonBox.RejectRole)
        
        self.create_button.setEnabled(False)
        
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        
        layout.addWidget(button_box)
        
        # 初期フォーカス
        self.new_item_edit.setFocus()
        
    def connectSignals(self):
        # 項目追加
        self.add_button.clicked.connect(self.add_item)
        self.new_item_edit.returnPressed.connect(self.add_item)
        
        # リスト操作
        self.edit_button.clicked.connect(self.edit_item)
        self.delete_button.clicked.connect(self.delete_item)
        self.up_button.clicked.connect(self.move_up)
        self.down_button.clicked.connect(self.move_down)
        
        # リスト選択変更
        self.items_list.itemSelectionChanged.connect(self.update_button_states)
        
        # ダブルクリックで編集
        self.items_list.itemDoubleClicked.connect(self.edit_item)
        
    def add_item(self):
        """新規項目を追加"""
        item_name = self.new_item_edit.text().strip()
        
        if not item_name:
            return
            
        # 重複チェック
        existing_items = [self.items_list.item(i).text() 
                          for i in range(self.items_list.count())]
        
        if item_name in existing_items:
            QMessageBox.warning(self, "重複エラー", 
                                f"項目名「{item_name}」は既に存在します。")
            return
            
        # リストに追加
        self.items_list.addItem(item_name)
        self.new_item_edit.clear()
        self.new_item_edit.setFocus()
        
        # 作成ボタンを有効化
        self.create_button.setEnabled(self.items_list.count() > 0)
        self.update_button_states()
        
    def edit_item(self):
        """選択項目を編集"""
        current_item = self.items_list.currentItem()
        if not current_item:
            return
            
        old_name = current_item.text()
        new_name, ok = QInputDialog.getText(self, "項目名の修正", 
                                            "新しい項目名:", 
                                            text=old_name)
        
        if ok and new_name and new_name != old_name:
            # 重複チェック
            existing_items = [self.items_list.item(i).text() 
                              for i in range(self.items_list.count())
                              if self.items_list.item(i) != current_item]
            
            if new_name in existing_items:
                QMessageBox.warning(self, "重複エラー", 
                                    f"項目名「{new_name}」は既に存在します。")
                return
                
            current_item.setText(new_name)
            
    def delete_item(self):
        """選択項目を削除"""
        current_row = self.items_list.currentRow()
        if current_row >= 0:
            self.items_list.takeItem(current_row)
            self.create_button.setEnabled(self.items_list.count() > 0)
            self.update_button_states()
            
    def move_up(self):
        """選択項目を上へ移動"""
        current_row = self.items_list.currentRow()
        if current_row > 0:
            item = self.items_list.takeItem(current_row)
            self.items_list.insertItem(current_row - 1, item)
            self.items_list.setCurrentRow(current_row - 1)
            
    def move_down(self):
        """選択項目を下へ移動"""
        current_row = self.items_list.currentRow()
        if current_row < self.items_list.count() - 1:
            item = self.items_list.takeItem(current_row)
            self.items_list.insertItem(current_row + 1, item)
            self.items_list.setCurrentRow(current_row + 1)
            
    def update_button_states(self):
        """ボタンの有効/無効を更新"""
        has_selection = self.items_list.currentRow() >= 0
        current_row = self.items_list.currentRow()
        
        self.edit_button.setEnabled(has_selection)
        self.delete_button.setEnabled(has_selection)
        self.up_button.setEnabled(has_selection and current_row > 0)
        self.down_button.setEnabled(has_selection and 
                                    current_row < self.items_list.count() - 1)
        
    def get_result(self):
        """作成結果を取得"""
        columns = [self.items_list.item(i).text() 
                   for i in range(self.items_list.count())]
        initial_rows = self.initial_rows_spin.value()
        
        return {
            'columns': columns,
            'initial_rows': initial_rows
        }

# dialogs.py の最後に追加
class RemoveDuplicatesDialog(QDialog):
    """重複行削除の設定ダイアログ"""
    
    def __init__(self, parent=None, headers=None):
        super().__init__(parent)
        self.headers = headers or []
        self.setWindowTitle("重複行の削除")
        self.setMinimumSize(400, 300)
        self.result = {}
        self.setupUi()
    
    def setupUi(self):
        layout = QVBoxLayout(self)
        
        # 説明ラベル
        info_label = QLabel("重複を判定する基準を選択してください。")
        info_label.setWordWrap(True)
        layout.addWidget(info_label)
        
        # 重複判定基準
        criteria_group = QGroupBox("重複判定基準")
        criteria_layout = QVBoxLayout(criteria_group)
        
        self.all_columns_radio = QRadioButton("すべての列が一致する行を重複とみなす")
        self.all_columns_radio.setChecked(True)
        criteria_layout.addWidget(self.all_columns_radio)
        
        self.selected_columns_radio = QRadioButton("選択した列が一致する行を重複とみなす")
        criteria_layout.addWidget(self.selected_columns_radio)
        
        # 列選択リスト
        self.column_list = QListWidget()
        self.column_list.setSelectionMode(QListWidget.MultiSelection)
        self.column_list.setEnabled(False)
        for header in self.headers:
            self.column_list.addItem(header)
        criteria_layout.addWidget(self.column_list)
        
        layout.addWidget(criteria_group)
        
        # 保持する行の選択
        keep_group = QGroupBox("保持する行")
        keep_layout = QVBoxLayout(keep_group)
        
        self.keep_first_radio = QRadioButton("最初の行を保持（デフォルト）")
        self.keep_first_radio.setChecked(True)
        self.keep_last_radio = QRadioButton("最後の行を保持")
        
        keep_layout.addWidget(self.keep_first_radio)
        keep_layout.addWidget(self.keep_last_radio)
        
        layout.addWidget(keep_group)
        
        # プレビュー情報
        self.preview_label = QLabel("重複行数: 計算中...")
        layout.addWidget(self.preview_label)
        
        # ボタン
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
        
        # シグナル接続
        self.selected_columns_radio.toggled.connect(self.column_list.setEnabled)
        
        # プレビュー更新のトリガー
        self.all_columns_radio.toggled.connect(self._update_preview)
        self.selected_columns_radio.toggled.connect(self._update_preview)
        self.column_list.itemSelectionChanged.connect(self._update_preview)
        self.keep_first_radio.toggled.connect(self._update_preview)
        self.keep_last_radio.toggled.connect(self._update_preview)
        
        # 初期プレビュー更新
        QTimer.singleShot(100, self._update_preview)

    def _update_preview(self):
        """重複行数を計算し、プレビューラベルを更新する"""
        try:
            parent_window = self.parent()
            if not hasattr(parent_window, 'table_model'):
                self.preview_label.setText("重複行数: データモデルが見つかりません。")
                return
            
            current_df = parent_window.table_model.get_dataframe()
            if current_df is None or current_df.empty:
                self.preview_label.setText("重複行数: 0 (データがありません)")
                return
            
            total_rows = len(current_df)
            
            temp_settings = self.get_result() # 現在のダイアログ設定を取得
            
            if temp_settings['use_all_columns']:
                df_unique = current_df.drop_duplicates(keep=temp_settings['keep'])
            else:
                if not temp_settings['selected_columns']:
                    self.preview_label.setText("重複行数: 列を選択してください")
                    return
                # 選択された列がDataFrameに存在するかチェック
                valid_columns = [col for col in temp_settings['selected_columns'] if col in current_df.columns]
                if not valid_columns:
                    self.preview_label.setText("重複行数: 選択された列がデータに見つかりません")
                    return
                df_unique = current_df.drop_duplicates(subset=valid_columns, keep=temp_settings['keep'])
            
            removed_count = total_rows - len(df_unique)
            self.preview_label.setText(f"重複行数: {removed_count}行 (総行数: {total_rows}行)")
            
        except Exception as e:
            self.preview_label.setText(f"重複行数: 計算エラー ({e})")
            print(f"Error updating duplicate preview: {e}")
            
    def get_result(self):
        selected_columns = []
        if self.selected_columns_radio.isChecked():
            for i in range(self.column_list.count()):
                item = self.column_list.item(i)
                if item.isSelected():
                    selected_columns.append(item.text())
        
        return {
            'use_all_columns': self.all_columns_radio.isChecked(),
            'selected_columns': selected_columns,
            'keep': 'first' if self.keep_first_radio.isChecked() else 'last'
        }