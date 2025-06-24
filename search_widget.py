# search_widget.py

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QGridLayout, QGroupBox, QRadioButton, QComboBox,
    QLineEdit, QCheckBox, QPushButton, QLabel, QListWidget, QHBoxLayout,
    QTextEdit, QFileDialog, QMessageBox, QTabWidget
)
from PySide6.QtCore import Signal, Qt
import os
import pandas as pd
import csv

class SearchWidget(QWidget):
    """
    ドッキング可能な検索・置換パネルのUI。
    """
    analysis_requested = Signal(dict)
    find_next_clicked = Signal(dict)
    find_prev_clicked = Signal(dict)
    replace_one_clicked = Signal(dict)
    replace_all_clicked = Signal(dict)
    extract_clicked = Signal(dict)
    
    replace_from_file_requested = Signal(dict)

    def __init__(self, headers, parent=None):
        super().__init__(parent)
        self.headers = headers
        self._create_widgets()
        self._connect_signals()
        # _update_ui_state はタブ内のウィジェットの状態を管理するため、直接呼び出すのではなく、
        # 親子関係グループのチェック状態に基づいて、タブ切り替え時にも正しい状態になるようにする
        # self._update_ui_state(self.parent_child_group.isChecked()) # 元々あったが、削除しても動作に影響なし
        self._update_replace_from_file_ui_state(self.replace_from_file_group.isChecked())

    def _create_widgets(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(5, 5, 5, 5)

        # ✅ タブウィジェットを作成
        self.tab_widget = QTabWidget()
        main_layout.addWidget(self.tab_widget)
        
        # ========== タブ1: 通常の検索・置換 ==========
        tab1 = QWidget()
        tab1_layout = QVBoxLayout(tab1)
        
        # 検索と置換
        search_group = QGroupBox("検索と置換")
        search_layout = QGridLayout(search_group)
        self.search_entry = QLineEdit()
        self.replace_entry = QLineEdit()
        search_layout.addWidget(QLabel("検索:"), 0, 0)
        search_layout.addWidget(self.search_entry, 0, 1, 1, 2)
        search_layout.addWidget(QLabel("置換:"), 1, 0)
        search_layout.addWidget(self.replace_entry, 1, 1, 1, 2)
        tab1_layout.addWidget(search_group)

        # 親子関係モード
        self.parent_child_group = QGroupBox("親子関係モード")
        self.parent_child_group.setCheckable(True)
        self.parent_child_group.setChecked(False)
        pc_layout = QVBoxLayout(self.parent_child_group)
        
        pc_config_layout = QHBoxLayout()
        self.column_combo = QComboBox()
        self.column_combo.addItems(self.headers)
        self.analyze_button = QPushButton("分析")
        pc_config_layout.addWidget(QLabel("基準列:"))
        pc_config_layout.addWidget(self.column_combo)
        pc_config_layout.addWidget(self.analyze_button)
        pc_layout.addLayout(pc_config_layout)
        
        self.analysis_text = QTextEdit()
        self.analysis_text.setReadOnly(True)
        self.analysis_text.setPlaceholderText("「分析実行」ボタンを押してください。")
        self.analysis_text.setFixedHeight(80)
        pc_layout.addWidget(self.analysis_text)
        
        # 分析方式の選択UI
        analysis_mode_group = QGroupBox("分析方式")
        analysis_mode_layout = QHBoxLayout(analysis_mode_group)
        self.mode_consecutive_radio = QRadioButton("連続グループ")
        self.mode_global_radio = QRadioButton("グローバル")
        self.mode_consecutive_radio.setChecked(True)
        self.mode_consecutive_radio.setToolTip("隣接する同じ値の行を1つのグループとして扱います。")
        self.mode_global_radio.setToolTip("ファイル全体で同じ値を持つ全ての行を1つのグループとして扱います。")
        analysis_mode_layout.addWidget(self.mode_consecutive_radio)
        analysis_mode_layout.addWidget(self.mode_global_radio)
        pc_layout.addWidget(analysis_mode_group)

        pc_target_group = QGroupBox("対象")
        pc_target_layout = QHBoxLayout(pc_target_group)
        self.target_all_radio = QRadioButton("全て")
        self.target_parent_radio = QRadioButton("親のみ")
        self.target_child_radio = QRadioButton("子のみ")
        self.target_all_radio.setChecked(True)
        pc_target_layout.addWidget(self.target_all_radio)
        pc_target_layout.addWidget(self.target_parent_radio)
        pc_target_layout.addWidget(self.target_child_radio)
        pc_layout.addWidget(pc_target_group)
        tab1_layout.addWidget(self.parent_child_group)

        # 対象列
        columns_group = QGroupBox("対象列")
        columns_layout = QVBoxLayout(columns_group)
        self.col_listwidget = QListWidget()
        self.col_listwidget.setSelectionMode(QListWidget.ExtendedSelection)
        self.col_listwidget.addItems(self.headers)
        self.col_listwidget.selectAll()
        columns_layout.addWidget(self.col_listwidget)
        tab1_layout.addWidget(columns_group)

        # オプション
        options_layout = QHBoxLayout()
        self.regex_check = QCheckBox("正規表現")
        self.case_check = QCheckBox("大文字/小文字を区別")
        self.selection_check = QCheckBox("選択範囲のみ")
        options_layout.addWidget(self.regex_check)
        options_layout.addWidget(self.case_check)
        options_layout.addWidget(self.selection_check)
        tab1_layout.addLayout(options_layout)

        # 通常のボタン
        buttons_layout = QGridLayout()
        self.find_prev_button = QPushButton("前を検索")
        self.find_next_button = QPushButton("次を検索")
        self.replace_button = QPushButton("置換")
        self.replace_all_button = QPushButton("すべて置換")
        self.extract_button = QPushButton("抽出")

        buttons_layout.addWidget(self.find_prev_button, 0, 0)
        buttons_layout.addWidget(self.find_next_button, 0, 1)
        buttons_layout.addWidget(self.replace_button, 1, 0)
        buttons_layout.addWidget(self.replace_all_button, 1, 1)
        buttons_layout.addWidget(self.extract_button, 2, 0, 1, 2)
        tab1_layout.addLayout(buttons_layout)
        
        tab1_layout.addStretch()
        
        # ========== タブ2: ファイル参照置換 ==========
        tab2 = QWidget()
        tab2_layout = QVBoxLayout(tab2)
        
        # ファイル参照置換設定
        self.replace_from_file_group = QGroupBox("ファイル参照置換")
        self.replace_from_file_group.setCheckable(True)
        self.replace_from_file_group.setChecked(False)
        rff_layout = QGridLayout(self.replace_from_file_group)
        
        rff_layout.addWidget(QLabel("自ファイルの対象項目:"), 0, 0)
        self.target_col_combo = QComboBox()
        self.target_col_combo.addItems(self.headers)
        rff_layout.addWidget(self.target_col_combo, 0, 1, 1, 2)

        rff_layout.addWidget(QLabel("参照ファイル名:"), 1, 0)
        self.lookup_filepath_entry = QLineEdit()
        self.lookup_filepath_entry.setReadOnly(True)
        rff_layout.addWidget(self.lookup_filepath_entry, 1, 1)
        self.browse_lookup_file_button = QPushButton("参照...")
        rff_layout.addWidget(self.browse_lookup_file_button, 1, 2)

        rff_layout.addWidget(QLabel("検出エンコーディング:"), 2, 0)
        self.lookup_file_encoding_label = QLabel("未検出")
        rff_layout.addWidget(self.lookup_file_encoding_label, 2, 1, 1, 2)

        rff_layout.addWidget(QLabel("参照ファイル 検索項目:"), 3, 0)
        self.lookup_key_col_combo = QComboBox()
        self.lookup_key_col_combo.addItems([])
        rff_layout.addWidget(self.lookup_key_col_combo, 3, 1, 1, 2)

        rff_layout.addWidget(QLabel("参照ファイル 置換項目:"), 4, 0)
        self.replace_val_col_combo = QComboBox()
        self.replace_val_col_combo.addItems([])
        rff_layout.addWidget(self.replace_val_col_combo, 4, 1, 1, 2)
        
        tab2_layout.addWidget(self.replace_from_file_group)
        
        # ファイル参照置換実行ボタン
        self.replace_from_file_execute_button = QPushButton("ファイル参照置換実行")
        self.replace_from_file_execute_button.setMinimumHeight(40)
        tab2_layout.addWidget(self.replace_from_file_execute_button)
        
        # 使い方の説明を追加
        help_text = QLabel(
            "【使い方】\n"
            "1. 「自ファイルの対象項目」で置換したい列を選択\n"
            "2. 「参照...」ボタンで参照用CSVファイルを選択\n"
            "3. 「検索項目」でキーとなる列を選択\n"
            "4. 「置換項目」で置換する値の列を選択\n"
            "5. 「ファイル参照置換実行」ボタンをクリック"
        )
        help_text.setWordWrap(True)
        help_text.setStyleSheet("QLabel { color: #666; padding: 10px; }")
        tab2_layout.addWidget(help_text)
        
        tab2_layout.addStretch()
        
        # タブを追加
        self.tab_widget.addTab(tab1, "検索・置換・抽出")
        self.tab_widget.addTab(tab2, "ファイル参照置換")

    def _connect_signals(self):
        self.parent_child_group.toggled.connect(self._update_ui_state)
        self.replace_from_file_group.toggled.connect(self._update_replace_from_file_ui_state)
        
        self.analyze_button.clicked.connect(lambda: self.analysis_requested.emit(self.get_settings()))
        self.find_next_button.clicked.connect(lambda: self.find_next_clicked.emit(self.get_settings()))
        self.find_prev_button.clicked.connect(lambda: self.find_prev_clicked.emit(self.get_settings()))
        self.replace_button.clicked.connect(lambda: self.replace_one_clicked.emit(self.get_settings()))
        self.replace_all_button.clicked.connect(lambda: self.replace_all_clicked.emit(self.get_settings()))
        self.extract_button.clicked.connect(lambda: self.extract_clicked.emit(self.get_settings()))
        
        self.browse_lookup_file_button.clicked.connect(self._browse_lookup_file)
        self.replace_from_file_execute_button.clicked.connect(self._execute_replace_from_file)
        
        self.target_col_combo.currentTextChanged.connect(self._update_replace_from_file_execute_button_state)
        self.lookup_filepath_entry.textChanged.connect(self._update_replace_from_file_execute_button_state)
        self.lookup_key_col_combo.currentTextChanged.connect(self._update_replace_from_file_execute_button_state)
        self.replace_val_col_combo.currentTextChanged.connect(self._update_replace_from_file_execute_button_state)

    def _update_ui_state(self, is_checked):
        # 親子関係モードの全ウィジェットの有効/無効を切り替え
        for i in range(self.parent_child_group.layout().count()):
            item = self.parent_child_group.layout().itemAt(i)
            if item.widget():
                item.widget().setEnabled(is_checked)
            elif item.layout():
                # ネストされたレイアウト内のウィジェットも処理
                layout = item.layout()
                for j in range(layout.count()):
                    sub_item = layout.itemAt(j)
                    if sub_item and sub_item.widget():
                        sub_item.widget().setEnabled(is_checked)

    def _update_replace_from_file_ui_state(self, is_checked):
        self.target_col_combo.setEnabled(is_checked)
        self.lookup_filepath_entry.setEnabled(is_checked)
        self.browse_lookup_file_button.setEnabled(is_checked)
        self.lookup_file_encoding_label.setEnabled(is_checked)
        self.lookup_key_col_combo.setEnabled(is_checked and self.lookup_filepath_entry.text() != "")
        self.replace_val_col_combo.setEnabled(is_checked and self.lookup_filepath_entry.text() != "")
        
        self._update_replace_from_file_execute_button_state()

    def _update_replace_from_file_execute_button_state(self):
        is_ready = self.replace_from_file_group.isChecked() and \
                   self.target_col_combo.currentText() != "" and \
                   self.lookup_filepath_entry.text() != "" and \
                   self.lookup_key_col_combo.currentText() != "" and \
                   self.replace_val_col_combo.currentText() != "" and \
                   self.lookup_file_encoding_label.text() not in ["未検出", "エラー", "検出失敗"]
        
        self.replace_from_file_execute_button.setEnabled(is_ready)

    def _browse_lookup_file(self):
        current_filepath = self.parent().filepath if hasattr(self.parent(), 'filepath') else ""

        filepath, _ = QFileDialog.getOpenFileName(
            self,
            "参照ファイルを開く",
            os.path.dirname(current_filepath) if current_filepath else "",
            "CSVファイル (*.csv);;テキストファイル (*.txt);;すべてのファイル (*.*)"
        )
        if not filepath:
            return

        detected_encoding = self._detect_encoding(filepath)
        if not detected_encoding:
            QMessageBox.critical(self, "エラー", "参照ファイルのエンコーディングを検出できませんでした。")
            self.lookup_filepath_entry.setText("")
            self.lookup_key_col_combo.clear()
            self.replace_val_col_combo.clear()
            self.lookup_file_encoding_label.setText("検出失敗")
            return

        try:
            lookup_df_sample = pd.read_csv(filepath, encoding=detected_encoding, nrows=0, dtype=str)
            lookup_headers = lookup_df_sample.columns.tolist()
            
            self.lookup_filepath_entry.setText(filepath)
            self.lookup_file_encoding_label.setText(f"{detected_encoding}")
            
            self.lookup_key_col_combo.clear()
            self.lookup_key_col_combo.addItems(lookup_headers)
            
            self.replace_val_col_combo.clear()
            self.replace_val_col_combo.addItems(lookup_headers)

            if lookup_headers:
                self.lookup_key_col_combo.setCurrentIndex(0)
                self.replace_val_col_combo.setCurrentIndex(0)
            
            self._update_replace_from_file_execute_button_state()

        except Exception as e:
            QMessageBox.critical(self, "ファイル読み込みエラー", f"参照ファイルの読み込みに失敗しました。\n{e}")
            self.lookup_filepath_entry.setText("")
            self.lookup_key_col_combo.clear()
            self.replace_val_col_combo.clear()
            self.lookup_file_encoding_label.setText("エラー")

    def _execute_replace_from_file(self):
        settings = {
            'target_col': self.target_col_combo.currentText(),
            'lookup_filepath': self.lookup_filepath_entry.text(),
            'lookup_file_encoding': self.lookup_file_encoding_label.text(),
            'lookup_key_col': self.lookup_key_col_combo.currentText(),
            'replace_val_col': self.replace_val_col_combo.currentText()
        }
        self.replace_from_file_requested.emit(settings)

    def _detect_encoding(self, filepath): # このメソッドは既に存在し、正しいインデントでした。
        for enc in ['utf-8-sig', 'utf-8', 'shift_jis', 'cp932', 'euc-jp', 'latin1']:
            try:
                with open(filepath, 'r', encoding=enc) as f:
                    f.read(1024)
                return enc
            except (UnicodeDecodeError, pd.errors.ParserError, csv.Error):
                continue
        return None

    def update_headers(self, new_headers: list):
        self.headers = new_headers
        self.column_combo.clear()
        self.column_combo.addItems(self.headers)
        self.col_listwidget.clear()
        self.col_listwidget.addItems(self.headers)
        self.col_listwidget.selectAll()
        
        self.target_col_combo.clear()
        self.target_col_combo.addItems(self.headers)
        if self.headers:
            self.target_col_combo.setCurrentIndex(0)
        
        self._update_replace_from_file_execute_button_state()

    def get_settings(self):
        target_type = "all"
        if self.target_parent_radio.isChecked(): target_type = "parent"
        elif self.target_child_radio.isChecked(): target_type = "child"
        
        analysis_mode = "global" if self.mode_global_radio.isChecked() else "consecutive"

        return {
            "search_term": self.search_entry.text(),
            "replace_term": self.replace_entry.text(),
            "target_columns": [item.text() for item in self.col_listwidget.selectedItems()],
            "is_regex": self.regex_check.isChecked(),
            "is_case_sensitive": self.case_check.isChecked(),
            "in_selection_only": self.selection_check.isChecked(),
            "is_parent_child_mode": self.parent_child_group.isChecked(),
            "key_column": self.column_combo.currentText(),
            "target_type": target_type,
            "analysis_mode": analysis_mode,
        }