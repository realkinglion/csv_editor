# search_widget.py
import os
import pandas as pd
import re 
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QFormLayout,
    QLineEdit, QTextEdit, QPlainTextEdit, QComboBox, QCheckBox, QRadioButton,
    QSpinBox, QDoubleSpinBox, QPushButton,
    QLabel, QProgressBar, QTableView, QListWidget, QAbstractItemView, 
    QGroupBox, QScrollArea, QDockWidget, QButtonGroup,
    QFileDialog, QMessageBox, QInputDialog, QProgressDialog, QDialogButtonBox,
    QTabWidget,
    QCompleter
)
from PySide6.QtGui import QKeySequence, QGuiApplication, QTextOption, QFont, QAction, QColor 
from PySide6.QtCore import Qt, Signal, Slot, QTimer, QModelIndex, QEvent, QObject, QStringListModel

class SearchWidget(QWidget):
    """
    検索、置換、抽出、ファイル参照置換、商品別割引適用
    の機能を提供するドックウィジェット内のウィジェット。
    """
    find_next_clicked = Signal(dict)
    find_prev_clicked = Signal(dict)
    replace_one_clicked = Signal(dict)
    replace_all_clicked = Signal(dict)
    extract_clicked = Signal(dict)
    analysis_requested = Signal(dict)
    replace_from_file_requested = Signal(dict)
    product_discount_requested = Signal(dict)
    bulk_extract_requested = Signal(dict) 

    def __init__(self, headers=None, parent=None):
        super().__init__(parent)
        self.headers = headers if headers is not None else []
        self.detected_encodings = {}
        
        self.settings_manager = None
        if parent and hasattr(parent, 'settings_manager'):
            self.settings_manager = parent.settings_manager
            print(f"設定マネージャーを取得しました: {self.settings_manager}")
        else:
            print(f"警告: 親ウィジェット({parent})に設定マネージャーがありません")
        
        self._create_widgets()
        self._connect_signals()
        self.update_headers(self.headers)
        
        self._setup_search_history()

    def _create_widgets(self):
        main_layout = QVBoxLayout(self)
        self.tab_widget = QTabWidget()
        main_layout.addWidget(self.tab_widget)

        # ========== タブ1: 検索・置換・抽出 ==========
        tab1 = QWidget()
        tab1_layout = QVBoxLayout(tab1)

        search_group = QGroupBox("検索条件")
        search_layout = QGridLayout(search_group)

        search_layout.addWidget(QLabel("検索語:"), 0, 0)
        self.search_entry = QComboBox()
        self.search_entry.setEditable(True)
        self.search_entry.setInsertPolicy(QComboBox.NoInsert)

        search_layout.addWidget(self.search_entry, 0, 1, 1, 2)
        
        # ⭐ ここからUI設計の改善 - 拡張された列選択UIの作成を呼び出す
        column_selection_group = self._create_enhanced_column_selection_ui()
        tab1_layout.addWidget(column_selection_group)
        # ⭐ ここまでUI設計の改善

        self.case_sensitive_check = QCheckBox("大文字・小文字を区別")
        search_layout.addWidget(self.case_sensitive_check, 2, 0, 1, 3) 

        self.regex_check = QCheckBox("正規表現を使用")
        search_layout.addWidget(self.regex_check, 3, 0, 1, 3) 

        self.in_selection_check = QCheckBox("選択範囲内のみ検索")
        search_layout.addWidget(self.in_selection_check, 4, 0, 1, 3) 
        tab1_layout.addWidget(search_group)

        # 検索ボタン
        button_layout = QHBoxLayout()
        self.find_prev_button = QPushButton("◀ 前を検索")
        self.find_next_button = QPushButton("次を検索 ▶")
        self.extract_button = QPushButton("抽出")
        button_layout.addWidget(self.find_prev_button)
        button_layout.addWidget(self.find_next_button)
        button_layout.addWidget(self.extract_button)
        tab1_layout.addLayout(button_layout)
        
        # ⭐ 検索グループ内に履歴クリアボタンを追加
        history_layout = QHBoxLayout()
        self.clear_history_button = QPushButton("履歴クリア")
        self.clear_history_button.setMaximumWidth(100)
        history_layout.addStretch()
        history_layout.addWidget(self.clear_history_button)
        search_layout.addLayout(history_layout, 5, 0, 1, 3) 


        # 置換
        replace_group = QGroupBox("置換")
        replace_layout = QGridLayout(replace_group)
        replace_layout.addWidget(QLabel("置換語:"), 0, 0)
        self.replace_entry = QLineEdit()
        replace_layout.addWidget(self.replace_entry, 0, 1, 1, 2)

        replace_button_layout = QHBoxLayout()
        self.replace_one_button = QPushButton("置換")
        self.replace_all_button = QPushButton("すべて置換")
        replace_button_layout.addWidget(self.replace_one_button)
        replace_button_layout.addWidget(self.replace_all_button)
        replace_layout.addLayout(replace_button_layout, 1, 0, 1, 3)
        tab1_layout.addWidget(replace_group)

        # 親子関係分析
        parent_child_group = QGroupBox("親子関係分析")
        parent_child_layout = QVBoxLayout(parent_child_group)
        parent_child_layout.addWidget(QLabel("キー列:"))
        self.parent_child_key_column_combo = QComboBox()
        self.parent_child_key_column_combo.addItem("選択してください")
        self.parent_child_key_column_combo.addItems(self.headers)
        parent_child_layout.addWidget(self.parent_child_key_column_combo)

        radio_layout = QHBoxLayout()
        self.consecutive_radio = QRadioButton("連続する同じ値でグループ化")
        self.global_radio = QRadioButton("ファイル全体で同じ値でグループ化")
        self.consecutive_radio.setChecked(True)
        radio_layout.addWidget(self.consecutive_radio)
        radio_layout.addWidget(self.global_radio)
        parent_child_layout.addLayout(radio_layout)

        self.analyze_button = QPushButton("親子関係を分析")
        parent_child_layout.addWidget(self.analyze_button)
        self.analysis_text = QTextEdit()
        self.analysis_text.setReadOnly(True)
        self.analysis_text.setPlaceholderText("分析結果が表示されます...")
        parent_child_layout.addWidget(self.analysis_text)
        tab1_layout.addWidget(parent_child_group)

        # 親子関係モード設定
        parent_child_mode_group = QGroupBox("親子関係モード")
        parent_child_mode_layout = QVBoxLayout(parent_child_mode_group)

        self.parent_child_mode_check = QCheckBox("親子関係モードを有効にする")
        parent_child_mode_layout.addWidget(self.parent_child_mode_check)

        target_type_layout = QHBoxLayout()
        target_type_layout.addWidget(QLabel("対象:"))
        self.target_all_radio = QRadioButton("すべて")
        self.target_parent_radio = QRadioButton("親のみ")
        self.target_child_radio = QRadioButton("子のみ")
        self.target_all_radio.setChecked(True)

        self.target_all_radio.setEnabled(False)
        self.target_parent_radio.setEnabled(False)
        self.target_child_radio.setEnabled(False)

        target_type_layout.addWidget(self.target_all_radio)
        target_type_layout.addWidget(self.target_parent_radio)
        target_type_layout.addWidget(self.target_child_radio)
        parent_child_mode_layout.addLayout(target_type_layout)

        tab1_layout.addWidget(parent_child_mode_group)

        # ⭐ パフォーマンス監視機能を追加
        self.perf_info_label = QLabel("検索範囲: 未選択")
        self.perf_info_label.setStyleSheet("color: #666; font-size: 11px;")
        tab1_layout.addWidget(self.perf_info_label)

        tab1_layout.addStretch()

        # ========== タブ2: ファイル参照置換 ==========
        tab2 = QWidget()
        tab2_layout = QVBoxLayout(tab2)

        replace_file_group = QGroupBox("ファイル参照置換")
        replace_file_layout = QGridLayout(replace_file_group)

        replace_file_layout.addWidget(QLabel("置換対象列:"), 0, 0)
        self.target_column_combo = QComboBox()
        replace_file_layout.addWidget(self.target_column_combo, 0, 1, 1, 2)

        replace_file_layout.addWidget(QLabel("参照ファイル:"), 1, 0)
        self.lookup_filepath_entry = QLineEdit()
        self.lookup_filepath_entry.setReadOnly(True)
        replace_file_layout.addWidget(self.lookup_filepath_entry, 1, 1)
        self.browse_lookup_file_button = QPushButton("参照...")
        replace_file_layout.addWidget(self.browse_lookup_file_button, 1, 2)
        
        replace_file_layout.addWidget(QLabel("参照キー列:"), 2, 0)
        self.lookup_key_column_combo = QComboBox()
        replace_file_layout.addWidget(self.lookup_key_column_combo, 2, 1, 1, 2)

        replace_file_layout.addWidget(QLabel("置換値列:"), 3, 0)
        self.replace_value_column_combo = QComboBox()
        replace_file_layout.addWidget(self.replace_value_column_combo, 3, 1, 1, 2)

        tab2_layout.addWidget(replace_file_group)

        self.replace_from_file_button = QPushButton("ファイルから置換実行")
        self.replace_from_file_button.setMinimumHeight(40)
        self.replace_from_file_button.setStyleSheet("font-weight: bold;")
        tab2_layout.addWidget(self.replace_from_file_button)
        tab2_layout.addStretch()

        # ========== タブ3: 商品別割引適用 ==========
        tab3 = QWidget()
        tab3_layout = QVBoxLayout(tab3)
        
        # 現在ファイル設定
        current_file_group = QGroupBox("現在ファイルの設定")
        current_layout = QGridLayout(current_file_group)
        
        current_layout.addWidget(QLabel("商品番号列:"), 0, 0)
        self.current_product_col_combo = QComboBox()
        self.current_product_col_combo.addItems(self.headers)
        current_layout.addWidget(self.current_product_col_combo, 0, 1)
        
        current_layout.addWidget(QLabel("金額列:"), 1, 0)
        self.current_price_col_combo = QComboBox()
        self.current_price_col_combo.addItems(self.headers)
        current_layout.addWidget(self.current_price_col_combo, 1, 1)
        
        tab3_layout.addWidget(current_file_group)
        
        # 参照ファイル設定
        discount_ref_group = QGroupBox("参照ファイルの設定")
        discount_ref_layout = QGridLayout(discount_ref_group)
        
        discount_ref_layout.addWidget(QLabel("参照ファイル:"), 0, 0)
        self.discount_filepath_entry = QLineEdit()
        self.discount_filepath_entry.setReadOnly(True)
        discount_ref_layout.addWidget(self.discount_filepath_entry, 0, 1)
        self.browse_discount_file_button = QPushButton("参照...")
        discount_ref_layout.addWidget(self.browse_discount_file_button, 0, 2)
        
        discount_ref_layout.addWidget(QLabel("商品番号列:"), 1, 0)
        self.ref_product_col_combo = QComboBox()
        discount_ref_layout.addWidget(self.ref_product_col_combo, 1, 1, 1, 2)
        
        discount_ref_layout.addWidget(QLabel("割引率列:"), 2, 0)
        self.ref_discount_col_combo = QComboBox()
        discount_ref_layout.addWidget(self.ref_discount_col_combo, 2, 1, 1, 2)
        
        tab3_layout.addWidget(discount_ref_group)
        
        # 計算オプション
        calc_options_group = QGroupBox("計算オプション")
        calc_options_layout = QVBoxLayout(calc_options_group)
        
        round_layout = QHBoxLayout()
        round_layout.addWidget(QLabel("丸め方式:"))
        self.round_truncate_radio = QRadioButton("切り捨て")
        self.round_round_radio = QRadioButton("四捨五入")
        self.round_ceil_radio = QRadioButton("切り上げ")
        self.round_truncate_radio.setChecked(True)
        round_layout.addWidget(self.round_truncate_radio)
        round_layout.addWidget(self.round_round_radio)
        round_layout.addWidget(self.round_ceil_radio)
        calc_options_layout.addLayout(round_layout)
        
        self.preview_check = QCheckBox("処理前にプレビュー表示")
        calc_options_layout.addWidget(self.preview_check)
        
        tab3_layout.addWidget(calc_options_group)
        
        # 実行ボタン
        self.product_discount_execute_button = QPushButton("商品別割引適用実行")
        self.product_discount_execute_button.setMinimumHeight(40)
        self.product_discount_execute_button.setStyleSheet("font-weight: bold;")
        tab3_layout.addWidget(self.product_discount_execute_button)
        
        # 使い方説明
        help_text = QLabel(
            "【使い方】\n"
            "1. 現在ファイルの商品番号列と金額列を選択\n"
            "2. 参照CSVファイルを選択（商品番号と割引率が含まれる）\n"
            "3. 参照ファイルの商品番号列と割引率列を選択\n"
            "4. 計算オプション（丸め方式）を設定\n"
            "5. 実行ボタンをクリックして一括適用"
        )
        help_text.setWordWrap(True)
        help_text.setStyleSheet("QLabel { color: #666; padding: 10px; }")
        tab3_layout.addWidget(help_text)
        
        tab3_layout.addStretch()
        
        # ========== タブ4: 商品リスト一括抽出/除外 ==========
        tab4 = QWidget()
        tab4_layout = QVBoxLayout(tab4)
        
        # 説明ラベル（更新）
        description_label = QLabel(
            "商品番号リストを入力して、該当する商品の抽出または除外ができます。\n"
            "エクセルからのコピー＆ペーストにも対応しています。"
        )
        description_label.setWordWrap(True)
        description_label.setStyleSheet("QLabel { color: #666; padding: 10px; }")
        tab4_layout.addWidget(description_label)
        
        # 🔥 新規追加：モード選択グループ
        mode_group = QGroupBox("処理モード")
        mode_layout = QHBoxLayout(mode_group)
        
        self.extract_mode_radio = QRadioButton("抽出モード（リストに含まれる商品のみ）")
        self.exclude_mode_radio = QRadioButton("除外モード（リストに含まれない商品のみ）")
        self.extract_mode_radio.setChecked(True)  # デフォルトは抽出モード
        
        mode_layout.addWidget(self.extract_mode_radio)
        mode_layout.addWidget(self.exclude_mode_radio)
        tab4_layout.addWidget(mode_group)
        
        # 対象列設定グループ（既存）
        target_group = QGroupBox("検索対象設定")
        target_layout = QGridLayout(target_group)
        
        target_layout.addWidget(QLabel("対象列:"), 0, 0)
        self.bulk_extract_column_combo = QComboBox()
        self.bulk_extract_column_combo.addItems(self.headers)
        target_layout.addWidget(self.bulk_extract_column_combo, 0, 1)
        
        # 検索オプション（既存）
        self.bulk_case_sensitive_check = QCheckBox("大文字・小文字を区別する")
        self.bulk_case_sensitive_check.setChecked(True)
        target_layout.addWidget(self.bulk_case_sensitive_check, 1, 0, 1, 2)
        
        self.bulk_exact_match_check = QCheckBox("完全一致のみ（部分一致を除外）")
        self.bulk_exact_match_check.setChecked(True)
        target_layout.addWidget(self.bulk_exact_match_check, 2, 0, 1, 2)
        
        self.bulk_trim_whitespace_check = QCheckBox("前後の空白を自動削除")
        self.bulk_trim_whitespace_check.setChecked(True)
        target_layout.addWidget(self.bulk_trim_whitespace_check, 3, 0, 1, 2)
        
        tab4_layout.addWidget(target_group)
        
        # 商品番号リスト入力エリア（既存）
        list_group = QGroupBox("商品番号リスト")
        list_layout = QVBoxLayout(list_group)
        
        # 入力ヒントと件数表示
        hint_layout = QHBoxLayout()
        hint_label = QLabel("商品番号を1行に1つずつ入力:")
        self.bulk_count_label = QLabel("0件")
        self.bulk_count_label.setStyleSheet("font-weight: bold; color: #2E86C1;")
        hint_layout.addWidget(hint_label)
        hint_layout.addStretch()
        hint_layout.addWidget(self.bulk_count_label)
        list_layout.addLayout(hint_layout)
        
        # 大きなテキストエリア
        self.bulk_product_list_text = QPlainTextEdit()
        self.bulk_product_list_text.setPlaceholderText(
            "商品番号を入力してください（例）:\n"
            "AA-AAA\n"
            "BB-BBB\n"
            "CC-CCC\n"
            "DD-DDD\n\n"
            "エクセルからコピー＆ペーストも可能です"
        )
        self.bulk_product_list_text.setMinimumHeight(200)
        
        # 等幅フォントで見やすく
        font = QFont("Consolas, Monaco, 'Courier New', monospace")
        font.setPointSize(10)
        self.bulk_product_list_text.setFont(font)
        
        list_layout.addWidget(self.bulk_product_list_text)
        
        # 便利機能ボタン（既存）
        button_layout = QHBoxLayout()
        self.bulk_clear_button = QPushButton("クリア")
        self.bulk_paste_button = QPushButton("クリップボードから貼り付け")
        self.bulk_validate_button = QPushButton("リストを検証")
        
        button_layout.addWidget(self.bulk_clear_button)
        button_layout.addWidget(self.bulk_paste_button)
        button_layout.addStretch()
        button_layout.addWidget(self.bulk_validate_button)
        list_layout.addLayout(button_layout)
        
        tab4_layout.addWidget(list_group)
        
        # 🔥 実行ボタンのテキストを動的に変更
        self.bulk_extract_button = QPushButton("商品リスト一括抽出実行")
        self.bulk_extract_button.setMinimumHeight(40)
        self.bulk_extract_button.setStyleSheet("""
            QPushButton {
                font-weight: bold;
                background-color: #27AE60;
                color: white;
                border-radius: 5px;
            }
            QPushButton:hover { background-color: #2ECC71; }
            QPushButton:pressed { background-color: #229954; }
        """)
        tab4_layout.addWidget(self.bulk_extract_button)
        
        # 結果表示エリア（既存）
        result_group = QGroupBox("処理結果")
        result_layout = QVBoxLayout(result_group)
        
        self.bulk_result_label = QLabel("商品リストを入力して実行してください")
        self.bulk_result_label.setWordWrap(True)
        result_layout.addWidget(self.bulk_result_label)
        
        tab4_layout.addWidget(result_group)
        tab4_layout.addStretch()
        
        # タブ名を更新
        self.tab_widget.addTab(tab1, "検索・置換・抽出")
        self.tab_widget.addTab(tab2, "ファイル参照置換")
        self.tab_widget.addTab(tab3, "商品別割引適用")
        self.tab_widget.addTab(tab4, "商品リスト一括抽出/除外")

    def _connect_signals(self):
        # ⭐ 検索ボタンクリック時に履歴保存を追加
        self.find_next_button.clicked.connect(self._on_search_with_history)
        self.find_prev_button.clicked.connect(self._on_search_with_history)
        
        self.replace_one_button.clicked.connect(lambda: self.replace_one_clicked.emit(self.get_settings()))
        self.replace_all_button.clicked.connect(lambda: self.replace_all_clicked.emit(self.get_settings()))
        self.extract_button.clicked.connect(lambda: self.extract_clicked.emit(self.get_settings()))
        self.analyze_button.clicked.connect(lambda: self.analysis_requested.emit(self.get_settings()))
        
        self.browse_lookup_file_button.clicked.connect(self._browse_lookup_file)
        self.replace_from_file_button.clicked.connect(lambda: self.replace_from_file_requested.emit(self.get_settings()))

        self.browse_discount_file_button.clicked.connect(self._browse_discount_file)
        self.product_discount_execute_button.clicked.connect(self._execute_product_discount)

        self.parent_child_mode_check.toggled.connect(self._on_parent_child_mode_toggled)
        
        # ⭐ 履歴クリアボタンの接続
        self.clear_history_button.clicked.connect(self._clear_history)

        # 商品リスト一括抽出関連
        self.bulk_extract_button.clicked.connect(self._execute_bulk_extract)
        self.bulk_clear_button.clicked.connect(self._clear_bulk_list)
        self.bulk_paste_button.clicked.connect(self._paste_from_clipboard)
        self.bulk_validate_button.clicked.connect(self._validate_bulk_list)
        self.bulk_product_list_text.textChanged.connect(self._update_bulk_count)

        # ⭐ QListWidgetのシグナル接続
        self.column_list_widget.itemSelectionChanged.connect(self._update_selection_status)
        self.select_all_btn.clicked.connect(self.column_list_widget.selectAll)
        self.select_none_btn.clicked.connect(self.column_list_widget.clearSelection)
        self.select_category_btn.clicked.connect(self._select_category_columns)
        self.select_price_btn.clicked.connect(self._select_price_columns)
        # ⭐ 関連列選択ボタンの接続は_suggest_related_columnsで行う

        # 🔥 モード切り替え時の処理（安全性チェック付き）
        if hasattr(self, 'extract_mode_radio') and hasattr(self, 'exclude_mode_radio') and hasattr(self, 'bulk_extract_button'): #
            self.extract_mode_radio.toggled.connect(self._update_bulk_button_text)
            self.exclude_mode_radio.toggled.connect(self._update_bulk_button_text)

    def _on_parent_child_mode_toggled(self, checked):
        """親子関係モードのチェックボックスの状態に応じてラジオボタンを有効/無効にする"""
        self.target_all_radio.setEnabled(checked)
        self.target_parent_radio.setEnabled(checked)
        self.target_child_radio.setEnabled(checked)

    def update_headers(self, headers):
        """モデルのヘッダーが変更されたときにコンボボックスを更新する"""
        self.headers = headers
        
        # 🔥 安全性チェック: 必要なウィジェットが存在するかチェック
        if not hasattr(self, 'column_list_widget'):
            print("WARNING: column_list_widget が初期化されていません")
            return
            
        self.column_list_widget.clear()
        # アイテムを追加
        for header in self.headers:
            self.column_list_widget.addItem(header)
        
        # デフォルトで最初の列を選択し、スクロール
        if self.column_list_widget.count() > 0:
            first_item = self.column_list_widget.item(0)
            if first_item: # itemが存在するかチェック
                first_item.setSelected(True)
                self._scroll_to_selected_item(first_item)
            
        self._update_other_combo_boxes() 
        # 🔥 安全な選択状況更新
        try:
            self._update_selection_status()
        except Exception as e:
            print(f"WARNING: 選択状況更新エラー: {e}")
        
    def _update_other_combo_boxes(self):
        """ヘッダー変更時に他のコンボボックスを更新するヘルパーメソッド"""
        # 親子関係キー列
        self.parent_child_key_column_combo.clear()
        self.parent_child_key_column_combo.addItem("選択してください")
        self.parent_child_key_column_combo.addItems(self.headers)

        # 置換対象列
        self.target_column_combo.clear()
        self.target_column_combo.addItems(self.headers)

        # 商品別割引適用 - 現在ファイルの商品番号列
        self.current_product_col_combo.clear()
        self.current_product_col_combo.addItems(self.headers)

        # 商品別割引適用 - 現在ファイルの金額列
        self.current_price_col_combo.clear()
        self.current_price_col_combo.addItems(self.headers)

        # 商品リスト一括抽出 - 対象列
        self.bulk_extract_column_combo.clear()
        self.bulk_extract_column_combo.addItems(self.headers)

    def get_settings(self):
        """現在のUI設定を辞書として返す"""
        selected_items = self.column_list_widget.selectedItems()
        
        if not selected_items:
            # 何も選択されていない場合は全列を対象
            target_columns = self.headers
        else:
            # 選択された列のみを対象
            target_columns = [item.text() for item in selected_items]
            
        settings = {
            "search_term": self.search_entry.currentText(),
            "target_columns": target_columns, 
            "is_case_sensitive": self.case_sensitive_check.isChecked(),
            "is_regex": self.regex_check.isChecked(),
            "in_selection_only": self.in_selection_check.isChecked(),
            "replace_term": self.replace_entry.text(),
            "key_column": self.parent_child_key_column_combo.currentText() if self.parent_child_key_column_combo.currentText() != "選択してください" else "",
            "analysis_mode": "consecutive" if self.consecutive_radio.isChecked() else "global",
            "is_parent_child_mode": self.parent_child_mode_check.isChecked(),
            "target_type": ("all" if self.target_all_radio.isChecked() else
                            "parent" if self.target_parent_radio.isChecked() else "child"),

            "target_col": self.target_column_combo.currentText(),
            "lookup_filepath": self.lookup_filepath_entry.text(),
            "lookup_file_encoding": self.detected_encodings.get(
                self.lookup_filepath_entry.text(), 'utf-8'
            ),
            "replace_val_col": self.replace_value_column_combo.currentText(),
            "lookup_key_col": self.lookup_key_column_combo.currentText(), 

            'current_product_col': self.current_product_col_combo.currentText(),
            'current_price_col': self.current_price_col_combo.currentText(),
            'discount_filepath': self.discount_filepath_entry.text(),
            'ref_product_col': self.ref_product_col_combo.currentText(),
            'ref_discount_col': self.ref_discount_col_combo.currentText(),
            'round_mode': ('truncate' if self.round_truncate_radio.isChecked() else
                           'round' if self.round_round_radio.isChecked() else 'ceil'),
            'preview': self.preview_check.isChecked(),

            "bulk_extract_column": self.bulk_extract_column_combo.currentText(),
            "product_list": self._parse_product_list(),
            "case_sensitive": self.bulk_case_sensitive_check.isChecked(),
            "exact_match": self.bulk_exact_match_check.isChecked(),
            "trim_whitespace": self.bulk_trim_whitespace_check.isChecked(),
            
            # 🔥 新規追加：モード設定
            "bulk_mode": "extract" if self.extract_mode_radio.isChecked() else "exclude",
        }
        
        settings['discount_file_encoding'] = self.detected_encodings.get(
            self.discount_filepath_entry.text(), 'shift_jis'
        )
        
        return settings

    def _browse_lookup_file(self):
        """参照ファイル選択ダイアログを表示し、選択されたファイルのヘッダーを読み込む"""
        filepath, _ = QFileDialog.getOpenFileName(self, "参照ファイルを選択", "", "CSVファイル (*.csv);;テキストファイル (*.txt);;すべてのファイル (*.*)")
        if filepath:
            self.lookup_filepath_entry.setText(filepath)
            self._load_reference_file_headers(filepath, 'lookup')
            QMessageBox.information(self, "参照ファイル", f"参照ファイルを設定しました:\n{os.path.basename(filepath)}")

    def _browse_discount_file(self):
        """商品別割引適用用の参照ファイル選択ダイアログを表示し、選択されたファイルのヘッダーを読み込む"""
        filepath, _ = QFileDialog.getOpenFileName(self, "割引率参照ファイルを選択", "", "CSVファイル (*.csv);;テキストファイル (*.txt);;すべてのファイル (*.*)")
        if filepath:
            self.discount_filepath_entry.setText(filepath)
            self._load_reference_file_headers(filepath, 'discount')
            QMessageBox.information(self, "参照ファイル", f"割引率参照ファイルを設定しました:\n{os.path.basename(filepath)}")

    def _load_reference_file_headers(self, filepath, context):
        """参照ファイルのヘッダーを読み込み、対応するコンボボックスを更新する"""
        try:
            encoding = 'utf-8'
            try_encodings = ['utf-8', 'shift_jis', 'cp932', 'utf-8-sig', 'euc-jp']
            for enc in try_encodings:
                try:
                    with open(filepath, 'r', encoding=enc) as f:
                        f.readline()
                    encoding = enc
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    print(f"Error checking encoding {enc}: {e}")
                    continue
            
            self.detected_encodings[filepath] = encoding
            
            temp_df = pd.read_csv(filepath, encoding=encoding, nrows=0, dtype=str, keep_default_na=False)
            headers = list(temp_df.columns)

            if context == 'lookup':
                self.lookup_key_column_combo.clear()
                self.lookup_key_column_combo.addItems(headers)
                self.replace_value_column_combo.clear()
                self.replace_value_column_combo.addItems(headers)
            elif context == 'discount':
                self.ref_product_col_combo.clear()
                self.ref_product_col_combo.addItems(headers)
                self.ref_discount_col_combo.clear()
                self.ref_discount_col_combo.addItems(headers)

        except Exception as e:
            QMessageBox.critical(self, "ファイル読み込みエラー", f"参照ファイルのヘッダー読み込み中にエラーが発生しました。\n{e}")
            if context == 'lookup':
                self.lookup_key_column_combo.clear()
                self.replace_value_column_combo.clear()
            elif context == 'discount':
                self.ref_product_col_combo.clear()
                self.ref_discount_col_combo.clear()


    def _execute_product_discount(self):
        """商品別割引適用を実行するためのシグナルを発行"""
        settings = self.get_settings()

        if not settings['current_product_col'] or settings['current_product_col'] not in self.headers:
            QMessageBox.warning(self, "入力エラー", "現在ファイルの商品番号列が選択されていないか、存在しません。")
            return
        if not settings['current_price_col'] or settings['current_price_col'] not in self.headers:
            QMessageBox.warning(self, "入力エラー", "現在ファイルの金額列が選択されていないか、存在しません。")
            return
        if not settings['discount_filepath']:
            QMessageBox.warning(self, "入力エラー", "割引率参照ファイルが選択されていません。")
            return
        if not settings['ref_product_col'] or not self.ref_product_col_combo.currentText():
            QMessageBox.warning(self, "入力エラー", "参照ファイルの商品番号列が選択されていません。")
            return
        if not settings['ref_discount_col'] or not self.ref_discount_col_combo.currentText():
            QMessageBox.warning(self, "入力エラー", "参照ファイルの割引率列が選択されていません。")
            return

        self.product_discount_requested.emit(settings)

    def _setup_search_history(self):
        """検索履歴の自動補完を設定"""
        if not self.settings_manager:
            print("設定マネージャーがありません")
            return
            
        history = self.settings_manager.get_search_history()
        print(f"読み込んだ履歴: {history}")
        
        current_text = self.search_entry.currentText()
        
        self.search_entry.clear()
        self.search_entry.addItems(history)
        
        self.search_entry.setCurrentText(current_text)
        
        completer = QCompleter(history)
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        completer.setMaxVisibleItems(10)
        completer.setCompletionMode(QCompleter.PopupCompletion)
        completer.setFilterMode(Qt.MatchContains)
        
        self.search_entry.setCompleter(completer)
        
        if history:
            self.search_entry.setPlaceholderText(f"検索語を入力 (履歴: {len(history)}件)")
        else:
            self.search_entry.setPlaceholderText("検索語を入力")

    def _on_search_with_history(self):
        """検索実行時に履歴を保存し、実際の検索処理を呼び出す"""
        search_term = self.search_entry.currentText()
        
        if self.settings_manager and search_term:
            self.settings_manager.save_search_history(search_term)
            
            history = self.settings_manager.get_search_history()
            
            current_items = [self.search_entry.itemText(i) for i in range(self.search_entry.count())]
            if current_items != history:
                self.search_entry.blockSignals(True) 
                self.search_entry.clear()
                self.search_entry.addItems(history)
                self.search_entry.setCurrentText(search_term) 
                self.search_entry.blockSignals(False) 
                
                completer = QCompleter(history)
                completer.setCaseSensitivity(Qt.CaseInsensitive)
                completer.setMaxVisibleItems(10)
                completer.setCompletionMode(QCompleter.PopupCompletion)
                completer.setFilterMode(Qt.MatchContains)
                self.search_entry.setCompleter(completer)
        
        if self.sender() == self.find_next_button:
            self.find_next_clicked.emit(self.get_settings())
        elif self.sender() == self.find_prev_button:
            self.find_prev_clicked.emit(self.get_settings())
            
    def _clear_history(self):
        """検索履歴をクリア"""
        if self.settings_manager:
            reply = QMessageBox.question(
                self, "確認", 
                "検索履歴をすべて削除しますか？",
                QMessageBox.Yes | QMessageBox.No
            )
            if reply == QMessageBox.Yes:
                self.settings_manager.clear_search_history()
                self._setup_search_history()
                self.parent().show_operation_status("検索履歴をクリアしました", 2000)

    def _parse_product_list(self):
        """QPlainTextEditから商品番号リストを解析して返す"""
        text = self.bulk_product_list_text.toPlainText()
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return lines

    def _clear_bulk_list(self):
        """商品番号リストのテキストエリアをクリア"""
        self.bulk_product_list_text.clear()
        self.bulk_result_label.setText("商品リストを入力して実行してください")
        self._update_bulk_count()

    def _paste_from_clipboard(self):
        """クリップボードの内容を商品番号リストのテキストエリアに貼り付け"""
        clipboard = QApplication.clipboard()
        self.bulk_product_list_text.setPlainText(clipboard.text())
        self._update_bulk_count()

    def _validate_bulk_list(self):
        """リストの検証（重複、空文字、統計情報の表示）"""
        product_list = self._parse_product_list()
        
        if not product_list:
            self.bulk_result_label.setText("商品番号が入力されていません。")
            return
            
        total_count = len(product_list)
        unique_count = len(set(product_list))
        duplicate_count = total_count - unique_count
        
        lengths = [len(item) for item in product_list]
        min_length = min(lengths) if lengths else 0
        max_length = max(lengths) if lengths else 0
        
        empty_count = sum(1 for item in product_list if not item.strip())
        
        # 🔥 モードに応じた説明を追加
        mode_text = "抽出モード" if self.extract_mode_radio.isChecked() else "除外モード"
        
        result_text = f"""リスト検証結果:
- 総件数: {total_count}件
- 重複除去後: {unique_count}件
- 重複: {duplicate_count}件
- 文字数: {min_length}〜{max_length}文字
- 空文字/空白のみ: {empty_count}件
- 現在のモード: {mode_text}"""
        
        if duplicate_count > 0:
            result_text += "\n\n⚠️ 重複があります。実行時に自動除去されます。"
        
        if empty_count > 0:
            result_text += "\n\n⚠️ 空の行があります。これらは除外されます。"
        
        if self.exclude_mode_radio.isChecked():
            result_text += "\n\n📌 除外モード: リストの商品以外が抽出されます。"
            
        self.bulk_result_label.setText(result_text)

    def _update_bulk_count(self):
        """入力件数のリアルタイム更新"""
        product_list = self._parse_product_list()
        count = len([item for item in product_list if item.strip()])
        self.bulk_count_label.setText(f"{count}件")

    def _execute_bulk_extract(self):
        """商品リスト一括抽出の実行（入力検証付き）"""
        settings = self.get_settings()
        
        if not settings['product_list']:
            QMessageBox.warning(self, "入力エラー", "商品番号リストが空です。")
            return
        
        if not settings['bulk_extract_column'] or settings['bulk_extract_column'] not in self.headers:
            QMessageBox.warning(self, "入力エラー", "対象列が選択されていないか、存在しません。")
            return
        
        original_count = len(settings['product_list'])
        unique_list = list(set(settings['product_list']))
        if len(unique_list) != original_count:
            reply = QMessageBox.question(
                self, "重複確認",
                f"リストに重複があります（{original_count}件 → {len(unique_list)}件）。\n"
                "重複を除去して続行しますか？",
                QMessageBox.Yes | QMessageBox.No
            )
            if reply == QMessageBox.No:
                return
            settings['product_list'] = unique_list
        
        if len(unique_list) > 10000:
            reply = QMessageBox.question(
                self, "大量データ確認",
                f"商品リストが非常に大きいです（{len(unique_list)}件）。\n"
                "処理に時間がかかる可能性がありますが続行しますか？",
                QMessageBox.Yes | QMessageBox.No
            )
            if reply == QMessageBox.No:
                return
        
        self.bulk_extract_requested.emit(settings)

    def set_target_column(self, column_name):
        """対象列を指定して設定し、自動スクロール"""
        if not column_name:
            return False
            
        # 🔥 安全性チェック
        if not hasattr(self, 'column_list_widget'):
            print("WARNING: column_list_widget が初期化されていません")
            return False
            
        # 既存の選択をクリア
        self.column_list_widget.clearSelection()
        
        # 指定された列を検索して選択
        for i in range(self.column_list_widget.count()):
            item = self.column_list_widget.item(i)
            if item and item.text() == column_name: 
                item.setSelected(True)
                
                # 🔥 重要：自動スクロール機能を追加
                self._scroll_to_selected_item(item)
                
                # 安全な選択状況更新
                try:
                    self._update_selection_status()
                except Exception as e:
                    print(f"WARNING: 選択状況更新エラー: {e}")
                
                print(f"DEBUG: 対象列を「{column_name}」に設定し、スクロールしました")
                return True
            
        print(f"DEBUG: 列「{column_name}」が見つかりませんでした")
        return False

    def reset_to_default_column(self):
        """デフォルト状態に戻し、自動スクロール"""
        if not hasattr(self, 'column_list_widget'): 
            return

        self.column_list_widget.clearSelection()
        
        if self.column_list_widget.count() > 0:
            first_item = self.column_list_widget.item(0)
            if first_item: 
                first_item.setSelected(True)
                self._scroll_to_selected_item(first_item)
            
        try: 
            self._update_selection_status()
        except Exception as e:
            print(f"WARNING: 選択状況更新エラー: {e}")
            
        print("DEBUG: 対象列をデフォルトにリセットし、スクロールしました")

    def _select_columns_by_keywords(self, keywords):
        """キーワードに基づく自動選択"""
        if not hasattr(self, 'column_list_widget'): 
            return

        self.column_list_widget.clearSelection()
        for i in range(self.column_list_widget.count()):
            item = self.column_list_widget.item(i)
            if item: 
                column_name = item.text().lower()
                if any(keyword in column_name for keyword in keywords):
                    item.setSelected(True)
        self._update_selection_status()

    def _select_category_columns(self):
        """カテゴリ関連列を自動選択"""
        category_keywords = ['カテゴリ', 'category', 'ジャンル', 'genre', '分類']
        self._select_columns_by_keywords(category_keywords)

    def _select_price_columns(self):
        """価格関連列を自動選択"""
        price_keywords = ['価格', 'price', '値段', '金額', 'amount', '料金']
        self._select_columns_by_keywords(price_keywords)
    
    def _update_selection_status(self):
        """選択状況の表示更新（視認性改善版）"""
        if not hasattr(self, 'column_list_widget') or not hasattr(self, 'selection_status_label'): 
            return

        selected_items = self.column_list_widget.selectedItems()
        selected_count = len(selected_items)
        
        if selected_count == 0:
            self.selection_status_label.setText("選択中: 0列")
            self.selection_status_label.setStyleSheet("""
                color: #7F8C8D;
                font-size: 12px;
                padding: 4px 8px;
                background-color: #F8F9FA;
                border-radius: 3px;
            """)
        elif selected_count == len(self.headers):
            self.selection_status_label.setText(f"選択中: 全{selected_count}列")
            self.selection_status_label.setStyleSheet("""
                color: white;
                font-size: 12px;
                font-weight: bold;
                padding: 4px 8px;
                background-color: #E74C3C;
                border-radius: 3px;
            """)
        else:
            self.selection_status_label.setText(f"選択中: {selected_count}列")
            self.selection_status_label.setStyleSheet("""
                color: white;
                font-size: 12px;
                font-weight: bold;
                padding: 4px 8px;
                background-color: #27AE60;
                border-radius: 3px;
            """)
            
        self._update_performance_info(selected_count)

        if selected_count == 1 and selected_items: 
            base_column_name = selected_items[0].text()
            self._suggest_related_columns(base_column_name)
        else:
            self.select_related_btn.setVisible(False)
            for i in range(self.column_list_widget.count()):
                item = self.column_list_widget.item(i)
                if item:
                    item.setBackground(QColor())

    def get_selected_columns(self):
        """現在選択されている列名のリストを返すヘルパーメソッド"""
        if not hasattr(self, 'column_list_widget'): 
            return []
        return [item.text() for item in self.column_list_widget.selectedItems() if item] 

    def _update_performance_info(self, selected_count=0):
        """パフォーマンス情報の表示（修正版）"""
        if not hasattr(self, 'perf_info_label') or not hasattr(self, 'headers'):
            return
            
        total_columns = len(self.headers)
        
        if total_columns == 0:
            self.perf_info_label.setText("検索範囲: データなし")
            self.perf_info_label.setStyleSheet("color: #666; font-size: 11px;")
            return
            
        if selected_count == 0:
            self.perf_info_label.setText("検索範囲: 未選択")
            self.perf_info_label.setStyleSheet("color: #666; font-size: 11px;")
            return
            
        column_ratio = selected_count / total_columns
        estimated_memory = f"{column_ratio * 100:.1f}%"
        
        if selected_count == total_columns:
            status = "⚠️ 全列検索 - 高負荷"
            color = "#E74C3C"
        elif selected_count > 10:
            status = "⚠️ 多列検索 - 中負荷"
            color = "#F39C12"
        else:
            status = "✅ 効率的な検索"
            color = "#27AE60"
            
        self.perf_info_label.setText(
            f"検索範囲: {selected_count}/{total_columns}列 "
            f"({estimated_memory}のメモリ使用) - {status}"
        )
        self.perf_info_label.setStyleSheet(f"color: {color}; font-size: 11px;")

    def _scroll_to_selected_item(self, item=None):
        """
        指定されたアイテム、または選択中の最初のアイテムにスクロール
        QAbstractItemView.PositionAtCenter を使用して中央に表示
        """
        if not hasattr(self, 'column_list_widget'): 
            return

        if item is None:
            selected_items = self.column_list_widget.selectedItems()
            if selected_items:
                item = selected_items[0]
            elif self.column_list_widget.count() > 0:
                item = self.column_list_widget.item(0)
        
        if item:
            try:
                from PySide6.QtWidgets import QAbstractItemView
                self.column_list_widget.scrollToItem(item, QAbstractItemView.PositionAtCenter)
                print(f"DEBUG: '{item.text()}'列にスクロールしました")
            except Exception as e:
                print(f"WARNING: スクロールエラー: {e}")

    def _find_related_columns(self, base_column):
        """関連列の自動検出（簡略版）"""
        if not base_column:
            return []
        
        related_columns = []
        base_name = base_column.lower()
        
        number_pattern = r'(\d+)$'
        match = re.search(number_pattern, base_name)
        
        if match:
            base_prefix = base_name[:match.start()]
            current_num = int(match.group(1))
            
            for i in range(max(1, current_num - 2), current_num + 3):
                if i != current_num:
                    candidate = f"{base_prefix}{i}"
                    for header in self.headers:
                        if header.lower() == candidate:
                            related_columns.append(header)
        
        keywords = {
            'カテゴリ': ['category', 'ジャンル', 'genre'],
            '価格': ['price', '値段', '金額'],
            '商品': ['product', 'item']
        }
        
        for keyword, alternatives in keywords.items():
            if keyword in base_name:
                for alt in alternatives:
                    for header in self.headers:
                        if alt in header.lower() and header != base_column:
                            related_columns.append(header)
        
        return list(set(related_columns))[:5]

    def _suggest_related_columns(self, base_column):
        """関連列の提案表示"""
        if not hasattr(self, 'column_list_widget') or not hasattr(self, 'select_related_btn'): 
            return

        related_columns = self._find_related_columns(base_column)
        
        for i in range(self.column_list_widget.count()):
            item = self.column_list_widget.item(i)
            if item:
                item.setBackground(QColor()) 

        if related_columns:
            for i in range(self.column_list_widget.count()):
                item = self.column_list_widget.item(i)
                if item and item.text() in related_columns:
                    item.setBackground(QColor("#E8F4FD")) 
            
            self.select_related_btn.setText(f"関連列を選択 ({len(related_columns)}件)")
            self.select_related_btn.setVisible(True)
            
            try:
                self.select_related_btn.clicked.disconnect()
            except TypeError: 
                pass
            self.select_related_btn.clicked.connect(
                lambda: self._select_related_columns(base_column, related_columns)
            )
        else:
            self.select_related_btn.setVisible(False)

    def _select_related_columns(self, base_column, related_columns):
        """関連列の一括選択"""
        if not hasattr(self, 'column_list_widget') or not hasattr(self, 'select_related_btn'): 
            return

        for i in range(self.column_list_widget.count()):
            item = self.column_list_widget.item(i)
            if item and item.text() in related_columns:
                item.setSelected(True)
                item.setBackground(QColor()) 
        
        if base_column:
            for i in range(self.column_list_widget.count()):
                item = self.column_list_widget.item(i)
                if item and item.text() == base_column:
                    item.setSelected(True)
                    self._scroll_to_selected_item(item)
                    break
        
        self._update_selection_status()
        self.select_related_btn.setVisible(False)

    def _create_enhanced_column_selection_ui(self):
        """拡張された列選択UIの作成"""
        column_selection_group = QGroupBox("検索対象列")
        column_selection_layout = QVBoxLayout(column_selection_group)
        
        self.column_list_widget = QListWidget()
        self.column_list_widget.setSelectionMode(QListWidget.MultiSelection)
        self.column_list_widget.setMaximumHeight(200)
        self.column_list_widget.setMinimumHeight(120)
        
        # 🔥 テーマカラーの取得
        theme = None
        if self.parent() and hasattr(self.parent(), 'theme'):
            theme = self.parent().theme
        
        # フォールバック用のデフォルトカラー
        primary_color = theme.PRIMARY if theme else "#2E86C1"
        primary_hover = theme.PRIMARY_HOVER if theme else "#5BA0F2"
        primary_active = theme.PRIMARY_ACTIVE if theme else "#1E5F8E"
        success_color = theme.SUCCESS if theme else "#27AE60"

        # 🔥 重要：選択項目のホバー問題を解決
        self.column_list_widget.setStyleSheet(f"""
            QListWidget::item {{
                padding: 6px 8px;
                border-bottom: 1px solid #E0E0E0;
                color: #2C3E50;
            }}
            QListWidget::item:selected {{
                background-color: {primary_color};
                color: white;
                font-weight: bold;
            }}
            QListWidget::item:hover {{
                background-color: #E8F4FD;
                color: #1F4E79;
            }}
            /* 🔥 これが重要：選択中かつホバー時の明示的な定義 */
            QListWidget::item:selected:hover {{
                background-color: {primary_active};
                color: white;
                font-weight: bold;
            }}
        """)
        
        column_selection_layout.addWidget(self.column_list_widget)
        
        self.select_related_btn = QPushButton("関連列を選択")
        self.select_related_btn.setVisible(False)
        self.select_related_btn.setMaximumWidth(120)
        self.select_related_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: #9B59B6;
                color: white;
                border: 1px solid #8E44AD;
                padding: 6px 12px;
                border-radius: 4px;
                font-weight: bold;
            }}
            QPushButton:hover {{
                background-color: #8E44AD;
            }}
            QPushButton:pressed {{
                background-color: #7D3C98;
            }}
        """)
        
        quick_select_layout = QHBoxLayout()
        self.select_all_btn = QPushButton("全選択")
        self.select_none_btn = QPushButton("選択解除")
        self.select_category_btn = QPushButton("カテゴリ系")
        self.select_price_btn = QPushButton("価格系")
        
        # 🔥 ボタンの視認性を大幅改善
        button_style = f"""
            QPushButton {{
                background-color: {primary_color};
                color: white;
                border: 1px solid {primary_color};
                border-radius: 4px;
                padding: 6px 12px;
                font-weight: bold;
                min-width: 70px;
            }}
            QPushButton:hover {{
                background-color: {primary_hover};
                border-color: {primary_hover};
            }}
            QPushButton:pressed {{
                background-color: {primary_active};
                border-color: {primary_active};
            }}
        """
        
        for btn in [self.select_all_btn, self.select_none_btn, 
                    self.select_category_btn, self.select_price_btn]:
            btn.setStyleSheet(button_style)
            btn.setMaximumWidth(80)
            
        self.select_all_btn.setStyleSheet(button_style.replace(primary_color, success_color))
        self.select_none_btn.setStyleSheet(button_style.replace(primary_color, "#E74C3C"))
        
        quick_select_layout.addWidget(self.select_all_btn)
        quick_select_layout.addWidget(self.select_none_btn)
        quick_select_layout.addWidget(self.select_category_btn)
        quick_select_layout.addWidget(self.select_price_btn)
        quick_select_layout.addWidget(self.select_related_btn) 
        column_selection_layout.addLayout(quick_select_layout)
        
        self.selection_status_label = QLabel("選択中: 0列")
        self.selection_status_label.setStyleSheet("color: #666; font-size: 11px;")
        column_selection_layout.addWidget(self.selection_status_label)
        
        return column_selection_group

    def _update_bulk_button_text(self):
        """モードに応じてボタンテキストを更新"""
        # extract_mode_radioとexclude_mode_radioが存在するかチェック
        if not hasattr(self, 'extract_mode_radio') or not hasattr(self, 'exclude_mode_radio'):
            return
            
        if not hasattr(self, 'bulk_extract_button'):
            return
            
        if self.extract_mode_radio.isChecked():
            self.bulk_extract_button.setText("商品リスト一括抽出実行")
            self.bulk_extract_button.setStyleSheet("""
                QPushButton {
                    font-weight: bold;
                    background-color: #27AE60;
                    color: white;
                    border-radius: 5px;
                }
                QPushButton:hover { background-color: #2ECC71; }
                QPushButton:pressed { background-color: #229954; }
            """)
        else:
            self.bulk_extract_button.setText("商品リスト一括除外実行")
            self.bulk_extract_button.setStyleSheet("""
                QPushButton {
                    font-weight: bold;
                    background-color: #E74C3C;
                    color: white;
                    border-radius: 5px;
                }
                QPushButton:hover { background-color: #EC7063; }
                QPushButton:pressed { background-color: #C0392B; }
            """)