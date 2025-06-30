import os
import pandas as pd
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QFormLayout,
    QLineEdit, QTextEdit, QPlainTextEdit, QComboBox, QCheckBox, QRadioButton,
    QSpinBox, QDoubleSpinBox, QPushButton,
    QLabel, QProgressBar, QTableView, QListWidget,
    QGroupBox, QScrollArea, QDockWidget, QButtonGroup,
    QFileDialog, QMessageBox, QInputDialog, QProgressDialog, QDialogButtonBox,
    QTabWidget,
    QCompleter
)
from PySide6.QtGui import QKeySequence, QGuiApplication, QTextOption, QFont, QAction
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

    def __init__(self, headers=None, parent=None):
        super().__init__(parent)
        self.headers = headers if headers is not None else []
        self.detected_encodings = {}
        
        # ⭐ 設定マネージャーを取得（改良版）
        self.settings_manager = None
        # SearchWidgetの親はmain_windowのはず
        if parent and hasattr(parent, 'settings_manager'):
            self.settings_manager = parent.settings_manager
            print(f"設定マネージャーを取得しました: {self.settings_manager}")
        else:
            print(f"警告: 親ウィジェット({parent})に設定マネージャーがありません")
        
        self._create_widgets()
        self._connect_signals()
        self.update_headers(self.headers)
        
        # ⭐ 検索履歴を設定
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
        # ⭐ 検索入力欄をQComboBoxに変更
        self.search_entry = QComboBox()
        self.search_entry.setEditable(True)
        self.search_entry.setInsertPolicy(QComboBox.NoInsert)

        search_layout.addWidget(self.search_entry, 0, 1, 1, 2)

        search_layout.addWidget(QLabel("対象列:"), 1, 0)
        self.column_combo = QComboBox()
        self.column_combo.addItem("すべての列")
        self.column_combo.addItems(self.headers)
        search_layout.addWidget(self.column_combo, 1, 1, 1, 2)

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
        
        # タブを追加
        self.tab_widget.addTab(tab1, "検索・置換・抽出")
        self.tab_widget.addTab(tab2, "ファイル参照置換")
        self.tab_widget.addTab(tab3, "商品別割引適用")

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


    def _on_parent_child_mode_toggled(self, checked):
        """親子関係モードのチェックボックスの状態に応じてラジオボタンを有効/無効にする"""
        self.target_all_radio.setEnabled(checked)
        self.target_parent_radio.setEnabled(checked)
        self.target_child_radio.setEnabled(checked)

    def update_headers(self, headers):
        """モデルのヘッダーが変更されたときにコンボボックスを更新する"""
        self.headers = headers
        
        self.column_combo.clear()
        self.column_combo.addItem("すべての列")
        self.column_combo.addItems(self.headers)
        
        self.parent_child_key_column_combo.clear()
        self.parent_child_key_column_combo.addItem("選択してください")
        self.parent_child_key_column_combo.addItems(self.headers)

        self.target_column_combo.clear()
        self.target_column_combo.addItems(self.headers)

        self.current_product_col_combo.clear()
        self.current_product_col_combo.addItems(self.headers)

        self.current_price_col_combo.clear()
        self.current_price_col_combo.addItems(self.headers)


    def get_settings(self):
        """現在のUI設定を辞書として返す"""
        settings = {
            "search_term": self.search_entry.currentText(),
            "target_columns": [self.column_combo.currentText()] if self.column_combo.currentText() != "すべての列" else self.headers,
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
            "lookup_key_col": self.lookup_key_column_combo.currentText(),
            "replace_val_col": self.replace_value_column_combo.currentText(),

            'current_product_col': self.current_product_col_combo.currentText(),
            'current_price_col': self.current_price_col_combo.currentText(),
            'discount_filepath': self.discount_filepath_entry.text(),
            'ref_product_col': self.ref_product_col_combo.currentText(),
            'ref_discount_col': self.ref_discount_col_combo.currentText(),
            'round_mode': ('truncate' if self.round_truncate_radio.isChecked() else
                           'round' if self.round_round_radio.isChecked() else 'ceil'),
            'preview': self.preview_check.isChecked()
        }
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

    # ⭐ 新規追加: 検索履歴の自動補完を設定
    def _setup_search_history(self):
        """検索履歴の自動補完を設定"""
        if not self.settings_manager:
            print("設定マネージャーがありません")
            return
            
        # 履歴を取得
        history = self.settings_manager.get_search_history()
        print(f"読み込んだ履歴: {history}")
        
        # 現在の検索語を保持
        current_text = self.search_entry.currentText()
        
        # QComboBoxのアイテムを更新
        self.search_entry.clear()
        self.search_entry.addItems(history)
        
        # 現在の検索語を復元（履歴更新後も入力を維持）
        self.search_entry.setCurrentText(current_text)
        
        # 自動補完を設定
        completer = QCompleter(history) # historyリストを直接使用
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        completer.setMaxVisibleItems(10)
        completer.setCompletionMode(QCompleter.PopupCompletion)
        completer.setFilterMode(Qt.MatchContains)
        
        # 検索入力欄に適用
        self.search_entry.setCompleter(completer)
        
        # プレースホルダーテキストを設定
        if history:
            self.search_entry.setPlaceholderText(f"検索語を入力 (履歴: {len(history)}件)")
        else:
            self.search_entry.setPlaceholderText("検索語を入力")

    # ⭐ 新規追加: 検索実行時に履歴を保存
    def _on_search_with_history(self):
        """検索実行時に履歴を保存し、実際の検索処理を呼び出す"""
        search_term = self.search_entry.currentText()
        
        # 履歴を保存
        if self.settings_manager and search_term:
            self.settings_manager.save_search_history(search_term)
            
            # 履歴リストを更新（現在の入力は保持）
            history = self.settings_manager.get_search_history()
            
            # 現在のアイテムリストと比較して、変更があった場合のみ更新
            current_items = [self.search_entry.itemText(i) for i in range(self.search_entry.count())]
            if current_items != history:
                # 現在の入力を保持しながら履歴を更新
                self.search_entry.blockSignals(True) # シグナルを一時的にブロック
                self.search_entry.clear()
                self.search_entry.addItems(history)
                self.search_entry.setCurrentText(search_term) # 検索語を維持
                self.search_entry.blockSignals(False) # シグナルブロックを解除
                
                # Completerも更新
                completer = QCompleter(history)
                completer.setCaseSensitivity(Qt.CaseInsensitive)
                completer.setMaxVisibleItems(10)
                completer.setCompletionMode(QCompleter.PopupCompletion)
                completer.setFilterMode(Qt.MatchContains)
                self.search_entry.setCompleter(completer)
        
        # 元の検索処理を実行
        if self.sender() == self.find_next_button:
            self.find_next_clicked.emit(self.get_settings())
        elif self.sender() == self.find_prev_button:
            self.find_prev_clicked.emit(self.get_settings())
            
    # ⭐ 新規追加: 検索履歴をクリア
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
                self._setup_search_history()  # 自動補完を更新
                self.parent().show_operation_status("検索履歴をクリアしました", 2000)