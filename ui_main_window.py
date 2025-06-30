# ui_main_window.py

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QMenu, QToolBar, QStatusBar, QLabel, QPushButton, QProgressBar,
    QTableView, QHeaderView, QAbstractItemView, QStyle, QDockWidget,
    QFormLayout, QTextEdit, QHBoxLayout, QScrollArea
)
from PySide6.QtGui import QAction, QKeySequence
from PySide6.QtCore import Qt, QSize

from dialogs import TooltipEventFilter

class Ui_MainWindow(object):
    """
    メインウィンドウのUI定義を専門に行うクラス。
    ロジックは含まず、ウィジェットの作成と配置のみを担当する。
    """
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.setWindowTitle("高機能CSVエディタ (PySide6)")
        MainWindow.setGeometry(100, 100, 1280, 720)
        MainWindow.setAcceptDrops(True)

        # メニューバーの作成
        self._create_menu_bar(MainWindow)

        # ツールバーの作成
        self._create_tool_bar(MainWindow)

        # 中央ウィジェットとレイアウト
        MainWindow.central_widget = QWidget()
        MainWindow.setCentralWidget(MainWindow.central_widget)
        MainWindow.main_layout = QVBoxLayout(MainWindow.central_widget)
        MainWindow.main_layout.setContentsMargins(0, 0, 0, 0)

        # ビューのスタック
        MainWindow.view_stack = QWidget()
        MainWindow.view_stack_layout = QVBoxLayout(MainWindow.view_stack)
        MainWindow.view_stack_layout.setContentsMargins(0,0,0,0)

        # テーブルビュー
        MainWindow.table_view = QTableView()
        MainWindow.table_view.setSortingEnabled(False)
        MainWindow.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        MainWindow.table_view.horizontalHeader().setStretchLastSection(True)
        MainWindow.table_view.verticalHeader().setSectionResizeMode(QHeaderView.Fixed)
        
        # 🔥 修正: 選択動作を修正
        MainWindow.table_view.setSelectionBehavior(QAbstractItemView.SelectItems)
        MainWindow.table_view.setSelectionMode(QAbstractItemView.ExtendedSelection)
        
        # 🔥 追加: 行・列ヘッダーの選択を有効化
        MainWindow.table_view.horizontalHeader().setSectionsClickable(True)
        MainWindow.table_view.verticalHeader().setSectionsClickable(True)
        
        MainWindow.table_view.setContextMenuPolicy(Qt.CustomContextMenu)
        MainWindow.table_view.setFocusPolicy(Qt.StrongFocus)
        MainWindow.view_stack_layout.addWidget(MainWindow.table_view)

        # スクロールエリアを作成
        MainWindow.card_scroll_area = QScrollArea()
        MainWindow.card_scroll_area.setWidgetResizable(True)
        MainWindow.card_scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        MainWindow.card_scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)

        # カードビューのコンテナを作成（スクロールエリアの中身）
        MainWindow.card_view_container = QWidget()
        self._create_card_view_container(MainWindow)
        
        # コンテナをスクロールエリアにセット
        MainWindow.card_scroll_area.setWidget(MainWindow.card_view_container)

        # スクロールエリアをビューのスタックに追加
        MainWindow.view_stack_layout.addWidget(MainWindow.card_scroll_area)
        MainWindow.card_scroll_area.hide()

        # 🔥 重要: view_stackをmain_layoutに追加
        MainWindow.main_layout.addWidget(MainWindow.view_stack)
        
        # 🔥 追加: 初期状態でview_stackを非表示にする（ウェルカム画面を表示するため）
        MainWindow.view_stack.hide()

        # 🔥 修正: ウェルカム画面の定義を ui_main_window.py に集約
        MainWindow.welcome_widget = QWidget()
        MainWindow.welcome_widget.setObjectName("welcome_widget") # Stylesheet用にオブジェクト名を設定
        welcome_layout = QVBoxLayout(MainWindow.welcome_widget)
        welcome_layout.setContentsMargins(50, 50, 50, 50) # マージンを追加

        # ロゴまたはタイトルラベル (MainWindowの属性にはしないが、このスコープで定義)
        welcome_title = QLabel("高機能CSVエディタ")
        welcome_title.setAlignment(Qt.AlignCenter)
        title_font = welcome_title.font()
        title_font.setPointSize(24)
        title_font.setBold(True)
        welcome_title.setFont(title_font)

        # 説明ラベルをMainWindowの属性として定義し、テキストとアラインメントを設定
        MainWindow.welcome_label = QLabel("CSVファイルをここにドラッグ＆ドロップ\nまたは、以下のボタンから選択してください", MainWindow) #
        MainWindow.welcome_label.setAlignment(Qt.AlignCenter) #
        desc_font = MainWindow.welcome_label.font() # MainWindow.welcome_labelのフォントを取得
        desc_font.setPointSize(12)
        MainWindow.welcome_label.setFont(desc_font) #

        # ボタンコンテナ
        button_container = QWidget()
        button_layout = QHBoxLayout(button_container)
        button_layout.setSpacing(20)

        # ボタンの作成とMainWindow属性への割り当て
        MainWindow.new_file_button_welcome = QPushButton("新規作成", MainWindow) #
        MainWindow.open_file_button_welcome = QPushButton("ファイルを開く", MainWindow) #
        MainWindow.sample_data_button_welcome = QPushButton("サンプルデータ", MainWindow) #

        # ボタンのサイズとアイコン設定
        for btn in [MainWindow.new_file_button_welcome, MainWindow.open_file_button_welcome, MainWindow.sample_data_button_welcome]: #
            btn.setMinimumSize(150, 50) #
            btn.setStyleSheet("font-weight: bold;")

        # アイコン設定
        MainWindow.new_file_button_welcome.setIcon(MainWindow.style().standardIcon(QStyle.SP_FileDialogNewFolder))
        MainWindow.open_file_button_welcome.setIcon(MainWindow.style().standardIcon(QStyle.SP_DialogOpenButton))
        MainWindow.sample_data_button_welcome.setIcon(MainWindow.style().standardIcon(QStyle.SP_FileDialogDetailedView))

        # ボタンをレイアウトに追加
        button_layout.addStretch()
        button_layout.addWidget(MainWindow.new_file_button_welcome)
        button_layout.addWidget(MainWindow.open_file_button_welcome)
        button_layout.addWidget(MainWindow.sample_data_button_welcome)
        button_layout.addStretch()

        # 全体レイアウトに追加
        welcome_layout.addStretch(1)
        welcome_layout.addWidget(welcome_title)
        welcome_layout.addSpacing(20)
        welcome_layout.addWidget(MainWindow.welcome_label) # MainWindow.welcome_label を使用
        welcome_layout.addSpacing(40)
        welcome_layout.addWidget(button_container)
        welcome_layout.addSpacing(30)
        welcome_layout.addStretch(2)

        # ウェルカムウィジェットをメインレイアウトに追加
        MainWindow.main_layout.addWidget(MainWindow.welcome_widget)
        
        # 初期状態でウェルカム画面を表示
        MainWindow.welcome_widget.show()

        # ステータスバーの作成
        self._create_status_bar(MainWindow)

    def _create_menu_bar(self, MainWindow):
        menuBar = MainWindow.menuBar()
        file_menu = menuBar.addMenu("ファイル(&F)")
        MainWindow.open_action = QAction(MainWindow.style().standardIcon(QStyle.SP_DialogOpenButton), "開く(&O)...", MainWindow)
        MainWindow.open_action.setShortcut(QKeySequence.Open)
        MainWindow.save_action = QAction(MainWindow.style().standardIcon(QStyle.SP_DialogSaveButton), "上書き保存(&S)", MainWindow)
        MainWindow.save_action.setShortcut(QKeySequence.Save)
        MainWindow.save_as_action = QAction("名前を付けて保存(&A)...", MainWindow)
        MainWindow.exit_action = QAction("終了(&X)", MainWindow)
        MainWindow.exit_action.setShortcut(QKeySequence.Quit)
        
        MainWindow.new_action = QAction(MainWindow.style().standardIcon(QStyle.SP_FileDialogNewFolder), "新規作成(&N)", MainWindow)
        MainWindow.new_action.setShortcut(QKeySequence.New)

        file_menu.addAction(MainWindow.new_action)
        file_menu.addAction(MainWindow.open_action)
        file_menu.addAction(MainWindow.save_action)
        file_menu.addAction(MainWindow.save_as_action)
        file_menu.addSeparator()
        file_menu.addAction(MainWindow.exit_action)

        MainWindow.edit_menu = menuBar.addMenu("編集(&E)")
        MainWindow.undo_action = QAction("元に戻す", MainWindow)
        MainWindow.undo_action.setShortcut(QKeySequence.Undo)
        MainWindow.redo_action = QAction("やり直し", MainWindow)
        MainWindow.redo_action.setShortcut(QKeySequence.Redo)
        MainWindow.cut_action = QAction("切り取り", MainWindow)
        MainWindow.cut_action.setShortcut(QKeySequence.Cut)
        MainWindow.copy_action = QAction("コピー", MainWindow)
        MainWindow.copy_action.setShortcut(QKeySequence.Copy)
        MainWindow.paste_action = QAction("貼り付け", MainWindow)
        MainWindow.paste_action.setShortcut(QKeySequence.Paste)
        MainWindow.delete_action = QAction("削除", MainWindow)
        MainWindow.delete_action.setShortcut(QKeySequence.Delete)
        MainWindow.cell_concatenate_action = QAction("セルの値を連結...", MainWindow)
        MainWindow.column_concatenate_action = QAction("列の値を連結...", MainWindow)
        merge_menu = QMenu("連結", MainWindow)
        merge_menu.addAction(MainWindow.cell_concatenate_action)
        merge_menu.addAction(MainWindow.column_concatenate_action)
        MainWindow.copy_column_action = QAction("列をコピー", MainWindow)
        MainWindow.copy_column_action.setShortcut(QKeySequence("Ctrl+Shift+C"))
        MainWindow.paste_column_action = QAction("列に貼り付け", MainWindow)
        MainWindow.paste_column_action.setShortcut(QKeySequence("Ctrl+Shift+V"))
        MainWindow.add_row_action = QAction("行を追加", MainWindow)
        MainWindow.add_column_action = QAction("右に列を挿入", MainWindow)
        MainWindow.delete_selected_rows_action = QAction("選択行を削除", MainWindow)
        MainWindow.delete_selected_column_action = QAction("選択列を削除", MainWindow)
        sort_menu = QMenu("ソート", MainWindow)
        MainWindow.sort_asc_action = QAction("現在の列を昇順でソート", MainWindow)
        MainWindow.sort_desc_action = QAction("現在の列を降順でソート", MainWindow)
        MainWindow.clear_sort_action = QAction("ソートをクリア", MainWindow)
        sort_menu.addAction(MainWindow.sort_asc_action)
        sort_menu.addAction(MainWindow.sort_desc_action)
        sort_menu.addSeparator()
        sort_menu.addAction(MainWindow.clear_sort_action)
        MainWindow.select_all_action = QAction("すべて選択", MainWindow)
        MainWindow.select_all_action.setShortcut(QKeySequence.SelectAll)
        MainWindow.search_action = QAction("検索パネル", MainWindow)
        MainWindow.search_action.setShortcut(QKeySequence.Find)
        MainWindow.edit_menu.addAction(MainWindow.undo_action)
        MainWindow.edit_menu.addAction(MainWindow.redo_action)
        MainWindow.edit_menu.addSeparator()
        MainWindow.edit_menu.addAction(MainWindow.cut_action)
        MainWindow.edit_menu.addAction(MainWindow.copy_action)
        MainWindow.edit_menu.addAction(MainWindow.paste_action)
        MainWindow.edit_menu.addAction(MainWindow.delete_action)
        MainWindow.edit_menu.addSeparator()
        MainWindow.edit_menu.addMenu(merge_menu)
        MainWindow.edit_menu.addSeparator()
        MainWindow.edit_menu.addAction(MainWindow.copy_column_action)
        MainWindow.edit_menu.addAction(MainWindow.paste_column_action)
        MainWindow.edit_menu.addSeparator()
        MainWindow.edit_menu.addAction(MainWindow.add_row_action)
        MainWindow.edit_menu.addAction(MainWindow.add_column_action)
        MainWindow.edit_menu.addAction(MainWindow.delete_selected_rows_action)
        MainWindow.edit_menu.addAction(MainWindow.delete_selected_column_action)
        MainWindow.edit_menu.addSeparator()
        MainWindow.edit_menu.addMenu(sort_menu)
        MainWindow.edit_menu.addSeparator()
        MainWindow.edit_menu.addAction(MainWindow.select_all_action)
        MainWindow.edit_menu.addSeparator()
        MainWindow.edit_menu.addAction(MainWindow.search_action)

        MainWindow.tools_menu = menuBar.addMenu("ツール(&T)")
        MainWindow.price_calculator_action = QAction("金額計算ツール...", MainWindow)
        MainWindow.tools_menu.addAction(MainWindow.price_calculator_action)
        
        MainWindow.text_processing_action = QAction("テキスト処理ツール...", MainWindow)
        MainWindow.tools_menu.addAction(MainWindow.text_processing_action)

        MainWindow.csv_format_menu = menuBar.addMenu("CSVフォーマット(&C)")
        MainWindow.save_format_action = QAction("保存形式を指定して保存...", MainWindow)
        MainWindow.csv_format_menu.addAction(MainWindow.save_format_action)

        help_menu = menuBar.addMenu("ヘルプ(&H)")
        MainWindow.shortcuts_action = QAction("ショートカットキー一覧", MainWindow)
        help_menu.addAction(MainWindow.shortcuts_action)
        
        MainWindow.diagnose_action = QAction("表示診断（デバッグ）", MainWindow)
        help_menu.addAction(MainWindow.diagnose_action)
        
        MainWindow.force_show_action = QAction("強制表示（デバッグ）", MainWindow)
        help_menu.addAction(MainWindow.force_show_action)


    def _create_tool_bar(self, MainWindow):
        toolbar = MainWindow.addToolBar("Main Toolbar")
        toolbar.setIconSize(QSize(24, 24))
        toolbar.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        
        toolbar.setStyleSheet("""
            QToolButton {
                padding: 4px 8px;
                margin: 2px;
                min-width: 60px;
            }
        """)
        
        def add_action_with_tooltip(action, text_callback):
            toolbar.addAction(action)
            action.setText(action.text().replace("✂️ ", "").replace("📋 ", "").replace("📎 ", "").replace("🗑️ ", "").replace("📊 ", "").replace("💰 ", ""))
            widget = toolbar.widgetForAction(action)
            if widget:
                tooltip_filter = TooltipEventFilter(widget, text_callback)
                widget.installEventFilter(tooltip_filter)
                if not hasattr(MainWindow, 'tooltip_filters'):
                    MainWindow.tooltip_filters = []
                MainWindow.tooltip_filters.append(tooltip_filter)

        # グループ1: ファイル操作
        add_action_with_tooltip(MainWindow.new_action, lambda: "新しいCSVファイルを作成します (Ctrl+N)")
        add_action_with_tooltip(MainWindow.open_action, lambda: "新しいCSVファイルを開きます (Ctrl+O)")
        add_action_with_tooltip(MainWindow.save_action, lambda: f"現在の変更をファイルに上書き保存します (Ctrl+S)\nパス: {MainWindow.filepath or '未保存'}")
        toolbar.addSeparator()
        # グループ2: 編集操作
        MainWindow.undo_action.setIcon(MainWindow.style().standardIcon(QStyle.SP_ArrowBack))
        MainWindow.redo_action.setIcon(MainWindow.style().standardIcon(QStyle.SP_ArrowForward))
        add_action_with_tooltip(MainWindow.undo_action, lambda: "操作を元に戻します (Ctrl+Z)")
        add_action_with_tooltip(MainWindow.redo_action, lambda: "操作をやり直します (Ctrl+Y)")
        toolbar.addSeparator()
        
        # グループ3: 行・列の操作
        MainWindow.add_row_action.setIcon(MainWindow.style().standardIcon(QStyle.SP_FileIcon))
        MainWindow.add_column_action.setIcon(MainWindow.style().standardIcon(QStyle.SP_ArrowRight))
        MainWindow.delete_selected_rows_action.setIcon(MainWindow.style().standardIcon(QStyle.SP_TrashIcon))
        
        add_action_with_tooltip(MainWindow.add_row_action, lambda: "カーソル位置の下に新しい行を追加します")
        add_action_with_tooltip(MainWindow.add_column_action, lambda: "カーソル位置の右に新しい列を挿入します")
        add_action_with_tooltip(MainWindow.delete_selected_rows_action, lambda: "選択されている行を削除します")
        toolbar.addSeparator()
        
        # グループ4: 検索と表示
        MainWindow.search_action.setIcon(MainWindow.style().standardIcon(QStyle.SP_FileDialogInfoView))
        MainWindow.search_action.setText("検索パネル")
        add_action_with_tooltip(MainWindow.search_action, lambda: "検索・置換・抽出パネルの表示/非表示 (Ctrl+F)")
        
        MainWindow.view_toggle_action = QAction(MainWindow.style().standardIcon(QStyle.SP_FileDialogDetailedView), "カードビュー", MainWindow)
        add_action_with_tooltip(MainWindow.view_toggle_action, lambda: "テーブル表示とカード表示を切り替えます")
        toolbar.addSeparator()
        
        # グループ5: 高度な機能
        MainWindow.price_calculator_action.setIcon(MainWindow.style().standardIcon(QStyle.SP_DialogApplyButton))
        MainWindow.price_calculator_action.setText("金額計算")
        add_action_with_tooltip(MainWindow.price_calculator_action, lambda: "選択列の金額を一括計算します")
        toolbar.addSeparator()
        
        MainWindow.cell_concatenate_action.setText("セル連結")
        MainWindow.column_concatenate_action.setText("列連結")
        add_action_with_tooltip(MainWindow.cell_concatenate_action, lambda: "選択セルを隣のセルと連結します")
        add_action_with_tooltip(MainWindow.column_concatenate_action, lambda: "選択列を隣の列と連結します")
        toolbar.addSeparator()
        
        MainWindow.test_action = QAction(MainWindow.style().standardIcon(QStyle.SP_DialogHelpButton), "テストデータ", MainWindow)
        add_action_with_tooltip(MainWindow.test_action, lambda: "動作確認用のサンプルデータを読み込みます")

        MainWindow.text_processing_action.setIcon(MainWindow.style().standardIcon(QStyle.SP_FileDialogContentsView))
        add_action_with_tooltip(
            MainWindow.text_processing_action,
            lambda: "テキストに接頭辞追加・バイト数制限・単語境界調整を行います"
        )
        toolbar.addSeparator()
        
        add_action_with_tooltip(MainWindow.force_show_action, lambda: "表示がおかしい場合にテーブルを強制表示します（デバッグ用）")
        
    def _create_card_view_container(self, MainWindow):
        layout = QFormLayout(MainWindow.card_view_container)
        layout.setContentsMargins(20,20,20,20)
        
        nav_button_layout = QHBoxLayout()
        MainWindow.prev_record_button = QPushButton("前のレコード (Ctrl+←)")
        MainWindow.next_record_button = QPushButton("次のレコード (Ctrl+→)")
        nav_button_layout.addStretch()
        nav_button_layout.addWidget(MainWindow.prev_record_button)
        nav_button_layout.addWidget(MainWindow.next_record_button)
        nav_button_layout.addStretch()
        
        layout.addRow(nav_button_layout)

    def _create_status_bar(self, MainWindow):
        MainWindow.status_label = QLabel("ファイルを開いてください。")
        MainWindow.statusBar().addWidget(MainWindow.status_label, 1)
        MainWindow.operation_label = QLabel("")
        MainWindow.statusBar().addPermanentWidget(MainWindow.operation_label)
        MainWindow.hint_label = QLabel("")
        MainWindow.statusBar().addPermanentWidget(MainWindow.hint_label)
        MainWindow.progress_bar = QProgressBar(MainWindow)
        MainWindow.progress_bar.setMaximumWidth(120)
        MainWindow.progress_bar.hide()
        MainWindow.statusBar().addPermanentWidget(MainWindow.progress_bar)