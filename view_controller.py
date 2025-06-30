# view_controller.py (提案1のみ反映版)

import os
from PySide6.QtWidgets import (
    QMessageBox, QFormLayout, QLabel, QPlainTextEdit, QSizePolicy, 
    QApplication, QDataWidgetMapper, QAbstractItemView, QStyle 
)
from PySide6.QtCore import QObject, Signal, Qt, QTimer, QModelIndex, QEvent 

# TooltipEventFilterクラスを完全に削除（13-25行目を削除）
# ※dialogs.pyに既に存在するため、ここでは削除します。

class ViewController(QObject):
    """ビューの表示と切り替えを管理するコントローラー"""
    
    # シグナル定義
    view_changed = Signal(str)  # 'table' or 'card'
    context_hint_changed = Signal(str)  # hint type
    
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window # CsvEditorAppQtのインスタンス
        self.current_view = 'table' # 初期ビューはテーブル
        self.card_fields_widgets = {} # カードビューのフィールドウィジェットを保持
        
    def show_welcome_screen(self):
        """ウェルカム画面を表示"""
        print("DEBUG: ViewController.show_welcome_screen called")
        self.main_window.view_stack.hide()
        self.main_window.welcome_widget.show()
        self.main_window._set_ui_state('welcome')
        self.main_window.status_label.setText("ファイルを開いてください。")
        self.main_window.view_toggle_action.setEnabled(False)
        # バックエンドが残っている場合を考慮してクリーンアップを要求
        self.main_window.async_manager.cleanup_backend_requested.emit()
    
    def show_main_view(self):
        """メインビュー（テーブルまたはカード）を表示"""
        print("DEBUG: ViewController.show_main_view called")
        
        # ウェルカム画面を非表示
        self.main_window.welcome_widget.hide()
        
        # view_stackを表示
        self.main_window.view_stack.show()
        
        # 現在のビュー状態に応じて表示を切り替える
        if self.current_view == 'table':
            print("DEBUG: テーブルビューを表示")
            self.main_window.table_view.show()
            self.main_window.card_scroll_area.hide()
            self.main_window.view_toggle_action.setText("カードビュー")
            self.main_window.view_toggle_action.setIcon(
                self.main_window.style().standardIcon(QStyle.SP_FileDialogDetailedView)
            )
        else: # self.current_view == 'card'
            print("DEBUG: カードビューを表示")
            self.main_window.table_view.hide()
            self.main_window.card_scroll_area.show()
            self.main_window.view_toggle_action.setText("テーブルビュー")
            # 🔥 修正: SP_FileDialogListView は存在しないため SP_FileDialogContentsView に変更
            self.main_window.view_toggle_action.setIcon(
                self.main_window.style().standardIcon(QStyle.SP_FileDialogContentsView)
            )
        
        self.main_window._set_ui_state('normal') # main_windowのUI状態を設定
        self.main_window.view_toggle_action.setEnabled(True)
        
        # ビューの更新を強制
        self.main_window.table_view.viewport().update()
        QApplication.processEvents()
        
        print(f"DEBUG: view_stack.isVisible() = {self.main_window.view_stack.isVisible()}")
        print(f"DEBUG: table_view.isVisible() = {self.main_window.table_view.isVisible()}")
    
    def toggle_view(self):
        """テーブルビューとカードビューを切り替える"""
        if self.main_window.table_model.rowCount() == 0:
            self.main_window.show_operation_status("表示するデータがありません。", 3000, is_error=True)
            return
        
        current_index = self.main_window.table_view.currentIndex()
        if not current_index.isValid() and self.main_window.table_model.rowCount() > 0:
            current_index = self.main_window.table_model.index(0, 0)
        
        try: # エラーハンドリングを追加
            if self.current_view == 'table':
                # テーブルビュー → カードビュー
                if not current_index.isValid():
                    QMessageBox.information(self.main_window, "情報", 
                                          "カードビューで表示する行を選択してください。")
                    return
                
                print("DEBUG: テーブルビュー → カードビューへ切り替え")
                self._show_card_view(current_index.row())
                self.main_window.table_view.hide()
                self.main_window.card_scroll_area.show()
                self.main_window.view_toggle_action.setText("テーブルビュー")
                # 🔥 修正: SP_FileDialogListView → SP_FileDialogContentsView
                self.main_window.view_toggle_action.setIcon(
                    self.main_window.style().standardIcon(QStyle.SP_FileDialogContentsView)
                )
                self.current_view = 'card'
                print("DEBUG: カードビューへの切り替え完了")
            else: # self.current_view == 'card'
                # カードビュー → テーブルビュー
                print("DEBUG: カードビュー → テーブルビューへ切り替え")
                self.main_window.card_scroll_area.hide()
                self.main_window.table_view.show()
                self.main_window.view_toggle_action.setText("カードビュー")
                self.main_window.view_toggle_action.setIcon(
                    self.main_window.style().standardIcon(QStyle.SP_FileDialogDetailedView)
                )
                self.current_view = 'table'
                print("DEBUG: テーブルビューへの切り替え完了")
            
            # モデルとビューの更新を強制
            self.main_window.table_model.layoutChanged.emit()
            self.main_window.table_view.viewport().update()
            self.view_changed.emit(self.current_view) # ビュー変更シグナルを発行
            
        except Exception as e:
            print(f"ERROR: ビュー切り替え中にエラーが発生: {e}")
            import traceback
            traceback.print_exc()
            self.main_window.show_operation_status(f"ビュー切り替えエラー: {e}", is_error=True)
        
    def recreate_card_view_fields(self):
        """カードビューのフィールドを再作成"""
        print("DEBUG: recreate_card_view_fields called")
        
        layout = self.main_window.card_view_container.layout()
        
        # レイアウトがQFormLayoutであることを確認し、もし異なれば再設定
        if not isinstance(layout, QFormLayout):
            print("警告: card_view_containerのレイアウトがQFormLayoutではありません。再作成します。")
            if layout is not None:
                while layout.count():
                    item = layout.takeAt(0)
                    if item.widget():
                        item.widget().deleteLater()
            layout = QFormLayout()
            self.main_window.card_view_container.setLayout(layout)
        
        # ナビゲーションボタン以外のフィールドを削除 (rowCount() > 1でナビゲーションボタン行を残す)
        while layout.rowCount() > 1:
            layout.removeRow(1) # 1はナビゲーションボタンの行を想定
        
        self.card_fields_widgets.clear()
        self.main_window.card_mapper.clearMapping()
        
        # ヘッダーが存在しない場合は終了
        if not hasattr(self.main_window, 'header') or not self.main_window.header:
            print("WARNING: ヘッダーが定義されていません")
            return
        
        # 新しいフィールドを作成し、マップに追加
        for col_idx, col_name in enumerate(self.main_window.header): # main_window.headerを参照
            label = QLabel(f"{col_name}:")
            
            field_widget = QPlainTextEdit()
            field_widget.setLineWrapMode(QPlainTextEdit.WidgetWidth)
            field_widget.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
            field_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
            
            # スタイル設定はmain_windowのテーマを使用
            theme = self.main_window.theme
            field_widget.setStyleSheet(f"""
                QPlainTextEdit {{
                    background-color: {theme.BG_LEVEL_0};
                    color: {theme.TEXT_PRIMARY};
                    border: 1px solid {theme.BG_LEVEL_3};
                    padding: 2px;
                }}
                QScrollBar:vertical {{
                    border: 1px solid {theme.BG_LEVEL_3};
                    background: {theme.BG_LEVEL_2};
                    width: 10px;
                    margin: 0px 0px 0px 0px;
                }}
                QScrollBar::handle:vertical {{
                    background: {theme.PRIMARY};
                    min-height: 20px;
                }}
                QScrollBar::add-line:vertical {{
                    border: none;
                    background: none;
                }}
                QScrollBar::sub-line:vertical {{
                    border: none;
                    background: none;
                }}
            """)
            
            # 高さ調整は自身(ViewController)のメソッドを呼び出し
            field_widget.document().contentsChanged.connect(
                lambda f=field_widget: self._adjust_text_edit_height(f)
            )
            
            density = self.main_window.density # main_windowの密度設定を使用
            # ここは_adjust_text_edit_height内で設定されるためコメントアウトまたは削除
            # field_widget.setMinimumHeight(int(density['row_height'] * 1.5))
            # field_widget.setMaximumHeight(int(density['row_height'] * 8))
            
            self.card_fields_widgets[col_name] = field_widget
            layout.addRow(label, field_widget)
            self.main_window.card_mapper.addMapping(field_widget, col_idx, b'plainText')
            field_widget.installEventFilter(self.main_window) # main_windowがイベントフィルターとして機能

        # カードマッパーの設定 (モデルはmain_windowのものを使用)
        self.main_window.card_mapper.setModel(self.main_window.table_model)
        
        # カードビューが表示されている場合、現在の行を再表示
        if self.main_window.card_scroll_area.isVisible():
            current_index = self.main_window.table_view.currentIndex()
            row_to_show = current_index.row() if current_index.isValid() else 0
            if self.main_window.table_model.rowCount() > 0:
                self._show_card_view(row_to_show)
        
        print(f"DEBUG: カードビューフィールド作成完了: {len(self.card_fields_widgets)}個のフィールド")
    
    def _show_card_view(self, row_idx_in_model):
        """カードビューを表示"""
        print(f"DEBUG: _show_card_view called with row {row_idx_in_model}")
        
        if not self.main_window.table_model.rowCount():
            self.main_window.show_operation_status("表示するデータがありません。", 3000, is_error=True)
            return
        
        model_index = self.main_window.table_model.index(row_idx_in_model, 0)
        if not model_index.isValid():
            model_index = self.main_window.table_model.index(0, 0) # 無効なら最初の行を試す
        
        if not model_index.isValid(): # それでも無効ならデータがない
            self.main_window.show_operation_status("表示するデータがありません。", 3000, is_error=True)
            return
        
        self.main_window.card_mapper.setCurrentIndex(model_index.row())
        
        # フィールドの高さを調整
        for field_widget in self.card_fields_widgets.values():
            self._adjust_text_edit_height(field_widget)
        
        # 自動送信ポリシーを設定
        self.main_window.card_mapper.setSubmitPolicy(
            QDataWidgetMapper.AutoSubmit # QDataWidgetMapperクラスから直接参照
        )
        
        print(f"DEBUG: カードビュー表示完了: 行 {model_index.row()}")
    
    # 提案1: 自動調整の改善（簡単）を反映
    def _adjust_text_edit_height(self, text_edit_widget: QPlainTextEdit):
        """改良版：コンテンツ量に完全対応"""
        doc = text_edit_widget.document()
        text_edit_widget.setUpdatesEnabled(False)
        
        # ドキュメントの実際の高さを計算
        content_height = int(doc.size().height() +
                             text_edit_widget.contentsMargins().top() +
                             text_edit_widget.contentsMargins().bottom() + 10)
        
        density = self.main_window.density
        min_height = int(density['row_height'] * 1.5)  # 最小1.5行
        
        # 最大高さを画面の50%に設定（8行制限を撤廃）
        screen_height = QApplication.primaryScreen().size().height()
        max_height = int(screen_height * 0.5)
        
        # HTMLコンテンツの場合は追加マージン
        # QPlainTextEditにHTMLコンテンツが設定される場合、toPlainText()でタグが削除されるため、
        # ここでHTMLコンテンツであるかを判別するのは難しい。
        # 描画されたドキュメントの高さで判断するため、この判定は不要か、別の方法を検討。
        # 今回は提案通りに実装するが、HTMLレンダリング後の正確な高さが必要ならQTextDocumentのレイアウトを直接参照すべき。
        if '<' in text_edit_widget.toPlainText():
            content_height += 20
        
        final_height = max(min_height, min(content_height, max_height))
        text_edit_widget.setFixedHeight(final_height)
        text_edit_widget.setUpdatesEnabled(True)
    
    def go_to_prev_record(self):
        """前のレコードへ移動"""
        current_row = self.main_window.card_mapper.currentIndex()
        new_row = current_row - 1
        self._move_card_record(new_row)
    
    def go_to_next_record(self):
        """次のレコードへ移動"""
        current_row = self.main_window.card_mapper.currentIndex()
        new_row = current_row + 1
        self._move_card_record(new_row)
    
    def _move_card_record(self, new_row: int):
        """カードビューのレコード移動ロジック"""
        if 0 <= new_row < self.main_window.table_model.rowCount():
            self.main_window.card_mapper.setCurrentIndex(new_row)
            
            # フィールドの高さを再調整
            for field_widget in self.card_fields_widgets.values():
                self._adjust_text_edit_height(field_widget)
            
            # テーブルビューも同期させる
            self.main_window.table_view.setCurrentIndex(
                self.main_window.table_model.index(new_row, 0)
            )
            # 🔥 修正: PositionAtCenterの正しい参照方法
            self.main_window.table_view.scrollTo(
                self.main_window.table_model.index(new_row, 0),
                QAbstractItemView.PositionAtCenter # QAbstractItemViewクラスから直接参照
            )
            
            self.main_window.show_operation_status(
                f"レコード {new_row + 1}/{self.main_window.table_model.rowCount()}"
            )
        else:
            self.main_window.show_operation_status("これ以上レコードはありません。", 2000)
    
    def show_context_hint(self, hint_type=''):
        """ステータスバーにヒントを表示"""
        if hint_type == 'column_selected':
            hint = "ヒント: 列ヘッダーを右クリックして列の操作、Ctrl+Shift+Cで列コピーができます。"
        elif hint_type == 'row_selected':
            hint = "ヒント: 選択行を右クリックして行削除、Ctrl+Cで行コピーができます。"
        elif hint_type == 'cell_selected':
            hint = "ヒント: Ctrl+Cでコピー、Ctrl+Xで切り取り、Deleteでクリアができます。"
        elif hint_type == 'editing':
            hint = "編集中: Enterで次のセルへ、Shift+Enterで上のセルへ移動します。"
        else:
            if self.main_window.filepath:
                total_rows = self.main_window.table_model.rowCount()
                total_cols = self.main_window.table_model.columnCount()
                hint = f"{os.path.basename(self.main_window.filepath)} ({total_rows:,}行, {total_cols}列, {self.main_window.encoding})"
            else:
                hint = "ファイルを開いてください。"
        
        self.main_window.status_label.setText(hint)
        self.context_hint_changed.emit(hint_type) # ヒント変更シグナルを発行