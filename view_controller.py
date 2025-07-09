# view_controller.py (提案1のみ反映版)

import os
from PySide6.QtWidgets import (
    QMessageBox, QFormLayout, QLabel, QPlainTextEdit, QSizePolicy, 
    QApplication, QDataWidgetMapper, QAbstractItemView, QStyle 
)
from PySide6.QtCore import QObject, Signal, Qt, QTimer, QModelIndex, QEvent 
import re # 追加: ContentAnalyzerでreを使用
from collections import Counter # 追加: ContentAnalyzerでCounterを使用

# TooltipEventFilterクラスを完全に削除（13-25行目を削除）
# ※dialogs.pyに既に存在するため、ここでは削除します。

class ContentAnalyzer:
    """実際のコンテンツを詳細に分析してサイズを決定"""
    
    # HTMLタグの複雑度分類
    SIMPLE_TAGS = {'br', 'b', 'i', 'u', 'strong', 'em', 'span'}
    COMPLEX_TAGS = {'table', 'div', 'ul', 'ol', 'dl', 'form'}
    MEDIA_TAGS = {'img', 'video', 'iframe', 'object', 'embed'}
    
    @classmethod
    def analyze_content(cls, content: str, column_name: str = "") -> dict:
        """コンテンツの詳細分析"""
        if not content:
            return {
                'type': 'empty',
                'complexity': 0,
                'suggested_rows': (1, 2),
                'priority': 'low'
            }
        
        content_str = str(content).strip()
        
        # 基本メトリクス
        char_count = len(content_str)
        line_breaks = content_str.count('\n') + content_str.count('<br')
        
        # HTMLタグ分析
        tag_analysis = cls._analyze_html_tags(content_str)
        
        # URL検出
        url_count = len(re.findall(r'https?://[^\s<>"]+', content_str))
        
        # 画像検出（imgタグ + 画像URL）
        img_count = tag_analysis['media_tags'].get('img', 0)
        img_url_count = len(re.findall(r'\.(jpg|jpeg|png|gif|webp|svg)["\s>]', content_str, re.I))
        total_images = img_count + img_url_count
        
        # コンテンツタイプの判定
        content_type = cls._determine_content_type(
            char_count, line_breaks, tag_analysis, total_images, url_count
        )
        
        # サイズ提案
        suggested_rows = cls._calculate_suggested_size(
            content_type, char_count, line_breaks, tag_analysis, total_images
        )
        
        return {
            'type': content_type,
            'complexity': tag_analysis['complexity'],
            'suggested_rows': suggested_rows,
            'priority': cls._determine_priority(content_type, tag_analysis),
            'metrics': {
                'chars': char_count,
                'lines': line_breaks,
                'images': total_images,
                'tables': tag_analysis['complex_tags'].get('table', 0),
                'urls': url_count
            }
        }
    
    @classmethod
    def _analyze_html_tags(cls, content: str) -> dict:
        """HTMLタグの詳細分析"""
        # すべてのHTMLタグを抽出
        all_tags = re.findall(r'<([^>/\s]+)[\s>]', content.lower())
        tag_counter = Counter(all_tags)
        
        # タグを分類
        simple_tags = {tag: count for tag, count in tag_counter.items() 
                       if tag in cls.SIMPLE_TAGS}
        complex_tags = {tag: count for tag, count in tag_counter.items() 
                        if tag in cls.COMPLEX_TAGS}
        media_tags = {tag: count for tag, count in tag_counter.items() 
                      if tag in cls.MEDIA_TAGS}
        
        # 複雑度の計算
        complexity = (
            sum(simple_tags.values()) * 1 +
            sum(complex_tags.values()) * 5 +
            sum(media_tags.values()) * 3
        )
        
        return {
            'total_tags': len(all_tags),
            'unique_tags': len(tag_counter),
            'simple_tags': simple_tags,
            'complex_tags': complex_tags,
            'media_tags': media_tags,
            'complexity': complexity
        }
    
    @classmethod
    def _determine_content_type(cls, chars, lines, tag_analysis, images, urls):
        """コンテンツタイプの判定"""
        # 画像のみ or 画像主体
        if images > 0 and chars < 100:
            return 'image_only'
        elif images > 3:
            return 'image_rich'
        
        # テーブル含有
        if tag_analysis['complex_tags'].get('table', 0) > 0:
            return 'table_content'
        
        # 複雑なHTML
        if tag_analysis['complexity'] > 20:
            return 'html_complex'
        
        # シンプルなHTML（br, b, i等のみ）
        if tag_analysis['total_tags'] > 0 and tag_analysis['complexity'] < 10:
            return 'html_simple'
        
        # URL主体
        if urls > 2:
            return 'url_list'
        
        # プレーンテキスト
        if chars > 500:
            return 'text_long'
        elif chars > 100:
            return 'text_medium'
        else:
            return 'text_short'
    
    @classmethod
    def _calculate_suggested_size(cls, content_type, chars, lines, tag_analysis, images):
        """コンテンツタイプに基づくサイズ計算"""
        # 基本サイズマップ
        size_map = {
            'empty': (1, 2),
            'text_short': (1, 3),
            'text_medium': (2, 5),
            'text_long': (3, 10),
            'html_simple': (2, 8),
            'html_complex': (5, 20),
            'table_content': (8, 25),
            'image_only': (3, 8),
            'image_rich': (5, 15),
            'url_list': (3, 10)
        }
        
        min_rows, max_rows = size_map.get(content_type, (2, 8))
        
        # 動的調整
        # 改行数による調整
        if lines > 5:
            min_rows = max(min_rows, min(lines // 2, 5))
            max_rows = max(max_rows, min(lines + 3, 30))
        
        # 画像数による調整
        if images > 0:
            # 画像1つにつき2-3行分のスペースを確保
            min_rows = max(min_rows, images * 2)
            max_rows = max(max_rows, images * 3 + 2)
        
        # テーブルによる調整
        if tag_analysis['complex_tags'].get('table', 0) > 0:
            min_rows = max(min_rows, 8)
            max_rows = max(max_rows, 20)
        
        return (min_rows, max_rows)
    
    @classmethod
    def _determine_priority(cls, content_type, tag_analysis):
        """表示優先度の決定"""
        if content_type in ['table_content', 'html_complex', 'image_rich']:
            return 'high'
        elif content_type in ['html_simple', 'text_long', 'url_list']:
            return 'medium'
        else:
            return 'low'

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
        """テーブルビューとカードビューを切り替える（安全版）"""
        if self.main_window.table_model.rowCount() == 0:
            self.main_window.show_operation_status("表示するデータがありません。", 3000, is_error=True)
            return

        current_index = self.main_window.table_view.currentIndex()
        if not current_index.isValid() and self.main_window.table_model.rowCount() > 0:
            current_index = self.main_window.table_model.index(0, 0)

        try:
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
                self.main_window.view_toggle_action.setIcon(
                    self.main_window.style().standardIcon(QStyle.SP_FileDialogContentsView)
                )
                self.current_view = 'card'
                print("DEBUG: カードビューへの切り替え完了")

            else:  # self.current_view == 'card'
                # カードビュー → テーブルビュー
                print("DEBUG: カードビュー → テーブルビューへ切り替え")
                
                # 🔥 重要：編集フラグチェックによる安全な保存
                has_edits = False
                if hasattr(self.main_window, 'card_mapper'):
                    # 各フィールドの編集状態をチェック
                    for widget in self.card_fields_widgets.values():
                        if hasattr(widget, 'document') and widget.document().isModified():
                            has_edits = True
                            break
                    
                    # 編集がある場合のみsubmit
                    if has_edits:
                        print("DEBUG: 編集内容を検出、保存を実行")
                        self.main_window.card_mapper.submit()
                        # 編集フラグをリセット
                        for widget in self.card_fields_widgets.values():
                            if hasattr(widget, 'document'):
                                widget.document().setModified(False)
                    else:
                        print("DEBUG: 編集なし、submitをスキップ")

                # ビューを切り替え
                self.main_window.card_scroll_area.hide()
                self.main_window.table_view.show()
                self.main_window.view_toggle_action.setText("カードビュー")
                self.main_window.view_toggle_action.setIcon(
                    self.main_window.style().standardIcon(QStyle.SP_FileDialogDetailedView)
                )
                self.current_view = 'table'

                # テーブルビューの現在位置を同期
                if hasattr(self.main_window, 'card_mapper'):
                    current_card_row = self.main_window.card_mapper.currentIndex()
                    if 0 <= current_card_row < self.main_window.table_model.rowCount():
                        table_index = self.main_window.table_model.index(current_card_row, 0)
                        self.main_window.table_view.setCurrentIndex(table_index)
                        self.main_window.table_view.scrollTo(table_index, QAbstractItemView.PositionAtCenter)

                print("DEBUG: テーブルビューへの切り替え完了")

            # モデルとビューの更新
            self.main_window.table_view.viewport().update()
            self.view_changed.emit(self.current_view)

        except Exception as e:
            print(f"ERROR: ビュー切り替え中にエラーが発生: {e}")
            import traceback
            traceback.print_exc()
            self.main_window.show_operation_status(f"ビュー切り替えエラー: {e}", is_error=True)

    def recreate_card_view_fields(self):
        """カードビューのフィールドを再作成（完全安全版）"""
        print("DEBUG: recreate_card_view_fields called")

        layout = self.main_window.card_view_container.layout()
        
        # レイアウトの確認と再作成
        if not isinstance(layout, QFormLayout):
            print("警告: card_view_containerのレイアウトがQFormLayoutではありません。再作成します。")
            if layout is not None:
                while layout.count():
                    item = layout.takeAt(0)
                    if item.widget():
                        item.widget().deleteLater()
            layout = QFormLayout()
            self.main_window.card_view_container.setLayout(layout)

        # ナビゲーションボタン以外のフィールドを削除
        while layout.rowCount() > 1:
            layout.removeRow(1)

        # 🔥 重要：マッピングクリア時にsubmitを防ぐ
        if hasattr(self.main_window, 'card_mapper'):
            # 一時的にManualSubmitに設定してからクリア
            self.main_window.card_mapper.setSubmitPolicy(QDataWidgetMapper.ManualSubmit)
            self.main_window.card_mapper.clearMapping()

        self.card_fields_widgets.clear()

        # ヘッダーが存在しない場合は終了
        if not hasattr(self.main_window, 'header') or not self.main_window.header:
            print("WARNING: ヘッダーが定義されていません")
            return

        # 新しいフィールドを作成
        for col_idx, col_name in enumerate(self.main_window.header):
            label = QLabel(f"{col_name}:")
            
            field_widget = QPlainTextEdit()
            field_widget.setProperty("column_name", col_name)
            field_widget.setLineWrapMode(QPlainTextEdit.WidgetWidth)
            field_widget.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
            field_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

            # スタイル設定
            theme = self.main_window.theme
            field_widget.setStyleSheet(f"""
                QPlainTextEdit {{
                    background-color: {theme.BG_LEVEL_0};
                    color: {theme.TEXT_PRIMARY};
                    border: 1px solid {theme.BG_LEVEL_3};
                    padding: 4px;
                    font-family: "Consolas", "Monaco", monospace;
                }}
            """)

            # 初期サイズ設定
            field_widget.setMinimumHeight(30)
            field_widget.setMaximumHeight(100)

            # 高さ調整の接続
            field_widget.document().contentsChanged.connect(
                lambda f=field_widget: self._adjust_text_edit_height(f)
            )
            
            # 🔥 新機能：直接的なモデル更新
            field_widget.textChanged.connect(
                lambda fw=field_widget, c=col_idx: self._on_card_field_changed(fw, c)
            )

            self.card_fields_widgets[col_name] = field_widget
            layout.addRow(label, field_widget)

            # マッピング追加
            self.main_window.card_mapper.addMapping(field_widget, col_idx, b'plainText')
            
            # イベントフィルター設定
            field_widget.installEventFilter(self)

        # カードマッパーの設定
        self.main_window.card_mapper.setModel(self.main_window.table_model)
        
        # 🔥 重要：ManualSubmitポリシーで固定
        self.main_window.card_mapper.setSubmitPolicy(QDataWidgetMapper.ManualSubmit)

        # 現在の行を再表示
        if self.main_window.card_scroll_area.isVisible():
            current_index = self.main_window.table_view.currentIndex()
            row_to_show = current_index.row() if current_index.isValid() else 0
            if self.main_window.table_model.rowCount() > 0:
                self._show_card_view(row_to_show)

        print(f"DEBUG: カードビューフィールド作成完了: {len(self.card_fields_widgets)}個のフィールド")

    def _show_card_view(self, row_idx_in_model):
        """カードビューを表示（安全版）"""
        print(f"DEBUG: _show_card_view called with row {row_idx_in_model}")

        if not self.main_window.table_model.rowCount():
            self.main_window.show_operation_status("表示するデータがありません。", 3000, is_error=True)
            return

        model_index = self.main_window.table_model.index(row_idx_in_model, 0)
        if not model_index.isValid():
            model_index = self.main_window.table_model.index(0, 0)

        if not model_index.isValid():
            self.main_window.show_operation_status("表示するデータがありません。", 3000, is_error=True)
            return

        # 🔥 安全な行変更
        if hasattr(self.main_window, 'card_mapper'):
            # 現在の編集内容を保存（必要な場合のみ）
            has_edits = any(widget.document().isModified()
                           for widget in self.card_fields_widgets.values()
                           if hasattr(widget, 'document'))
            
            if has_edits:
                print("DEBUG: 行変更前に編集内容を保存")
                self.main_window.card_mapper.submit()
                # 編集フラグをリセット
                for widget in self.card_fields_widgets.values():
                    if hasattr(widget, 'document'):
                        widget.document().setModified(False)

            # 新しい行に移動
            self.main_window.card_mapper.setCurrentIndex(model_index.row())

            # フィールドの高さを調整
            for field_widget in self.card_fields_widgets.values():
                self._adjust_text_edit_height(field_widget)

        # フォーカス設定
        self.main_window.card_scroll_area.setFocus()
        
        if self.card_fields_widgets:
            first_widget = next(iter(self.card_fields_widgets.values()))
            QTimer.singleShot(50, lambda: first_widget.setFocus())

        print(f"DEBUG: カードビュー表示完了: 行 {model_index.row()}")

    def _on_card_field_changed(self, field_widget: QPlainTextEdit, col_idx: int):
        """カードフィールドの内容変更時の直接モデル更新"""
        current_row = self.main_window.card_mapper.currentIndex()
        if not (0 <= current_row < self.main_window.table_model.rowCount()):
            return

        model_index = self.main_window.table_model.index(current_row, col_idx)
        new_value = field_widget.toPlainText()
        
        # 直接モデルを更新（QDataWidgetMapperを経由しない）
        self.main_window.table_model.setData(model_index, new_value, Qt.EditRole)

    def _adjust_text_edit_height(self, text_edit_widget: QPlainTextEdit):
        """コンテンツ分析に基づく動的高さ調整"""
        try:
            text_edit_widget.setUpdatesEnabled(False)
            
            # 基本情報の取得
            column_name = text_edit_widget.property("column_name") or ""
            content = text_edit_widget.toPlainText()
            
            # コンテンツ分析
            analysis = ContentAnalyzer.analyze_content(content, column_name)
            
            # 画面とレイアウト情報
            density = self.main_window.density
            line_height = density['row_height']
            screen_height = QApplication.primaryScreen().size().height()
            
            # サイズ計算
            min_rows, max_rows = analysis['suggested_rows']
            
            # 列名によるヒント調整（補助的）
            if column_name:
                col_lower = column_name.lower()
                # モバイル/PC用説明文は通常長い
                if any(x in col_lower for x in ['pc用', 'スマートフォン用', 'mobile']):
                    min_rows = max(min_rows, 3)
                    max_rows = max(max_rows, 15)
                # 明示的に「番号」「コード」「ID」を含む場合は抑制
                elif any(x in col_lower for x in ['番号', 'コード', 'code', 'id']) and \
                     analysis['type'] in ['text_short', 'text_medium']:
                    max_rows = min(max_rows, 3)
            
            # 安全な範囲に制限
            min_height = max(30, int(line_height * min_rows))
            max_height = min(
                int(screen_height * 0.4),  # 画面の40%まで
                int(line_height * max_rows)
            )
            
            # 現在の高さから段階的に変更（急激な変更を避ける）
            current_height = text_edit_widget.height()
            if current_height > 0:
                # 急激な縮小を防ぐ
                if max_height < current_height * 0.5:
                    max_height = int(current_height * 0.7)
                # 急激な拡大を防ぐ
                if min_height > current_height * 2:
                    min_height = int(current_height * 1.3)
            
            # サイズ設定
            text_edit_widget.setMinimumHeight(min_height)
            text_edit_widget.setMaximumHeight(max_height)
            
            # メタデータ保存（デバッグ用）
            text_edit_widget.setProperty("content_analysis", analysis)
            
            # デバッグ出力（開発時のみ）
            if os.environ.get('CSV_EDITOR_DEBUG', '0') == '1':
                print(f"Field '{column_name}': Type={analysis['type']}, "
                      f"Size={min_height}-{max_height}px, "
                      f"Metrics={analysis['metrics']}")
            
        except Exception as e:
            # エラー時のフォールバック
            print(f"Height adjustment error for {column_name}: {e}")
            text_edit_widget.setMinimumHeight(50)
            text_edit_widget.setMaximumHeight(200)
        finally:
            text_edit_widget.setUpdatesEnabled(True)
    
    # 修正1: 未実装メソッドの追加
    def go_to_prev_record(self):
        """前のレコードへ移動"""
        current_row = self.main_window.card_mapper.currentIndex()
        new_row = current_row - 1
        self._move_card_record(new_row)
    
    # 修正1: 未実装メソッドの追加 (go_to_next_recordは既存だが、完全なガイドに従い再度記載)
    def go_to_next_record(self): 
        """次のレコードへ移動""" 
        current_row = self.main_window.card_mapper.currentIndex() 
        new_row = current_row + 1 
        self._move_card_record(new_row) 
    
    def _move_card_record(self, new_row: int):
        """カードビューのレコード移動ロジック（安全版）"""
        if 0 <= new_row < self.main_window.table_model.rowCount():
            # 編集内容の保存（必要な場合のみ）
            has_edits = any(widget.document().isModified()
                           for widget in self.card_fields_widgets.values()
                           if hasattr(widget, 'document'))
            
            if has_edits and hasattr(self.main_window, 'card_mapper'):
                print("DEBUG: レコード移動前に編集内容を保存")
                self.main_window.card_mapper.submit()
                # 編集フラグをリセット
                for widget in self.card_fields_widgets.values():
                    if hasattr(widget, 'document'):
                        widget.document().setModified(False)

            # 新しいレコードに移動
            self.main_window.card_mapper.setCurrentIndex(new_row)

            # フィールドの高さを再調整
            for field_widget in self.card_fields_widgets.values():
                self._adjust_text_edit_height(field_widget)

            # テーブルビューも同期
            self.main_window.table_view.setCurrentIndex(
                self.main_window.table_model.index(new_row, 0)
            )
            self.main_window.table_view.scrollTo(
                self.main_window.table_model.index(new_row, 0),
                QAbstractItemView.PositionAtCenter
            )
            self.main_window.show_operation_status(
                f"レコード {new_row + 1}/{self.main_window.table_model.rowCount()}"
            )
        else:
            self.main_window.show_operation_status("これ以上レコードはありません。", 2000)
    
    # 修正2: ViewControllerへのイベントフィルター実装
    def eventFilter(self, obj, event):
        """
        カードビュー内のQPlainTextEditからのキーイベントを捕捉し、
        レコード移動を処理する専用イベントフィルター
        """
        if isinstance(obj, QPlainTextEdit):
            # FocusInイベントで誤ってデータが変更されないようにする
            if event.type() == QEvent.FocusIn:
                return False  # FocusInイベントは通常通り処理
            
            # KeyPressイベントのみ特別処理
            if event.type() == QEvent.KeyPress:
                if event.modifiers() & Qt.ControlModifier:
                    if event.key() == Qt.Key_Left:
                        print("DEBUG: Ctrl+Left pressed in card view")
                        self.go_to_prev_record()
                        return True  # イベントを消費
                    elif event.key() == Qt.Key_Right:
                        print("DEBUG: Ctrl+Right pressed in card view")
                        self.go_to_next_record()
                        return True
                    elif event.key() == Qt.Key_Up:
                        print("DEBUG: Ctrl+Up pressed in card view")
                        current_row = self.main_window.card_mapper.currentIndex()
                        if current_row > 0:
                            self._move_card_record(current_row - 1)
                        else:
                            self.main_window.show_operation_status("最初のレコードです。", 2000)
                        return True
                    elif event.key() == Qt.Key_Down:
                        print("DEBUG: Ctrl+Down pressed in card view")
                        current_row = self.main_window.card_mapper.currentIndex()
                        if current_row < self.main_window.table_model.rowCount() - 1:
                            self._move_card_record(current_row + 1)
                        else:
                            self.main_window.show_operation_status("最後のレコードです。", 2000)
                        return True
            
        return super().eventFilter(obj, event)

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