# file_io_controller.py

import os
import csv
import pandas as pd
import traceback
from PySide6.QtWidgets import (
    QFileDialog, QMessageBox, QApplication, QProgressDialog,
    QDialog, QVBoxLayout, QRadioButton, QPushButton,
    QLabel, QDialogButtonBox, QInputDialog
)
from PySide6.QtCore import QObject, Signal, Qt, QTimer, QModelIndex # QModelIndex を追加
import config

# 🔥 修正: dialogs.pyからのEncodingSaveDialog, CSVSaveFormatDialog, NewFileDialogを明示的にインポート
from dialogs import EncodingSaveDialog, CSVSaveFormatDialog, NewFileDialog
import re
import psutil
from threading import Thread

# main_qt.py をインポート (CsvEditorAppQt を参照するため)
# ただし、循環参照を避けるため、必要な関数やクラスのみをインポートするか、
# 関数内で遅延インポートを検討する。ここでは CsvEditorAppQt クラス全体が必要なので
# メソッド内で遅延インポートを試みる。

class FileIOController(QObject):
    """ファイルI/O操作を管理するコントローラー"""
    
    # シグナル定義
    # dataframe/backend は object 型として定義。実際には pd.DataFrame または SQLiteBackend/LazyCSVLoader のインスタンス
    file_loaded = Signal(object, str, str)  # data_object (df or backend), filepath, encoding
    file_saved = Signal(str)  # filepath
    load_mode_changed = Signal(str)  # 'normal', 'sqlite', 'lazy'
    
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window # CsvEditorAppQtのインスタンス
        self.current_load_mode = 'normal'

    def _is_welcome_screen_active(self):
        """ウェルカム画面が表示されており、かつデータがロードされていない状態かを正確に判定するヘルパーメソッド"""
        # 🔥 重要：複数の条件で正確に判定
        welcome_visible = (
            hasattr(self.main_window, 'welcome_widget') and 
            self.main_window.welcome_widget.isVisible()
        )
        
        view_stack_hidden = (
            hasattr(self.main_window, 'view_stack') and 
            self.main_window.main_window_is_initialized and # ui_main_window.py の setupUi が完了していることを保証
            self.main_window.view_stack.isHidden()
        )
        
        # table_model の rowCount() が 0 であること
        no_data = self.main_window.table_model.rowCount() == 0
        
        result = welcome_visible and view_stack_hidden and no_data
        print(f"DEBUG: ウェルカム画面判定 - welcome_visible: {welcome_visible}, "
              f"view_stack_hidden: {view_stack_hidden}, no_data: {no_data} → {result}")
        
        return result
        
    def open_file(self, filepath=None):
        """CSVファイルを開く（ウェルカム画面考慮版）"""
        print("DEBUG: FileIOController.open_file called.")
        
        if not filepath:
            # ファイル選択ダイアログから選択
            filepath_tuple = QFileDialog.getOpenFileName(
                self.main_window,
                "CSVファイルを開く",
                "",
                "CSVファイル (*.csv);;テキストファイル (*.txt);;すべてのファイル (*.*)"
            )
            if not filepath_tuple[0]:
                return None
            filepath = filepath_tuple[0]
            
        print(f"DEBUG: ファイルを開く処理を開始: {filepath}")
        
        # 🔥 修正のポイント：ウェルカム画面の状態を正確に判定
        if self._is_welcome_screen_active():
            # ウェルカム画面の場合 → 既存ウィンドウで開く
            print("DEBUG: ウェルカム画面状態のため、既存ウィンドウで開きます")
            self._start_file_loading_process(filepath)
            return filepath
        else:
            # 既にデータがある場合 → 新しいウィンドウで開く
            print("DEBUG: 既存データがあるため、新しいウィンドウで開きます")
            
            # 🔥 改善：確認ダイアログでユーザビリティ向上
            # open_new_window_with_fileが filepathの存在チェックをしているため、ここでは不要
            reply = QMessageBox.question(
                self.main_window,
                "新しいウィンドウで開く",
                f"'{os.path.basename(filepath)}' を新しいウィンドウで開きます。\n"
                f"現在の作業内容は保持されます。\n\n"
                f"続行しますか？",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.Yes
            )
            
            if reply == QMessageBox.No:
                return None
            
            # 新しいウィンドウで開く
            self.main_window.open_new_window_with_file(filepath)
            return filepath
    
    # ファイル読み込みプロセスを開始するラッパーメソッド
    def _start_file_loading_process(self, filepath):
        # UIスレッドをブロックしないように、ここでの重い処理はAsyncDataManagerに委譲

        try:
            # 🔥 改善: ファイル存在確認とパーミッションエラーハンドリング
            if not os.path.exists(filepath):
                QMessageBox.critical(
                    self.main_window, 
                    "ファイルエラー", 
                    f"指定されたファイルが見つかりません:\n{filepath}"
                )
                self.main_window.view_controller.show_welcome_screen() # エラー時はウェルカム画面に戻す
                self.main_window.async_manager.cleanup_backend_requested.emit()
                return None
            
            try:
                # ファイルの読み込み可能性を確認（実際には読み込まない）
                with open(filepath, 'rb') as f:
                    pass
            except PermissionError:
                QMessageBox.critical(
                    self.main_window,
                    "アクセス権限エラー",
                    f"ファイルにアクセスする権限がありません:\n{filepath}"
                )
                self.main_window.view_controller.show_welcome_screen() # エラー時はウェルカム画面に戻す
                self.main_window.async_manager.cleanup_backend_requested.emit()
                return None
            except Exception as e:
                QMessageBox.critical(
                    self.main_window,
                    "予期しないエラー",
                    f"ファイル準備中にエラーが発生しました:\n{str(e)}\n\n詳細:\n{traceback.format_exc()}"
                )
                self.main_window.view_controller.show_welcome_screen() # エラー時はウェルカム画面に戻す
                self.main_window.async_manager.cleanup_backend_requested.emit()
                return None


            # エンコーディング検出の進捗通知
            self.main_window.file_loading_progress.emit(
                "エンコーディングを検出中...", 0, 3
            )
            encoding = self._detect_encoding(filepath)
            if not encoding:
                # エラーメッセージはUIスレッドで安全に表示
                QTimer.singleShot(0, lambda: QMessageBox.critical(self.main_window, "エラー",
                                   "ファイルのエンコーディングを検出できませんでした。"))
                QTimer.singleShot(0, self.main_window.view_controller.show_welcome_screen)
                self.main_window.file_loading_finished.emit()
                self.main_window.async_manager.cleanup_backend_requested.emit() # エラー時もクリーンアップ
                return None

            # ファイルサイズチェックの進捗通知
            self.main_window.file_loading_progress.emit(
                "ファイルサイズを確認中...", 1, 3
            )
            file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
            print(f"DEBUG: ファイルパス: {filepath}")
            print(f"DEBUG: ファイルサイズ: {file_size_mb:.2f} MB")
            
            # メモリ使用量とファイルサイズの事前チェック
            memory_ok, memory_msg = self._check_memory_feasibility(file_size_mb)

            selected_mode = 'normal' # デフォルトは通常モード

            if file_size_mb <= config.FILE_SIZE_MODE_SELECTION_THRESHOLD_MB:
                print(f"DEBUG: 小さいファイル({file_size_mb:.2f}MB)のため通常モードで直接読み込み")
                selected_mode = 'normal' # 小さいファイルは強制的に通常モード
            else:
                # 閾値を超えた場合、モード選択ダイアログを表示（UIスレッドで同期的に実行）
                mode_dialog = QDialog(self.main_window)
                mode_dialog.setWindowTitle("読み込みモード選択")
                layout = QVBoxLayout(mode_dialog)
                
                info_label = QLabel(f"ファイルサイズが {file_size_mb:.1f} MB と大きいため、\n"
                                   f"適切な読み込みモードを選択してください。")
                layout.addWidget(info_label)
                
                normal_radio = QRadioButton("通常モード (高速だがメモリ使用量大)")
                sqlite_radio = QRadioButton("SQLiteモード (推奨：メモリ効率的)")
                lazy_radio = QRadioButton("遅延読み込みモード (巨大ファイル用)")
                
                # ファイルサイズに応じたデフォルト選択
                if file_size_mb > 100 or not memory_ok:
                    sqlite_radio.setChecked(True)
                    if not memory_ok:
                        QMessageBox.warning(self.main_window, "メモリ不足",
                                            f"{memory_msg}\nSQLiteモードを推奨します。")
                else:
                    normal_radio.setChecked(True)
                    
                layout.addWidget(normal_radio)
                layout.addWidget(sqlite_radio)
                layout.addWidget(lazy_radio)
                
                button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
                button_box.accepted.connect(mode_dialog.accept)
                button_box.rejected.connect(mode_dialog.reject)
                layout.addWidget(button_box)
                
                if mode_dialog.exec() == QDialog.Accepted:
                    if sqlite_radio.isChecked():
                        selected_mode = 'sqlite'
                    elif lazy_radio.isChecked():
                        selected_mode = 'lazy'
                    else:
                        selected_mode = 'normal'
                else:
                    self.main_window.show_operation_status("ファイルの読み込みをキャンセルしました。", 3000)
                    if hasattr(self.main_window, 'progress_dialog') and self.main_window.progress_dialog is not None:
                        self.main_window._close_progress_dialog()
                    if hasattr(self.main_window, 'loading_overlay') and self.main_window.loading_overlay.isVisible():
                        self.main_window.loading_overlay.hide()
                    self.main_window.view_controller.show_welcome_screen()
                    self.main_window.async_manager.cleanup_backend_requested.emit() # エラー時もクリーンアップ
                    return None
            
            self.current_load_mode = selected_mode
            self.load_mode_changed.emit(self.current_load_mode)

            # AsyncDataManager経由でのファイル読み込みを開始
            self.main_window.async_manager.load_full_dataframe_async(
                filepath, encoding, selected_mode
            )
            
        except pd.errors.ParserError as e:
            print(f"ERROR: CSV解析エラー: {e}")
            QTimer.singleShot(0, lambda: QMessageBox.critical(
                self.main_window,
                "CSV解析エラー",
                f"CSVファイルの解析中にエラーが発生しました。\n\n"
                f"ファイルが正しいCSV形式であることを確認してください。\n\n"
                f"詳細: {str(e)[:200]}..."
            ))
            QTimer.singleShot(0, self.main_window.view_controller.show_welcome_screen)
            self.main_window.file_loading_finished.emit()
            self.main_window.async_manager.cleanup_backend_requested.emit()
        except MemoryError:
            print("ERROR: メモリ不足")
            QTimer.singleShot(0, lambda: QMessageBox.critical(
                self.main_window,
                "メモリ不足",
                "ファイルが大きすぎてメモリに読み込めません。\n"
                "より小さいファイルを使用するか、システムのメモリを増やしてください。"
            ))
            QTimer.singleShot(0, self.main_window.view_controller.show_welcome_screen)
            self.main_window.file_loading_finished.emit()
            self.main_window.async_manager.cleanup_backend_requested.emit()
        except Exception as e:
            print(f"ERROR: 予期しないファイル読み込みエラー: {e}")
            print(f"スタックトレース:\n{traceback.format_exc()}")
            if hasattr(self.main_window, 'progress_dialog') and self.main_window.progress_dialog is not None:
                self.main_window._close_progress_dialog()
            if hasattr(self.main_window, 'loading_overlay') and self.main_window.loading_overlay.isVisible():
                self.main_window.loading_overlay.hide()

            QMessageBox.critical(
                self.main_window,
                "ファイル読み込みエラー",
                f"ファイルの読み込み中に予期しないエラーが発生しました。\n\n{str(e)}"
            )
            QTimer.singleShot(0, self.main_window.view_controller.show_welcome_screen)
            self.main_window.file_loading_finished.emit()
            self.main_window.async_manager.cleanup_backend_requested.emit()
        finally:
            pass
        
        return None

    def _check_memory_feasibility(self, file_size_mb):
        """メモリ容量の事前チェック"""
        available_memory_mb = psutil.virtual_memory().available / (1024 * 1024)
        estimated_memory_mb = file_size_mb * 3  # CSV→DataFrame変換での膨張率
        
        if estimated_memory_mb > available_memory_mb * 0.7:
            return False, f"必要メモリ: {estimated_memory_mb:.1f}MB, 利用可能: {available_memory_mb:.1f}MB"
        return True, ""
    
    def save_file(self, filepath=None, is_save_as=True):
        """ファイルを保存"""
        if self.main_window.is_readonly_mode():
            self.main_window.show_operation_status("このモードでは上書き保存できません。「名前を付けて保存」を使用してください。", 3000, True)
            return False
            
        save_filepath = filepath
        
        if save_filepath is None or is_save_as:
            save_filepath = self._get_save_filepath()
            if not save_filepath:
                return False
        
        if self.main_window.table_model.rowCount() == 0:
            QMessageBox.warning(self.main_window, "保存不可", 
                              "データが空のため保存できません.")
            return False
        
        # エンコーディング選択
        encoding_dialog = EncodingSaveDialog(self.main_window)
        if encoding_dialog.exec() != QDialog.Accepted:
            return False
        save_encoding = encoding_dialog.result_encoding
        
        # フォーマット選択
        format_dialog = CSVSaveFormatDialog(self.main_window)
        if format_dialog.exec() != QDialog.Accepted:
            return False
        format_info = format_dialog.result
        
        # 実際の保存処理
        success = self._perform_save(save_filepath, save_encoding, format_info)
        
        if success:
            self.file_saved.emit(save_filepath)
            pass
            
        return success
    
    def save_as_with_dialog(self):
        """必ず名前を付けて保存ダイアログを表示"""
        print("DEBUG: FileIOController.save_as_with_dialog called")
        return self.save_file(is_save_as=True)
    
    def create_new_file(self):
        """新規CSVファイルを作成（ウェルカム画面考慮版）"""
        print("DEBUG: FileIOController.create_new_file called.")
        
        # 設定確認（ダイアログを表示するかどうか）
        show_dialog = self.main_window.settings_manager.get_show_new_file_dialog()
        if show_dialog:
            from dialogs import NewFileDialog
            dialog = NewFileDialog(self.main_window)
            if dialog.exec() != QDialog.Accepted:
                return
                
            result = dialog.get_result()
            columns = result['columns']
            initial_rows = result['initial_rows']
        else:
            columns = ['列1', '列2', '列3']
            initial_rows = 1
        
        # 新しいDataFrameを作成
        data = {}
        for col in columns:
            data[col] = [''] * initial_rows
        new_df = pd.DataFrame(data)
        
        print(f"DEBUG: 新規DataFrame作成 - shape: {new_df.shape}, columns: {list(new_df.columns)}")
        
        # 🔥 修正のポイント：ウェルカム画面の状態で分岐
        if self._is_welcome_screen_active():
            # ウェルカム画面の場合 → 既存ウィンドウで作成
            print("DEBUG: ウェルカム画面状態のため、既存ウィンドウで新規作成します")
            self._create_new_file_in_current_window(new_df)
        else:
            # 既にデータがある場合 → 新しいウィンドウで作成
            print("DEBUG: 既存データがあるため、新しいウィンドウで新規作成します")
            
            # 未保存の変更確認 (新しいウィンドウで開く場合のみ確認)
            if self.main_window.undo_manager.can_undo():
                reply = QMessageBox.question(
                    self.main_window,
                    "確認",
                    "現在のファイルに未保存の変更があります。\n"
                    "新しいウィンドウで新規作成しますか？",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.Yes
                )
                if reply == QMessageBox.No:
                    return
            
            # 新しいウィンドウで新規作成
            # open_new_window_with_new_data には `_cleanup_backend` や `undo_manager.clear()` は不要です。
            # 新しいウィンドウが初期化される際に、自動的にクリーンな状態が作られるためです。
            self.main_window.open_new_window_with_new_data(new_df)

    # 🔥 新規追加メソッド：現在のウィンドウで新規ファイルを作成する内部ヘルパー
    def _create_new_file_in_current_window(self, new_df):
        """現在のウィンドウのデータをリセットし、新規DataFrameで上書きする"""
        print("DEBUG: 現在のウィンドウで新規ファイルを作成中...")
        
        # 🔥 重要：完全なクリーンアップ
        self._complete_cleanup_for_new_file()
        
        # 🔥 重要：新しいDataFrameを設定
        self._setup_new_dataframe(new_df)
        
        # UIの更新
        self._update_ui_for_new_file(new_df)
        
        print("DEBUG: 現在のウィンドウでの新規ファイル作成完了")

    def _complete_cleanup_for_new_file(self):
        """新規ファイル作成のための完全なクリーンアップ"""
        print("DEBUG: 完全クリーンアップ開始")
        
        # バックエンドのクリーンアップ
        self.main_window._cleanup_backend()
        
        # Undo/Redo履歴のクリア
        self.main_window.undo_manager.clear()
        
        # 検索ハイライトのクリア
        if hasattr(self.main_window, 'search_controller'):
            self.main_window.search_controller.clear_search_highlight()
        
        # ソート情報のクリア
        self.main_window._clear_sort()
        
        # 🔥 重要：モデルのキャッシュをクリア
        if hasattr(self.main_window.table_model, '_row_cache'):
            self.main_window.table_model._row_cache.clear()
        if hasattr(self.main_window.table_model, '_cache_queue'):
            self.main_window.table_model._cache_queue.clear()
        
        # 🔥 重要：選択状態のクリア
        self.main_window.table_view.clearSelection()
        
        print("DEBUG: 完全クリーンアップ完了")

    def _setup_new_dataframe(self, new_df):
        """新しいDataFrameをセットアップ"""
        print("DEBUG: 新しいDataFrameのセットアップ開始")
        
        # メインウィンドウの状態を更新
        self.main_window._df = new_df
        self.main_window.header = list(new_df.columns)
        self.main_window.filepath = None
        self.main_window.encoding = 'shift_jis'
        self.main_window.performance_mode = False
        
        # 🔥 重要：モデルを完全にリセット
        self.main_window.table_model.beginResetModel()
        
        # 内部データを直接設定
        self.main_window.table_model._dataframe = new_df
        self.main_window.table_model._headers = list(new_df.columns)
        self.main_window.table_model._backend = None
        
        # 検索ハイライトをクリア
        self.main_window.table_model._search_highlight_indexes = set()
        self.main_window.table_model._current_search_index = QModelIndex()
        
        self.main_window.table_model.endResetModel()
        
        print(f"DEBUG: モデル設定完了 - rowCount: {self.main_window.table_model.rowCount()}, "
              f"columnCount: {self.main_window.table_model.columnCount()}")

    def _update_ui_for_new_file(self, new_df):
        """新規ファイル用にUIを更新"""
        print("DEBUG: UI更新開始")
        
        # 検索パネルのヘッダー更新
        if self.main_window.search_panel:
            self.main_window.search_panel.update_headers(self.main_window.header)
        
        # カードビューの再作成
        self.main_window.view_controller.recreate_card_view_fields()
        
        # ビューの表示
        self.main_window.view_controller.show_main_view()
        
        # ステータスバーの更新
        status_text = f"新規ファイル ({len(new_df):,}行, {len(new_df.columns)}列)"
        self.main_window.status_label.setText(status_text)
        self.main_window.setWindowTitle("高機能CSVエディタ (PySide6) - 無題")
        
        # 操作メッセージ
        self.main_window.show_operation_status("新規ファイルを作成しました")
        
        # UI状態の設定
        self.main_window._set_ui_state('normal')
        
        # 最初のセルを選択
        if self.main_window.table_model.rowCount() > 0 and self.main_window.table_model.columnCount() > 0:
            first_index = self.main_window.table_model.index(0, 0)
            self.main_window.table_view.setCurrentIndex(first_index)
            self.main_window.table_view.scrollTo(first_index)
            self.main_window.table_view.setFocus()
        
        # 🔥 重要：ビューを強制的に更新
        self.main_window.table_view.viewport().update()
        QApplication.processEvents()
        
        print("DEBUG: UI更新完了")
    
    def _load_file_data(self, filepath, encoding):
        """
        楽天CSV対応のファイル読み込み処理 (通常モード用)
        """
        read_options = config.CSV_READ_OPTIONS.copy()
        read_options['encoding'] = encoding
        
        try:
            with open(filepath, 'r', encoding=encoding) as f:
                first_line = f.readline()
                if first_line.count(',') > 100:
                    if read_options.get('engine') != 'python':
                        read_options['low_memory'] = False
        except Exception as e:
            print(f"WARNING: ファイルの先頭行読み込み中にエラー: {e}")
            pass
        
        df = pd.read_csv(filepath, **read_options)
        
        for col in df.columns:
            df[col] = df[col].fillna('').astype(str)
        
        print(f"DEBUG: CSVファイル読み込み成功: {df.shape}")
        return df
            
    def _detect_encoding(self, filepath):
        """エンコーディングを検出"""
        encodings_to_try = [
            'shift_jis',
            'cp932',
            'utf-8-sig',
            'utf-8',
            'euc-jp'
        ]
        
        for enc in encodings_to_try:
            try:
                print(f"DEBUG: エンコーディング '{enc}' を試行中...")
                with open(filepath, 'r', encoding=enc) as f:
                    f.read(1024)
                print(f"DEBUG: エンコーディング '{enc}' を使用")
                return enc
            except UnicodeDecodeError:
                print(f"DEBUG: エンコーディング '{enc}' でデコードエラー")
                continue
            except Exception as e:
                print(f"DEBUG: エンコーディング '{enc}' 試行中に予期せぬエラー: {e}")
                continue
        
        return None
    
    def _get_save_filepath(self):
        """保存先ファイルパスを取得"""
        initial_dir = ""
        suggested_filename = ""
        
        if self.main_window.filepath:
            if os.path.isabs(self.main_window.filepath):
                initial_dir = os.path.dirname(self.main_window.filepath)
                suggested_filename = os.path.basename(self.main_window.filepath)
            else:
                initial_dir = os.getcwd()
                suggested_filename = self.main_window.filepath
        else:
            initial_dir = os.path.expanduser("~")
            suggested_filename = "untitled.csv"
        
        initial_path = os.path.join(initial_dir, suggested_filename)
        
        filepath_tuple = QFileDialog.getSaveFileName(
            self.main_window,
            "名前を付けて保存",
            initial_path,
            "CSVファイル (*.csv);;テキストファイル (*.txt);;すべてのファイル (*.*)"
        )
        
        if not filepath_tuple[0]:
            return None
            
        filepath = filepath_tuple[0]
        
        if not filepath.lower().endswith(('.csv', '.txt')):
            filepath += '.csv'
            
        return filepath
    
    def _perform_save(self, filepath, encoding, format_info):
        """実際の保存処理（楽天市場CSV対応版）"""
        try:
            self.main_window._show_progress_dialog(
                f"「{os.path.basename(filepath)}」を保存中...", None
            )
            
            if self.main_window.db_backend:
                def progress_callback(current, total):
                    self.main_window._update_progress_dialog(
                        "ファイルを保存中...", current, total
                    )
                
                self.main_window.db_backend.export_to_csv(
                    filepath, encoding, format_info['quoting'],
                    progress_callback=progress_callback,
                    line_terminator=format_info['line_terminator']
                )
            else:
                df_to_save = self.main_window.table_model.get_dataframe()
                if df_to_save is None or df_to_save.empty:
                    self.main_window._close_progress_dialog()
                    QMessageBox.warning(self.main_window, "保存不可", 
                                      "データが空のため保存できません.")
                    return False
                
                df_to_save = self._prepare_dataframe_for_rakuten(df_to_save, format_info)
                
                df_to_save.to_csv(
                    filepath,
                    index=False,
                    encoding=encoding,
                    quoting=format_info['quoting'],
                    errors='replace',
                    lineterminator=format_info['line_terminator'],
                    escapechar=None if format_info.get('preserve_html', True) else '\\',
                    doublequote=True
                )
            
            self.main_window._close_progress_dialog()
            self.main_window.show_operation_status("ファイルを保存しました")
            
            self.main_window.undo_manager.clear()
            self.main_window.update_menu_states()

            return True
            
        except Exception as e:
            self.main_window._close_progress_dialog()
            self.main_window.show_operation_status(f"ファイル保存エラー: {e}", is_error=True)
            QMessageBox.critical(
                self.main_window,
                "保存エラー",
                f"ファイルの保存中にエラーが発生しました。\n{e}\n{traceback.format_exc()}"
            )
            return False

    def _prepare_dataframe_for_rakuten(self, df, format_info):
        """楽天市場向けのDataFrame準備"""
        print(f"DEBUG: 楽天市場向けDataFrame準備 - 入力: {df.shape}")
        
        if df is None or df.empty:
            print("WARNING: DataFrameが空です")
            return pd.DataFrame()
        
        df_copy = df.copy()
        
        for col in df_copy.columns:
            df_copy[col] = df_copy[col].fillna('').astype(str)
        
        if not format_info.get('preserve_html', True):
            print("DEBUG: HTMLタグをエスケープします。")
            for col in df_copy.columns:
                df_copy[col] = df_copy[col].apply(lambda x: re.sub(r'&(?!#?\w+;)', '&amp;', str(x)))
                df_copy[col] = df_copy[col].str.replace('<', '&lt;', regex=False)
                df_copy[col] = df_copy[col].str.replace('>', '&gt;', regex=False)
        else:
            print("DEBUG: HTMLタグはそのまま保持します。")
            
        if not format_info.get('preserve_linebreaks', True):
            print("DEBUG: セル内の改行を<br>タグに変換します。")
            for col in df_copy.columns:
                df_copy[col] = df_copy[col].str.replace('\r\n', '<br>', regex=False)
                df_copy[col] = df_copy[col].str.replace('\n', '<br>', regex=False)
                df_copy[col] = df_copy[col].str.replace('\r', '<br>', regex=False)
        else:
            print("DEBUG: セル内の改行はそのまま保持します。")
            
        print(f"DEBUG: 楽天市場向けDataFrame準備完了 - 出力: {df_copy.shape}")
        return df_copy