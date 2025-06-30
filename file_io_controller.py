# file_io_controller.py

import os
import csv
import pandas as pd
import traceback
from PySide6.QtWidgets import QFileDialog, QMessageBox, QApplication, QProgressDialog, QDialog, QVBoxLayout, QRadioButton, QPushButton, QLabel, QDialogButtonBox, QInputDialog
from PySide6.QtCore import QObject, Signal, Qt, QTimer

import config
from dialogs import EncodingSaveDialog, CSVSaveFormatDialog, NewFileDialog
import re
import psutil
from threading import Thread


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
        
    def open_file(self, filepath=None):
        """CSVファイルを開く"""
        print("DEBUG: FileIOController.open_file called.")
        
        if not filepath:
            filepath_tuple = QFileDialog.getOpenFileName(
                self.main_window, # 親ウィジェットとしてmain_windowを指定
                "CSVファイルを開く",
                "",
                "CSVファイル (*.csv);;テキストファイル (*.txt);;すべてのファイル (*.*)"
            )
            if not filepath_tuple[0]:
                return None
            filepath = filepath_tuple[0]
        
        # 既存のバックエンドをクリーンアップ
        self.main_window._cleanup_backend()
        
        # AsyncDataManagerにファイル読み込みを委譲する前の初期進捗通知
        # ローディング開始を通知（UIスレッドで即座に実行）
        self.main_window.file_loading_started.emit()

        # ファイル読み込みプロセスを非同期で開始
        QTimer.singleShot(50, lambda: self._start_file_loading_process(filepath))
    
    # ファイル読み込みプロセスを開始するラッパーメソッド
    def _start_file_loading_process(self, filepath):
        # UIスレッドをブロックしないように、ここでの重い処理はAsyncDataManagerに委譲

        try:
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

            # 🔥 修正: 小さいファイルはモード選択ダイアログをスキップして直接非同期読み込みを開始
            if file_size_mb <= config.PERFORMANCE_MODE_THRESHOLD / 1000:
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
                if file_size_mb > 100 or not memory_ok: # 100MB以上またはメモリ不足の場合はSQLiteを推奨
                    sqlite_radio.setChecked(True)
                    if not memory_ok:
                        QMessageBox.warning(self.main_window, "メモリ不足",
                                            f"{memory_msg}\nSQLiteモードを推奨します。")
                else: # 閾値超～100MB未満は通常モードをデフォルト
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
                    # 🔥 修正: キャンセル時の処理
                    self.main_window.show_operation_status("ファイルの読み込みをキャンセルしました。", 3000)
                    # プログレスダイアログが表示されている場合は閉じる
                    if hasattr(self.main_window, 'progress_dialog') and self.main_window.progress_dialog is not None:
                        self.main_window._close_progress_dialog()
                    # ローディングオーバーレイも閉じる
                    if hasattr(self.main_window, 'loading_overlay') and self.main_window.loading_overlay.isVisible():
                        self.main_window.loading_overlay.hide()
                    self.main_window.view_controller.show_welcome_screen() # ウェルカム画面に戻る
                    self.main_window.async_manager.cleanup_backend_requested.emit() # キャンセル時もクリーンアップ
                    return None
            
            self.current_load_mode = selected_mode
            self.load_mode_changed.emit(self.current_load_mode) # シグナルを発行

            # AsyncDataManager経由でのファイル読み込みを開始
            self.main_window.async_manager.load_full_dataframe_async(
                filepath, encoding, selected_mode # selected_mode を渡す
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
            self.main_window.async_manager.cleanup_backend_requested.emit() # エラー時もクリーンアップ
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
            self.main_window.async_manager.cleanup_backend_requested.emit() # エラー時もクリーンアップ
        except Exception as e:
            print(f"ERROR: 予期しないファイル読み込みエラー: {e}")
            print(f"スタックトレース:\n{traceback.format_exc()}")
            # 🔥 修正: エラー時もプログレスダイアログとオーバーレイを閉じる
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
            self.main_window.async_manager.cleanup_backend_requested.emit() # エラー時もクリーンアップ
        finally:
            pass # AsyncDataManagerが終了を通知するため、ここでは特に処理は不要
        
        return None

    # --- 以下のメソッドは AsyncDataManager にロジックが統合されたため削除 ---
    # def _load_normal_file_with_progress(self, filepath, encoding):
    #     pass

    # def _finalize_file_load(self, data_object, filepath, encoding):
    #     pass

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
        
        # filepathがNoneの場合、またはis_save_asがTrueの場合は、ファイル選択ダイアログを表示
        if save_filepath is None or is_save_as:
            save_filepath = self._get_save_filepath()
            if not save_filepath:
                return False
        
        # データが空の場合は保存不可
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
    
    # ⭐ 新規作成機能を追加
    def create_new_file(self):
        """新規CSVファイルを作成"""
        print("DEBUG: FileIOController.create_new_file called.")
        
        # 既存のデータがある場合は確認
        if self.main_window.table_model.rowCount() > 0:
            if self.main_window.undo_manager.can_undo():
                reply = QMessageBox.question(
                    self.main_window,
                    "確認",
                    "未保存の変更があります。新規作成を続行しますか？",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No
                )
                if reply == QMessageBox.No:
                    return
        
        # 設定確認（ダイアログを表示するかどうか）
        show_dialog = self.main_window.settings_manager.get_show_new_file_dialog()
        
        if show_dialog:
            # 項目設定ダイアログを表示
            from dialogs import NewFileDialog
            dialog = NewFileDialog(self.main_window)
            if dialog.exec() != QDialog.Accepted:
                return
                
            result = dialog.get_result()
            columns = result['columns']
            initial_rows = result['initial_rows']
        else:
            # デフォルトの列構成
            columns = ['列1', '列2', '列3']
            initial_rows = 1
        
        # 新規DataFrameを作成
        data = {}
        for col in columns:
            data[col] = [''] * initial_rows
        
        df = pd.DataFrame(data)
        
        # バックエンドをクリーンアップ
        self.main_window._cleanup_backend()
        self.main_window.undo_manager.clear()
        
        # 新規データを設定
        self.main_window._df = df
        self.main_window.header = list(df.columns)
        self.main_window.filepath = None  # 未保存状態
        self.main_window.encoding = 'shift_jis'  # デフォルトエンコーディング
        self.main_window.performance_mode = False
        
        # モデルとUIを更新
        self.main_window.table_model.set_dataframe(df)
        
        if self.main_window.search_panel:
            self.main_window.search_panel.update_headers(self.main_window.header)
        
        self.main_window.view_controller.recreate_card_view_fields()
        self.main_window._clear_sort()
        self.main_window.view_controller.show_main_view()
        
        # ステータス更新
        status_text = f"新規ファイル ({len(df):,}行, {len(df.columns)}列)"
        self.main_window.status_label.setText(status_text)
        self.main_window.setWindowTitle("高機能CSVエディタ (PySide6) - 無題")
        
        self.main_window.show_operation_status("新規ファイルを作成しました")
        self.main_window._set_ui_state('normal')
        
        # 最初のセルを選択
        if self.main_window.table_model.rowCount() > 0 and self.main_window.table_model.columnCount() > 0:
            first_index = self.main_window.table_model.index(0, 0)
            self.main_window.table_view.setCurrentIndex(first_index)
            self.main_window.table_view.scrollTo(first_index)
            
    def _load_file_data(self, filepath, encoding):
        """
        楽天CSV対応のファイル読み込み処理 (通常モード用)
        このメソッドは AsyncDataManager にロジックが統合されたため、
        現在のコードベースではほぼ使用されないか、最終的に削除されるべきです。
        ここでは変更せず残します。
        """
        read_options = config.CSV_READ_OPTIONS.copy()
        read_options['encoding'] = encoding
        
        # 楽天市場CSVの特殊な処理
        try:
            # 巨大ファイル・多列対策
            with open(filepath, 'r', encoding=encoding) as f:
                first_line = f.readline()
                if first_line.count(',') > 100:  # 100列以上ある場合
                    # Python エンジンでは low_memory は使えないので除外
                    if read_options.get('engine') == 'python':
                        # Python エンジンの場合は low_memory を設定しない
                        pass
                    else:
                        # C エンジンの場合のみ low_memory を設定
                        read_options['low_memory'] = False
        except Exception as e:
            print(f"WARNING: ファイルの先頭行読み込み中にエラー: {e}")
            pass
        
        # CSVを読み込み
        df = pd.read_csv(filepath, **read_options)
        
        # 楽天CSV後処理：全て文字列として扱う
        for col in df.columns:
            df[col] = df[col].fillna('').astype(str)
        
        print(f"DEBUG: CSVファイル読み込み成功: {df.shape}")
        return df
            
    def _detect_encoding(self, filepath):
        """エンコーディングを検出"""
        # config.py の CSV_READ_OPTIONS['encoding'] を参照してデフォルトのエンコーディングリストを構築
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
                    f.read(1024) # ファイルの冒頭を少量読み込んでデコードを試みる
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
        
        # main_windowのfilepathから初期パスを決定
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
            self.main_window, # 親ウィジェット
            "名前を付けて保存",
            initial_path,
            "CSVファイル (*.csv);;テキストファイル (*.txt);;すべてのファイル (*.*)"
        )
        
        if not filepath_tuple[0]:
            return None
            
        filepath = filepath_tuple[0]
        
        # 拡張子がない場合は追加
        if not filepath.lower().endswith(('.csv', '.txt')):
            filepath += '.csv'
            
        return filepath
    
    def _perform_save(self, filepath, encoding, format_info):
        """実際の保存処理（楽天市場CSV対応版）"""
        try:
            # プログレスダイアログ表示
            self.main_window._show_progress_dialog(
                f"「{os.path.basename(filepath)}」を保存中...", None
            )
            
            if self.main_window.db_backend:
                # SQLiteバックエンドの場合
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
                # 通常のDataFrame保存（楽天市場対応）
                df_to_save = self.main_window.table_model.get_dataframe()
                if df_to_save is None or df_to_save.empty:
                    self.main_window._close_progress_dialog()
                    QMessageBox.warning(self.main_window, "保存不可", 
                                      "データが空のため保存できません.")
                    return False
                
                # 楽天市場向けのDataFrame準備
                df_to_save = self._prepare_dataframe_for_rakuten(df_to_save, format_info)
                
                # 楽天市場向けの保存オプション
                df_to_save.to_csv(
                    filepath,
                    index=False,
                    encoding=encoding,
                    quoting=format_info['quoting'],
                    errors='replace', # エンコーディングエラー時の挙動
                    lineterminator=format_info['line_terminator'],
                    escapechar=None if format_info.get('preserve_html', True) else '\\',
                    doublequote=True # クォート内のクォートは二重にする (CSV標準)
                )
            
            self.main_window._close_progress_dialog()
            self.main_window.show_operation_status("ファイルを保存しました")
            
            # 保存成功時にUndo履歴をクリア
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
        
        # 文字列型に統一（NaNを空文字列に変換）
        for col in df_copy.columns:
            df_copy[col] = df_copy[col].fillna('').astype(str)
        
        # HTMLタグの処理（preserve_htmlがFalseの場合のみエスケープ）
        if not format_info.get('preserve_html', True):
            print("DEBUG: HTMLタグをエスケープします。")
            for col in df_copy.columns:
                df_copy[col] = df_copy[col].apply(lambda x: re.sub(r'&(?!#?\w+;)', '&amp;', str(x)))
                df_copy[col] = df_copy[col].str.replace('<', '&lt;', regex=False)
                df_copy[col] = df_copy[col].str.replace('>', '&gt;', regex=False)
        else:
            print("DEBUG: HTMLタグはそのまま保持します。")
            
        # 改行の処理（preserve_linebreaksがFalseの場合のみ<br>タグに変換）
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