# settings_manager.py
import json
import os
from pathlib import Path
from PySide6.QtCore import QSettings, QSize # QSize を追加
from PySide6.QtWidgets import QApplication, QWidget, QToolBar # QToolBar を追加
from PySide6.QtCore import QByteArray 
import base64 
import traceback 


class SettingsManager:
    """アプリケーションの設定を管理するクラス"""
    
    def __init__(self):
        # QSettingsはレジストリ（Windows）やplist（macOS）に保存
        # Windowsの場合: C:\Users\ユーザー名\AppData\Roaming\CSVEditor\settings.ini
        # Macの場合: ~/Library/Preferences/com.csvEditor.plist
        self.settings = QSettings("CSVEditor", "RakutenCSVTool")
        
        # JSONファイルの保存場所
        # Windows: C:\Users\ユーザー名\.csv_editor\settings.json
        # Mac: ~/.csv_editor/settings.json
        self.json_path = Path.home() / ".csv_editor" / "settings.json"
        self.json_path.parent.mkdir(exist_ok=True)  # フォルダがなければ作成

    def save_window_settings(self, main_window: QWidget):
        """ウィンドウの位置とサイズを保存"""
        self.settings.setValue("window/x", main_window.x())
        self.settings.setValue("window/y", main_window.y())
        self.settings.setValue("window/width", main_window.width())
        self.settings.setValue("window/height", main_window.height())
        self.settings.setValue("window/maximized", main_window.isMaximized())
        
        print(f"ウィンドウ設定を保存しました: {main_window.width()}x{main_window.height()}")
        # トラブルシューティング提案のデバッグコードを追加
        print(f"QSettings ファイルパス: {self.settings.fileName()}")
        
    def load_window_settings(self, main_window: QWidget):
        """保存されたウィンドウ設定を復元"""
        x = self.settings.value("window/x", 100, type=int)
        y = self.settings.value("window/y", 100, type=int)
        width = self.settings.value("window/width", 1280, type=int)
        height = self.settings.value("window/height", 720, type=int)
        maximized = self.settings.value("window/maximized", False, type=bool)
        
        # トラブルシューティング提案のコードを追加: 画面内に収まるように調整
        screen = QApplication.primaryScreen().geometry()
        
        # ウィンドウが画面外なら中央に配置
        if x < screen.left() or y < screen.top() or \
           x + width > screen.right() or y + height > screen.bottom():
            x = (screen.width() - width) // 2
            y = (screen.height() - height) // 2
            
        main_window.move(x, y)
        main_window.resize(width, height)
        
        if maximized:
            main_window.showMaximized()
            
        print(f"ウィンドウ設定を復元しました: {width}x{height}")

    def save_csv_settings(self, encoding: str, quote_all: bool, preserve_html: bool, preserve_linebreaks: bool):
        """CSV保存設定を記憶"""
        settings_dict = {
            "encoding": encoding,
            "quote_all": quote_all,
            "preserve_html": preserve_html,
            "preserve_linebreaks": preserve_linebreaks
        }
        
        try:
            with open(self.json_path, 'w', encoding='utf-8') as f:
                json.dump(settings_dict, f, ensure_ascii=False, indent=2)
            print("CSV設定を保存しました")
        except Exception as e:
            print(f"設定の保存に失敗: {e}")
            import traceback
            traceback.print_exc()
            
    def load_csv_settings(self):
        """保存されたCSV設定を読み込む"""
        # デフォルト値（楽天市場向け）
        default = {
            "encoding": "shift_jis",
            "quote_all": True,
            "preserve_html": True,
            "preserve_linebreaks": True
        }
        
        try:
            if self.json_path.exists():
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    saved = json.load(f)
                    # JSONにないキーを考慮し、savedから取得できるキーのみ更新
                    for key in default:
                        if key in saved:
                            default[key] = saved[key]
                print("CSV設定を読み込みました")
        except Exception as e:
            print(f"設定の読み込みに失敗: {e}")
            import traceback
            traceback.print_exc()
            
        return default

    def reset_all_settings(self):
        """すべての設定を初期値に戻す"""
        self.settings.clear()
        if self.json_path.exists():
            self.json_path.unlink()  # JSONファイルを削除
        print("すべての設定をリセットしました")

    # ⭐ 新規追加: 検索履歴の保存・取得・クリア機能
    def save_search_history(self, search_term: str):
        """検索履歴を保存（最大20件）"""
        if not search_term.strip():  # 空の検索語は保存しない
            return
            
        # 現在の履歴を読み込む
        history = self.settings.value("search/history", [], type=list)
        
        # 重複を削除（既に履歴にある場合は先頭に移動）
        if search_term in history:
            history.remove(search_term)
        
        # 先頭に追加
        history.insert(0, search_term)
        
        # 最大20件に制限
        history = history[:20]
        
        # 保存
        self.settings.setValue("search/history", history)
        print(f"検索履歴を保存: '{search_term}'")
        
    def get_search_history(self) -> list:
        """検索履歴を取得"""
        return self.settings.value("search/history", [], type=list)
        
    def clear_search_history(self):
        """検索履歴をクリア"""
        self.settings.remove("search/history")
        print("検索履歴をクリアしました")

    # ⭐ 新規追加: 新規作成時にダイアログを表示するかどうかの設定
    def get_show_new_file_dialog(self):
        """新規作成時にダイアログを表示するかどうかの設定を取得"""
        return self.settings.value("behavior/show_new_file_dialog", True, type=bool)
        
    def set_show_new_file_dialog(self, show):
        """新規作成時にダイアログを表示するかどうかの設定を保存"""
        self.settings.setValue("behavior/show_new_file_dialog", show)

    def save_toolbar_state(self, main_window):
        """ツールバー状態の包括的保存"""
        try:
            # Qt標準のstate保存
            toolbar_state = main_window.saveState()
            encoded_state = base64.b64encode(toolbar_state.data()).decode('ascii') 
            self.settings.setValue("ui/toolbar_state", encoded_state)
            
            # 追加：カスタム設定の保存
            # 🔥 QToolBarが正しくインポートされているか確認
            toolbar = main_window.findChild(QToolBar, "MainToolbar") #
            if toolbar:
                custom_settings = {
                    'button_style': int(toolbar.toolButtonStyle()),
                    'icon_size': [toolbar.iconSize().width(), toolbar.iconSize().height()],
                    'floating': toolbar.isFloating(),
                    'area': int(main_window.toolBarArea(toolbar))
                }
                
                self.settings.setValue("ui/toolbar_custom", json.dumps(custom_settings))
                
            print("ツールバー状態を保存しました")
            
        except Exception as e:
            print(f"ツールバー状態保存エラー: {e}")
            traceback.print_exc()

    def load_toolbar_state(self, main_window):
        """ツールバー状態の完全復元"""
        try:
            # Qt標準のstate復元
            encoded_state = self.settings.value("ui/toolbar_state", "", type=str)
            if encoded_state:
                # QByteArray.fromRawData() は Python の bytes-like object を受け取る
                toolbar_state = QByteArray(base64.b64decode(encoded_state.encode('ascii')))
                main_window.restoreState(toolbar_state)
            
            # 追加：カスタム設定の復元（save_toolbar_stateで保存した内容）
            custom_settings_str = self.settings.value("ui/toolbar_custom", "", type=str)
            if custom_settings_str:
                custom_settings = json.loads(custom_settings_str)
                # 🔥 QToolBarが正しくインポートされているか確認
                toolbar = main_window.findChild(QToolBar, "MainToolbar") #
                if toolbar:
                    toolbar.setToolButtonStyle(custom_settings.get('button_style', toolbar.toolButtonStyle()))
                    icon_size_list = custom_settings.get('icon_size')
                    # 🔥 QSizeをインポートしていないとここでエラーになるので注意
                    if icon_size_list and len(icon_size_list) == 2:
                        toolbar.setIconSize(QSize(icon_size_list[0], icon_size_list[1])) #
                    
                    is_floating = custom_settings.get('floating')
                    if is_floating is not None: 
                        toolbar.setFloating(is_floating)
            
            print("ツールバー状態を復元しました")
            
        except Exception as e:
            print(f"ツールバー状態復元エラー: {e}")
            traceback.print_exc()
            # エラー時は初期状態を維持