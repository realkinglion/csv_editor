# config.py

"""
アプリケーション全体で使用する定数や設定値を管理します。
"""

VIRTUAL_LIST_CHUNK_SIZE = 200

# =============================================================================
# フェーズ1 UI/UX改善により追加
# =============================================================================
# ▼▼▼ 修正箇所: PySide6向けテーマをインポート ▼▼▼
from themes_qt import ThemeQt, DarkThemeQt # 修正

# UI設定
# アプリケーション全体でこのテーマオブジェクトを参照する
CURRENT_THEME = ThemeQt()  # 将来的にテーマを切り替える際は、この行を変更する # 修正
ENABLE_ANIMATIONS = True
ENABLE_SOUNDS = False

# 表示密度
class DisplayDensity:
    COMPACT = {"row_height": 25, "padding": 3, "font_size": 9}
    NORMAL = {"row_height": 30, "padding": 5, "font_size": 10}
    COMFORTABLE = {"row_height": 40, "padding": 8, "font_size": 11}

# ▼▼▼ 表示密度を「快適」に変更 ▼▼▼
CURRENT_DENSITY = DisplayDensity.COMFORTABLE

# =============================================================================
# レベル1 高速化改善により追加
# =============================================================================
# パフォーマンスモード（大量データ時は自動的にON）
PERFORMANCE_MODE_THRESHOLD = 10000  # 10,000行以上で自動有効化