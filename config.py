# config.py

"""
アプリケーション全体で使用する定数や設定値を管理します。
"""

import csv

VIRTUAL_LIST_CHUNK_SIZE = 200

# =============================================================================
# フェーズ1 UI/UX改善により追加
# =============================================================================
from themes_qt import ThemeQt, DarkThemeQt

# UI設定
# アプリケーション全体でこのテーマオブジェクトを参照する
CURRENT_THEME = ThemeQt()
ENABLE_ANIMATIONS = True
ENABLE_SOUNDS = False

# 表示密度
class DisplayDensity:
    COMPACT = {"row_height": 25, "padding": 3, "font_size": 9}
    NORMAL = {"row_height": 30, "padding": 5, "font_size": 10}
    COMFORTABLE = {"row_height": 40, "padding": 8, "font_size": 11}

CURRENT_DENSITY = DisplayDensity.COMFORTABLE

# =============================================================================
# レベル1 高速化改善により追加
# =============================================================================
# パフォーマンスモード（大量データ時は自動的にON）
PERFORMANCE_MODE_THRESHOLD = 10000  # 10,000行以上で自動有効化

# 🔥 追加: 読み込みモード選択ダイアログを表示するファイルサイズ閾値 (MB)
FILE_SIZE_MODE_SELECTION_THRESHOLD_MB = 10 

# =============================================================================
# 楽天市場CSV対応のための追加設定
# =============================================================================
# 楽天市場向けデフォルト設定
RAKUTEN_DEFAULTS = {
    "encoding": "shift_jis",
    "quoting": csv.QUOTE_ALL,
    "line_terminator": "\r\n",
    "preserve_html": True,
    "preserve_linebreaks": True,
    "escape_char": None,
}

# CSVリーダーの設定（楽天市場対応）
CSV_READ_OPTIONS = {
    "dtype": str,
    "na_filter": False,
    "keep_default_na": False,
    "on_bad_lines": 'warn',
    "quoting": csv.QUOTE_MINIMAL,
    "escapechar": '\\',
    "encoding_errors": 'replace',
}

# 🔥 追加: ファイルを開く際の動作設定
OPEN_FILE_BEHAVIOR = {
    'always_new_window': True,     # 「ファイル」->「開く」メニューから常に新しいウィンドウで開くか
    'offset_new_windows': True,    # 新しいウィンドウの位置をずらして表示するか
    'max_child_windows': 10,       # 最大子ウィンドウ数 (この機能はまだ実装されていませんが、設定として追加)
}