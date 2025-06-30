# config.py

"""
アプリケーション全体で使用する定数や設定値を管理します。
"""

import csv # 追加: csv.QUOTE_ALLなどの定数を使用するため

VIRTUAL_LIST_CHUNK_SIZE = 200

# =============================================================================
# フェーズ1 UI/UX改善により追加
# =============================================================================
# ▼▼▼ 修正箇所: PySide6向けテーマをインポート ▼▼▼
from themes_qt import ThemeQt, DarkThemeQt # 修正

# UI設定
# アプリケーション全体でこのテーマオブジェクトを参照する
CURRENT_THEME = ThemeQt()  # 将来的にテーマを切り替える際は、この行を変更する
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

# =============================================================================
# 楽天市場CSV対応のための追加設定
# =============================================================================
# 楽天市場向けデフォルト設定
RAKUTEN_DEFAULTS = {
    "encoding": "shift_jis",  # 楽天市場標準エンコーディング
    "quoting": csv.QUOTE_ALL,  # 全フィールドをクォート
    "line_terminator": "\r\n", # Windows改行コード
    "preserve_html": True,     # HTMLタグを保持
    "preserve_linebreaks": True, # セル内改行を保持
    "escape_char": None,       # エスケープ文字なし（to_csvのデフォルト挙動に依存）
    # "max_field_size": 131072,  # 楽天市場の最大フィールドサイズ（現状to_csvでは直接制御不可）
}

# CSVリーダーの設定（楽天市場対応）
CSV_READ_OPTIONS = {
    "dtype": str,             # 全て文字列として読み込み
    "na_filter": False,        # 空文字をNaNに変換しない
    "keep_default_na": False,  # デフォルトのNA値を使わない
    "on_bad_lines": 'warn',    # エラー行は警告
    # "engine": 'python',        # より柔軟なパーサー 🔥 ここを削除またはコメントアウト
    "quoting": csv.QUOTE_MINIMAL, # 読み込み時は最小限のクォートを想定
    "escapechar": '\\',        # 読み込み時にバックスラッシュをエスケープ文字と想定
    "encoding_errors": 'replace', # エンコーディングエラーは置換
}