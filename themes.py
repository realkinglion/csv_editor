# themes.py

"""
アプリケーション全体のテーマ管理
"""

class Theme:
    """テーマの基底クラス"""
    # 色彩システム
    PRIMARY = "#2E86C1"
    PRIMARY_HOVER = "#5BA0F2"
    PRIMARY_ACTIVE = "#1E5F8E"
    
    SUCCESS = "#27AE60"
    WARNING = "#F39C12"
    DANGER = "#E74C3C"
    INFO = "#3498DB"
    
    # 背景レベル
    BG_LEVEL_0 = "#FFFFFF"  # コンテンツ背景
    BG_LEVEL_1 = "#F8F9FA"  # カード背景、ウィンドウ背景
    BG_LEVEL_2 = "#E9ECEF"  # ヘッダー背景、無効化された要素
    BG_LEVEL_3 = "#DEE2E6"  # 境界線
    
    # テキスト
    TEXT_PRIMARY = "#212529"
    TEXT_SECONDARY = "#6C757D"
    TEXT_MUTED = "#ADB5BD"
    
    # セル選択色
    CELL_SELECT_START = "#4A90E2"
    CELL_SELECT_END = "#5BA0F2" # 将来的なグラデーション対応用
    CELL_SELECT_BORDER = "#2E6DA4"
    
    # アニメーション設定
    TRANSITION_FAST = 150  # ms
    TRANSITION_NORMAL = 300
    TRANSITION_SLOW = 500
    
    # 影 (Tkinterでは直接サポートされないため将来的な拡張用)
    SHADOW_SM = "0 1px 2px rgba(0,0,0,0.05)"
    SHADOW_MD = "0 2px 4px rgba(0,0,0,0.1)"
    SHADOW_LG = "0 4px 8px rgba(0,0,0,0.15)"

class DarkTheme(Theme):
    """ダークテーマ"""
    BG_LEVEL_0 = "#1A1A1A"
    BG_LEVEL_1 = "#2D2D2D"
    BG_LEVEL_2 = "#3A3A3A"
    BG_LEVEL_3 = "#4A4A4A"
    
    TEXT_PRIMARY = "#E9ECEF"
    TEXT_SECONDARY = "#ADB5BD"
    
    CELL_SELECT_START = "#5BA0F2"
    CELL_SELECT_END = "#6BB0FF"