# themes_qt.py
from PySide6.QtGui import QColor

class ThemeQt:
    """テーマの基底クラス (PySide6向け調整)"""
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
    
    # PySide6のQColorオブジェクト
    @property
    def PRIMARY_QCOLOR(self): return QColor(self.PRIMARY)
    @property
    def PRIMARY_HOVER_QCOLOR(self): return QColor(self.PRIMARY_HOVER)
    @property
    def PRIMARY_ACTIVE_QCOLOR(self): return QColor(self.PRIMARY_ACTIVE)
    @property
    def SUCCESS_QCOLOR(self): return QColor(self.SUCCESS)
    @property
    def WARNING_QCOLOR(self): return QColor(self.WARNING)
    @property
    def DANGER_QCOLOR(self): return QColor(self.DANGER)
    @property
    def INFO_QCOLOR(self): return QColor(self.INFO)
    @property
    def BG_LEVEL_0_QCOLOR(self): return QColor(self.BG_LEVEL_0)
    @property
    def BG_LEVEL_1_QCOLOR(self): return QColor(self.BG_LEVEL_1)
    @property
    def BG_LEVEL_2_QCOLOR(self): return QColor(self.BG_LEVEL_2)
    @property
    def BG_LEVEL_3_QCOLOR(self): return QColor(self.BG_LEVEL_3)
    @property
    def TEXT_PRIMARY_QCOLOR(self): return QColor(self.TEXT_PRIMARY)
    @property
    def TEXT_SECONDARY_QCOLOR(self): return QColor(self.TEXT_SECONDARY)
    @property
    def TEXT_MUTED_QCOLOR(self): return QColor(self.TEXT_MUTED)
    @property
    def CELL_SELECT_START_QCOLOR(self): return QColor(self.CELL_SELECT_START)
    @property
    def CELL_SELECT_END_QCOLOR(self): return QColor(self.CELL_SELECT_END)
    @property
    def CELL_SELECT_BORDER_QCOLOR(self): return QColor(self.CELL_SELECT_BORDER)


class DarkThemeQt(ThemeQt):
    """ダークテーマ (PySide6向け調整)"""
    BG_LEVEL_0 = "#1A1A1A"
    BG_LEVEL_1 = "#2D2D2D"
    BG_LEVEL_2 = "#3A3A3A"
    BG_LEVEL_3 = "#4A4A4A"
    
    TEXT_PRIMARY = "#E9ECEF"
    TEXT_SECONDARY = "#ADB5BD"
    
    CELL_SELECT_START = "#5BA0F2"
    CELL_SELECT_END = "#6BB0FF"