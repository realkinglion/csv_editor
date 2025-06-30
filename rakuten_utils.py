# rakuten_utils.py
import csv

class RakutenCSVUtils:
    """楽天市場CSV特化ユーティリティ"""
    
    # Shift_JIS問題文字のマッピング
    PROBLEMATIC_CHARS = {
        '①': '(1)', '②': '(2)', '③': '(3)', '④': '(4)', '⑤': '(5)',
        '⑥': '(6)', '⑦': '(7)', '⑧': '(8)', '⑨': '(9)', '⑩': '(10)',
        '髙': '高', '﨑': '崎', '德': '徳', '檜': '桧',
        '～': '〜',  # 波ダッシュ問題
        '－': 'ー',  # 全角ハイフン問題
        'Ⅰ': 'I', 'Ⅱ': 'II', 'Ⅲ': 'III', 'Ⅳ': 'IV', 'Ⅴ': 'V'
    }
    
    @classmethod
    def clean_for_shift_jis(cls, text):
        """Shift_JIS安全な文字列に変換"""
        if not text:
            return text
        
        result = str(text)
        for problem_char, safe_char in cls.PROBLEMATIC_CHARS.items():
            result = result.replace(problem_char, safe_char)
        
        return result
    
    @classmethod
    def validate_shift_jis_safe(cls, text):
        """Shift_JIS互換性をチェック"""
        try:
            text.encode('shift_jis')
            return True, ""
        except UnicodeEncodeError as e:
            char = text[e.start:e.end]
            return False, f"Shift_JIS非対応文字: '{char}' (位置: {e.start})"