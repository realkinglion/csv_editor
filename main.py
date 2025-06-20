# main.py

"""
アプリケーションを起動するためのエントリーポイントです。
このファイルを実行すると、CSVエディタが起動します。
"""

from app_main import CsvEditorApp
import pandas as pd

if __name__ == "__main__":
    app = CsvEditorApp()
    app.mainloop()