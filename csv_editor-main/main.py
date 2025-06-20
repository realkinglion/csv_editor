# main.py

"""
アプリケーションを起動するためのエントリーポイントです。
このファイルを実行すると、CSVエディタが起動します。
"""

import tkinter as tk
from app_main import CsvEditorApp
import pandas as pd

if __name__ == "__main__":
    # 1. ルートウィンドウを先に作成
    root = tk.Tk()
    
    # 2. アプリケーション本体（Frame）を、ルートウィンドウを親として作成
    app = CsvEditorApp(root)
    
    # 3. アプリケーションフレームをウィンドウ全体に広がるように配置
    app.pack(fill="both", expand=True)

    # 4. ルートウィンドウのメインループを開始
    root.mainloop()