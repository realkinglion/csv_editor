@echo off
REM ===== main.py ランチャー =====
cd /d "%~dp0"       REM ← この .bat があるフォルダに移動
python main.py %*   REM ← 引数があればそのまま渡す
pause               REM ← 終了確認（要らなきゃ消してOK）
