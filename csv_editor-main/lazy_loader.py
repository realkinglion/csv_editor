# lazy_loader.py

import pandas as pd
import os
import tkinter as tk
from tkinter import ttk
import csv
from io import StringIO

class LazyCSVLoader:
    """必要な部分だけを読み込むローダー（UI対応版）"""
    
    def __init__(self, filepath, encoding='utf-8', theme=None):
        self.filepath = filepath
        self.encoding = encoding
        self.theme = theme
        self.header = None
        self.delimiter = ','  # デフォルトの区切り文字
        self.total_rows = 0
        self._cache = {}
        self._row_index = []  # 各行の開始位置を記録
        
        # メタデータを初期化
        self._init_metadata()
    
    def _init_metadata(self):
        """ヘッダーと行インデックスを構築（区切り文字の自動検出を強化）"""
        try:
            with open(self.filepath, 'r', encoding=self.encoding, errors='ignore') as f:
                # Snifferのために十分なサンプルを読む
                sample = f.read(4096)
                f.seek(0)
                
                # csv.Snifferで区切り文字を検出
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=[',', '\t', ';', '|'])
                    self.delimiter = dialect.delimiter
                except csv.Error:
                    # Snifferが失敗した場合、一般的な区切り文字を試す
                    if '\t' in sample:
                        self.delimiter = '\t'
                    elif ';' in sample:
                        self.delimiter = ';'
                    else:
                        self.delimiter = ',' # 最終的なフォールバック
            
            # 検出した区切り文字でヘッダーを読む
            self.header = pd.read_csv(
                self.filepath, 
                nrows=0, 
                encoding=self.encoding,
                sep=self.delimiter
            ).columns.tolist()
            
            # 行の位置をインデックス化
            self._build_row_index()

        except Exception as e:
            print(f"Error initializing metadata: {e}")
            self.header = [f"Column {i+1}" for i in range(10)]
            self.total_rows = 0

    # ▼▼▼ 修正箇所：引用符内の改行を正しく処理するロジックに全面的に書き換え ▼▼▼
    def _build_row_index(self):
        """
        各行のファイル内開始位置を記録する。
        引用符で囲まれたフィールド内の改行を正しく1行の一部として扱う。
        """
        self._row_index = []
        try:
            with open(self.filepath, 'rb') as f:
                # ヘッダー行をスキップ
                f.readline()
                
                # ファイルの終わりまでループ
                while True:
                    # 現在位置をレコードの開始位置として記録
                    record_start_pos = f.tell()
                    
                    # 最初の行を読み込む
                    line = f.readline()
                    
                    # ファイルの終端に達したらループを抜ける
                    if not line:
                        break
                    
                    # 引用符の数を数え、奇数である間（＝フィールドが閉じられていない間）は次の行を読み込み続ける
                    while line.count(b'"') % 2 != 0:
                        next_line = f.readline()
                        if not next_line:
                            break # ファイル末尾で引用符が閉じていない場合
                        line += next_line
                    
                    # 引用符が正しく閉じられた1レコードの開始位置をインデックスに追加
                    self._row_index.append(record_start_pos)

            self.total_rows = len(self._row_index)
        except Exception as e:
            print(f"Error building row index: {e}")
            self.total_rows = 0

    def get_rows(self, start, end):
        """指定範囲の行を高速取得（列数不一致に対応）"""
        if start >= self.total_rows:
            return pd.DataFrame(columns=self.header)

        cache_key = f"{start}_{end}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        rows_data = []
        actual_end = min(end, self.total_rows)

        try:
            with open(self.filepath, 'r', encoding=self.encoding, errors='ignore') as f:
                for i in range(start, actual_end):
                    f.seek(self._row_index[i])
                    
                    # _build_row_index と同様のロジックで1レコードを完全に読み込む
                    line = f.readline()
                    while line.count('"') % 2 != 0:
                        next_line = f.readline()
                        if not next_line: break
                        line += next_line
                    rows_data.append(line.strip())

            if not rows_data:
                return pd.DataFrame(columns=self.header)

            csv_data = StringIO('\n'.join(rows_data))
            
            # Python標準のcsvリーダーで安全にパース
            reader = csv.reader(csv_data, delimiter=self.delimiter, quotechar='"')
            parsed_rows = []
            expected_cols = len(self.header)
            for row in reader:
                # 列数が足りない場合は空文字で埋め、多い場合は切り捨てる
                row.extend([''] * (expected_cols - len(row)))
                parsed_rows.append(row[:expected_cols])
            
            # パース済みのリストからDataFrameを作成
            df = pd.DataFrame(parsed_rows, columns=self.header)
            
            df.index = range(start, start + len(df))
            self._update_cache(cache_key, df)
            return df

        except Exception as e:
            print(f"Error getting rows {start}-{end}: {e}")
            return pd.DataFrame(columns=self.header)
    # ▲▲▲ 修正箇所 ▲▲▲

    def _update_cache(self, key, data):
        """LRUキャッシュの更新"""
        max_cache_size = 20
        if len(self._cache) >= max_cache_size:
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]
        self._cache[key] = data

    def search_in_file(self, search_term, progress_callback=None):
        """ファイル内を直接検索（メモリ効率的）"""
        matched_indices = []
        search_term_lower = search_term.lower()

        try:
            with open(self.filepath, 'r', encoding=self.encoding, errors='ignore') as f:
                f.readline() # ヘッダーをスキップ
                
                current_row_index = 0
                while current_row_index < self.total_rows:
                    f.seek(self._row_index[current_row_index])
                    
                    # 1レコードを完全に読み込む
                    line = f.readline()
                    while line.count('"') % 2 != 0:
                        next_line = f.readline()
                        if not next_line: break
                        line += next_line

                    if search_term_lower in line.lower():
                        matched_indices.append(current_row_index)
                    
                    if progress_callback and current_row_index % 1000 == 0:
                        if not progress_callback(current_row_index, self.total_rows):
                            break
                    
                    current_row_index += 1
            return matched_indices
        except Exception as e:
            print(f"Error searching in file: {e}")
            return []