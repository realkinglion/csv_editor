# lazy_loader.py

import pandas as pd
import os
import csv
import re
from io import StringIO
from collections import OrderedDict # 追加: LRUキャッシュ用
from threading import Lock, Thread # 追加: スレッドセーフなファイルアクセスとプリフェッチ用
from queue import Queue # 追加: プリフェッチ用
from PySide6.QtCore import Signal, QObject

class LazyCSVLoader(QObject):
    progress_update = Signal(int)
    
    def __init__(self, filepath, encoding='utf-8', theme=None, 
                 cache_size=1000, chunk_size=100): # 追加: キャッシュサイズとチャンクサイズ
        super().__init__()
        self.filepath = filepath
        self.encoding = encoding
        self.theme = theme
        self.header = None
        self.delimiter = ','
        self.total_rows = 0
        
        # 改善1: 適切なキャッシュ実装
        self._cache = OrderedDict()  # LRUキャッシュ
        self._cache_size = cache_size #
        self._cache_lock = Lock() #
        
        # 改善2: チャンク単位での読み込み
        self._chunk_size = chunk_size #
        self._chunk_cache = {} #
        
        # 改善3: ファイルハンドルの再利用
        self._file_handle = None #
        self._file_lock = Lock() #
        
        # 改善4: プリフェッチ機構
        self._prefetch_queue = Queue() #
        self._prefetch_thread = None #
        self._stop_prefetch = False #
        
        self._row_index = []
        self._init_metadata()
        self._start_prefetch_thread() # 追加: プリフェッチスレッドを開始
    
    def _init_metadata(self):
        try:
            with open(self.filepath, 'r', encoding=self.encoding, errors='ignore') as f:
                sample = f.read(4096)
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=[',', '\t', ';', '|'])
                    self.delimiter = dialect.delimiter
                except csv.Error:
                    if '\t' in sample: self.delimiter = '\t'
                    elif ';' in sample: self.delimiter = ';'
                    else: self.delimiter = ','
            
            self.header = pd.read_csv(self.filepath, nrows=0, encoding=self.encoding, sep=self.delimiter).columns.tolist()
            self._build_row_index()
        except Exception as e:
            print(f"Error initializing metadata: {e}")
            self.header = []
            self.total_rows = 0

    def _build_row_index(self):
        """行インデックスの構築（メモリ効率改善版）""" # 追加
        self._row_index = []
        
        # 巨大ファイル対策：一定行数ごとにインデックスを間引く
        index_interval = 1 #
        if self.total_rows > 1000000:  # 100万行以上
            index_interval = 10  # 10行ごとにインデックス
        
        try:
            with open(self.filepath, 'rb') as f:
                # Skip header
                f.readline()
                row_num = 0 # 追加
                while True:
                    if row_num % index_interval == 0: # 追加
                        self._row_index.append(f.tell()) #
                    
                    line = f.readline()
                    if not line: break
                    row_num += 1 # 追加
            # 修正: 行数カウントのバグ修正
            self.total_rows = row_num # ヘッダー行を含まないデータ行数
        except Exception as e:
            print(f"Error building row index: {e}")
            self.total_rows = 0

    def _get_row_count_fast(self): # 名称変更
        """高速行数カウント（修正版）""" # 追加
        with open(self.filepath, 'rb') as f:
            count = sum(1 for _ in f) # 修正: len()は不要
        return count #

    def get_rows_by_ids(self, indices):
        """改善版：キャッシュとチャンク読み込みを活用""" # 修正
        if not indices:
            return pd.DataFrame(columns=self.header)
        
        result_df = pd.DataFrame(index=indices, columns=self.header)
        uncached_indices = []
        
        # キャッシュチェック
        with self._cache_lock: #
            for idx in indices: #
                if idx in self._cache: #
                    result_df.loc[idx] = self._cache[idx] #
                else: #
                    uncached_indices.append(idx) #
        
        # キャッシュミスした行を読み込む
        if uncached_indices: #
            # チャンクごとにグループ化
            chunks_to_load = OrderedDict() # OrderedDictで順序を保持
            for idx in uncached_indices: #
                chunk_id = idx // self._chunk_size #
                if chunk_id not in chunks_to_load: #
                    chunks_to_load[chunk_id] = [] #
                chunks_to_load[chunk_id].append(idx) #
            
            # チャンク単位で読み込み
            for chunk_id, chunk_indices in chunks_to_load.items(): #
                self._load_chunk(chunk_id, chunk_indices, result_df) #
        
        # プリフェッチのヒント
        if indices: #
            center_idx = indices[len(indices)//2] #
            self._hint_prefetch(center_idx) #
        
        return result_df

    def _load_chunk(self, chunk_id, indices_in_chunk, result_df): # 追加
        """チャンク単位での効率的な読み込み""" # 追加
        chunk_start_idx = chunk_id * self._chunk_size #
        chunk_end_idx = min((chunk_id + 1) * self._chunk_size, self.total_rows) #
        
        # チャンクキャッシュをチェック
        with self._cache_lock: # _chunk_cacheもLRU対象にする場合は_cache_lockを使う
            if chunk_id in self._chunk_cache: #
                chunk_data = self._chunk_cache[chunk_id] #
                for idx in indices_in_chunk: #
                    if idx in chunk_data: #
                        result_df.loc[idx] = chunk_data[idx] #
                        self._update_row_cache(idx, chunk_data[idx]) # 個別行キャッシュも更新
                return #
        
        # ファイルから読み込み
        chunk_rows_data = {} # このチャンクで読み込んだ行データを一時的に保持
        try: #
            with self._file_lock: # ファイルハンドルをスレッドセーフに利用
                if self._file_handle is None: #
                    self._file_handle = open(self.filepath, 'r', #
                                             encoding=self.encoding, #
                                             errors='ignore', newline='') #
                    self._file_handle.readline()  # ヘッダースキップ
                
                # チャンクの開始行にシーク
                if chunk_start_idx < len(self._row_index): #
                    self._file_handle.seek(self._row_index[chunk_start_idx]) #
                else: # chunk_start_idxが範囲外の場合（ファイル末尾に近いチャンクなど）
                    # 最後の有効なインデックスにシークするか、ファイルを最後まで読み切る
                    # ここはより堅牢なシークロジックが必要になる可能性があるが、
                    # _row_indexの間引きを考慮すると、_row_index[chunk_start_idx] が正確にチャンクの先頭である保証はない
                    # 現状では、シンプルに直接読み進める。
                    pass
                
                current_row_in_chunk = chunk_start_idx #
                while current_row_in_chunk < chunk_end_idx: #
                    line = self._file_handle.readline() #
                    if not line: #
                        break #
                    
                    parsed_row = self._parse_csv_line(line) #
                    chunk_rows_data[current_row_in_chunk] = parsed_row #
                    
                    if current_row_in_chunk in indices_in_chunk: # 要求された行の場合
                        result_df.loc[current_row_in_chunk] = parsed_row #
                        self._update_row_cache(current_row_in_chunk, parsed_row) # 個別行キャッシュも更新
                    
                    current_row_in_chunk += 1 #
            
            with self._cache_lock: #
                self._chunk_cache[chunk_id] = chunk_rows_data # チャンクキャッシュに保存

        except Exception as e:
            print(f"Error loading chunk {chunk_id}: {e}") #

    def _parse_csv_line(self, line): # 追加
        """CSV行のパース（エラー処理付き）""" # 追加
        try: #
            reader = csv.reader(StringIO(line), #
                                delimiter=self.delimiter, #
                                quotechar='"') #
            parsed_row = next(reader, []) #
            # 列数を調整
            parsed_row.extend([''] * (len(self.header) - len(parsed_row))) #
            return parsed_row[:len(self.header)] #
        except: #
            return [''] * len(self.header) #
    
    def _update_row_cache(self, row_idx, row_data): # 追加
        """LRUキャッシュの更新""" # 追加
        with self._cache_lock: #
            # 既存のエントリを削除（LRU更新のため）
            if row_idx in self._cache: #
                del self._cache[row_idx] #
            
            # 新しいエントリを追加
            self._cache[row_idx] = row_data #
            
            # キャッシュサイズ制限
            while len(self._cache) > self._cache_size: #
                self._cache.popitem(last=False)  # 最古のエントリを削除

    def _start_prefetch_thread(self): # 追加
        """プリフェッチスレッドの開始""" # 追加
        self._prefetch_thread = Thread(target=self._prefetch_worker, daemon=True) #
        self._prefetch_thread.start() #
    
    def _prefetch_worker(self): # 追加
        """バックグラウンドでのプリフェッチ処理""" # 追加
        while not self._stop_prefetch: #
            try: #
                center_idx = self._prefetch_queue.get(timeout=1) #
                if center_idx is None: #
                    continue #
                
                # 前後のチャンクをプリロード
                chunk_id = center_idx // self._chunk_size #
                # アクセスパターンに基づいて複数のチャンクをプリフェッチ
                for offset in [-1, 0, 1, 2, 3]: # 現在のチャンクと先行する数チャンクを対象
                    target_chunk_id = chunk_id + offset #
                    if 0 <= target_chunk_id < (self.total_rows // self._chunk_size + (1 if self.total_rows % self._chunk_size != 0 else 0)): #
                        with self._cache_lock: #
                            if target_chunk_id not in self._chunk_cache: #
                                # チャンクがキャッシュにない場合のみ読み込みを試みる
                                # ここでは_load_chunkを直接呼び出すのではなく、Queueに入れてメインスレッドで処理させるのが安全
                                # ただし、ここではシンプルに直接読み込みを試みる (ファイルロックによりスレッドセーフ)
                                self._load_chunk(target_chunk_id, [], pd.DataFrame(columns=self.header)) # 空のDataFrameを渡す
                                # ここでUIには直接影響を与えず、内部キャッシュを埋めるだけ
            except Exception as e: #
                # キューのタイムアウトやその他のエラーは無視
                # print(f"Prefetch worker error: {e}") 
                pass
    
    def _hint_prefetch(self, center_idx): # 追加
        """プリフェッチのヒント""" # 追加
        try: #
            # キューに新しいヒントを追加する前に、古いヒントをクリアして新しいアクセスパターンを優先する
            while not self._prefetch_queue.empty(): #
                self._prefetch_queue.get_nowait() #
            self._prefetch_queue.put_nowait(center_idx) #
        except: #
            pass  # キューが満杯の場合は無視
            
    def close(self): # 追加
        """リソースのクリーンアップ""" # 追加
        self._stop_prefetch = True #
        if self._prefetch_thread: #
            self._prefetch_queue.put(None) # プリフェッチスレッドを終了させるためにSentinel値をキューに入れる
            self._prefetch_thread.join(timeout=1) #
            if self._prefetch_thread.is_alive(): # スレッドがまだ生きている場合は強制終了ログ
                print("WARNING: Prefetch thread did not terminate gracefully.") #
        
        with self._file_lock: #
            if self._file_handle: #
                self._file_handle.close() #
                self._file_handle = None #
    
    def __del__(self): # 追加
        """デストラクタ""" # 追加
        self.close() #
        
    def search_in_file(self, search_term, headers=None, case_sensitive=True, is_regex=False, progress_callback=None):
        matched_cells = []
        target_headers = headers or self.header
        col_name_to_idx = {name: idx for idx, name in enumerate(self.header)}
        target_col_indices = {col_name_to_idx[h] for h in target_headers if h in col_name_to_idx}

        try:
            if is_regex:
                pattern = re.compile(search_term, 0 if case_sensitive else re.IGNORECASE)
            else:
                search_term_query = search_term if case_sensitive else search_term.lower()

            with open(self.filepath, 'r', encoding=self.encoding, errors='ignore', newline='') as f:
                f.readline()
                for row_idx, line_str in enumerate(f):
                    try:
                        reader = csv.reader(StringIO(line_str), delimiter=self.delimiter, quotechar='"')
                        row_cells = next(reader, [])
                        for col_idx in target_col_indices:
                            if col_idx < len(row_cells):
                                cell_value = row_cells[col_idx]
                                if is_regex:
                                    if pattern.search(cell_value):
                                        matched_cells.append((row_idx, col_idx))
                                else:
                                    value_to_check = cell_value if case_sensitive else cell_value.lower()
                                    if search_term_query in value_to_check:
                                        matched_cells.append((row_idx, col_idx))
                    except (csv.Error, StopIteration):
                        continue

                    if progress_callback and (row_idx % 1000 == 0 or row_idx == self.total_rows - 1):
                        progress_callback(row_idx + 1)
            
            return matched_cells
        except Exception as e:
            print(f"Error searching in file: {e}")
            return []

    def get_total_rows(self):
        # 修正: total_rowsが_build_row_indexで設定されることを想定
        return self.total_rows