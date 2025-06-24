# lazy_loader.py

import pandas as pd
import os
import csv
import re
from io import StringIO
from PySide6.QtCore import Signal, QObject

class LazyCSVLoader(QObject):
    progress_update = Signal(int)
    
    def __init__(self, filepath, encoding='utf-8', theme=None):
        super().__init__()
        self.filepath = filepath
        self.encoding = encoding
        self.theme = theme
        self.header = None
        self.delimiter = ','
        self.total_rows = 0
        self._cache = {}
        self._row_index = []
        self._init_metadata()
    
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
        self._row_index = []
        try:
            with open(self.filepath, 'rb') as f:
                # Skip header
                f.readline()
                while True:
                    record_start_pos = f.tell()
                    line = f.readline()
                    if not line: break
                    self._row_index.append(record_start_pos)
            self.total_rows = len(self.get_row_count_fast(self.filepath)) -1
        except Exception as e:
            print(f"Error building row index: {e}")
            self.total_rows = 0

    def get_row_count_fast(self, filepath):
        with open(filepath, 'rb') as f:
            count = 0
            buf_size = 1024 * 1024
            buf = f.read(buf_size)
            while buf:
                count += buf.count(b'\n')
                buf = f.read(buf_size)
            return count

    def get_rows_by_ids(self, indices):
        if not indices: return pd.DataFrame(columns=self.header)
        
        result_df = pd.DataFrame(index=indices, columns=self.header)
        rows_to_fetch = sorted(list(set(indices)))
        
        try:
            with open(self.filepath, 'r', encoding=self.encoding, errors='ignore', newline='') as f:
                for i in rows_to_fetch:
                    if i >= self.total_rows: continue
                    f.seek(self._row_index[i])
                    line = f.readline()
                    csv_data = StringIO(line)
                    reader = csv.reader(csv_data, delimiter=self.delimiter, quotechar='"')
                    try:
                        parsed_row = next(reader)
                        parsed_row.extend([''] * (len(self.header) - len(parsed_row)))
                        result_df.loc[i] = parsed_row[:len(self.header)]
                    except StopIteration:
                        result_df.loc[i] = [''] * len(self.header)
            return result_df

        except Exception as e:
            print(f"Error in get_rows_by_ids for indices {indices}: {e}")
            return pd.DataFrame(columns=self.header)

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
        return self.total_rows