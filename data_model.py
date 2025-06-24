# data_model.py

from PySide6.QtCore import QAbstractTableModel, Qt, QModelIndex, Signal
from PySide6.QtGui import QColor, QTextDocument
from PySide6.QtWidgets import QMessageBox, QApplication
import pandas as pd
import re

class CsvTableModel(QAbstractTableModel):
    data_requested = Signal(list)

    def __init__(self, dataframe=pd.DataFrame(), theme=None, parent=None):
        super().__init__(parent)
        self._dataframe = dataframe
        self._headers = list(dataframe.columns) if dataframe is not None else []
        self._theme = theme
        self._backend = None
        self._app_instance = None
        self._search_highlight_indexes = set()
        self._current_search_index = QModelIndex()

    def set_dataframe(self, dataframe):
        self.beginResetModel()
        self._dataframe = dataframe if dataframe is not None else pd.DataFrame()
        self._headers = list(self._dataframe.columns)
        self._backend = None
        self.endResetModel()

    def set_header(self, headers):
        self._headers = headers
        self.layoutChanged.emit()

    def set_backend(self, backend_instance):
        self.beginResetModel()
        self._backend = backend_instance
        if hasattr(self._backend, 'header') and self._backend.header:
            self._headers = self._backend.header
        else:
            pass 
        self._dataframe = pd.DataFrame()
        self.endResetModel()

    def set_app_instance(self, app_instance):
        self._app_instance = app_instance

    def set_search_highlight_indexes(self, indexes: list[QModelIndex]):
        old_indexes = self._search_highlight_indexes
        self._search_highlight_indexes = set(indexes)
        indexes_to_update = old_indexes.union(self._search_highlight_indexes)
        if indexes_to_update:
            rows = [idx.row() for idx in indexes_to_update]
            cols = [idx.column() for idx in indexes_to_update]
            if rows and cols:
                min_row, max_row = min(rows), max(rows)
                min_col, max_col = min(cols), max(cols)
                self.dataChanged.emit(self.index(min_row, min_col), self.index(max_row, max_col), [Qt.BackgroundRole, Qt.ForegroundRole])

    def set_current_search_index(self, index: QModelIndex):
        old_index = self._current_search_index
        self._current_search_index = index
        if old_index.isValid():
            self.dataChanged.emit(old_index, old_index, [Qt.BackgroundRole, Qt.ForegroundRole])
        if index.isValid():
            self.dataChanged.emit(index, index, [Qt.BackgroundRole, Qt.ForegroundRole])

    def rowCount(self, parent=QModelIndex()):
        if self._backend:
            return self._backend.get_total_rows() if hasattr(self._backend, 'get_total_rows') else 0
        return self._dataframe.shape[0] if self._dataframe is not None else 0

    def columnCount(self, parent=QModelIndex()):
        return len(self._headers)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or not self._theme: return None
        row, col = index.row(), index.column()

        if role in (Qt.DisplayRole, Qt.EditRole):
            if self._backend:
                try:
                    df_row = self._backend.get_rows_by_ids([row]) 
                    if not df_row.empty and col < len(self._headers):
                        col_name = self.headerData(col, Qt.Horizontal)
                        if col_name in df_row.columns:
                            cell_content = df_row.loc[row, col_name]
                            # ▼▼▼ 修正箇所 ▼▼▼
                            # HTMLタグを保持するため、QTextDocumentによる除去処理を削除
                            return str(cell_content)
                except Exception as e:
                    print(f"Error fetching data from backend at row {row}, col {col}: {e}"); return "ERROR"
                return ""
            if self._dataframe is not None and 0 <= row < self._dataframe.shape[0] and 0 <= col < self.columnCount():
                cell_content = self._dataframe.iloc[row, col]
                # ▼▼▼ 修正箇所 ▼▼▼
                # HTMLタグを保持するため、QTextDocumentによる除去処理を削除
                return str(cell_content)
            return None

        if role == Qt.BackgroundRole:
            if self._app_instance and index in self._app_instance.pulsing_cells:
                return self._theme.INFO_QCOLOR
            if index == self._current_search_index: return QColor(self._theme.DANGER)
            elif index in self._search_highlight_indexes: return QColor(self._theme.WARNING).lighter(150)
            return self._theme.BG_LEVEL_0_QCOLOR if row % 2 == 0 else self._theme.BG_LEVEL_1_QCOLOR
            
        if role == Qt.ForegroundRole and index == self._current_search_index: return QColor("white")
        
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                if 0 <= section < len(self._headers): return self._headers[section]
            elif orientation == Qt.Vertical: return str(section + 1)
        return None

    def setHeaderData(self, section, orientation, value, role=Qt.EditRole):
        if orientation == Qt.Horizontal and role == Qt.EditRole and 0 <= section < len(self._headers):
            old_header = self._headers[section]
            if str(old_header) == str(value):
                return False

            self._headers[section] = value
            
            self.headerDataChanged.emit(orientation, section, section)
            return True
        return super().setHeaderData(section, orientation, value, role)

    def flags(self, index):
        if not index.isValid(): return Qt.NoItemFlags
        if self._app_instance and self._app_instance.lazy_loader is not None:
             return Qt.ItemIsSelectable | Qt.ItemIsEnabled
        return Qt.ItemIsSelectable | Qt.ItemIsEnabled | Qt.ItemIsEditable

    def setData(self, index, value, role=Qt.EditRole):
        if not (role == Qt.EditRole and index.isValid()):
            return False
        
        row, col = index.row(), index.column()
        col_name = self.headerData(col, Qt.Horizontal)

        # ▼▼▼ 修正箇所 ▼▼▼
        # HTMLをそのまま扱うため、QTextDocumentによるクリーンアップ処理は行わない
        # 渡された値をそのまま使用する
        plain_text_value = value
        
        current_data = self.data(index, Qt.DisplayRole)
        if str(current_data) == str(plain_text_value):
            return False

        if self._backend and hasattr(self._backend, 'update_cells'):
            change = [{'row_idx': row, 'col_name': col_name, 'new_value': plain_text_value}]
            self._backend.update_cells(change)
            self.dataChanged.emit(index, index, [Qt.DisplayRole, Qt.EditRole])
            return True
        elif self._dataframe is not None:
            if 0 <= row < self.rowCount() and 0 <= col < self.columnCount():
                self._dataframe.iloc[row, col] = plain_text_value
                self.dataChanged.emit(index, index, [Qt.DisplayRole, Qt.EditRole])
                return True
        return False

    def insertColumns(self, column, count, parent=QModelIndex(), names=None):
        if self._backend and hasattr(self._backend, 'recreate_table_with_new_columns'):
            old_headers_current = list(self._headers)
            temp_headers = list(old_headers_current)
            
            new_col_names = []
            for i in range(count):
                if names and i < len(names):
                    final_col_name = names[i]
                else:
                    new_col_name_base = "new_column"
                    counter = 1
                    while f"{new_col_name_base}_{counter}" in temp_headers:
                        counter += 1
                    final_col_name = f"{new_col_name_base}_{counter}"
                
                new_col_names.append(final_col_name)
                temp_headers.insert(column + i, final_col_name)
            
            QApplication.setOverrideCursor(Qt.WaitCursor)
            self._app_instance.progress_bar.setRange(0, self.rowCount())
            self._app_instance.progress_bar.setValue(0)
            self._app_instance.progress_bar.show()
            self._app_instance.show_operation_status("列の挿入: テーブルを再構築中...", duration=0)

            try:
                success = self._backend.recreate_table_with_new_columns(temp_headers, old_headers_current, 
                                                                         progress_callback=lambda p: self._app_instance.progress_bar_update_signal.emit(p))
                if success:
                    self.beginResetModel()
                    self._headers = temp_headers
                    self.endResetModel()
                else:
                    self.show_operation_status("列の挿入に失敗しました。", is_error=True)
                
                self._app_instance.progress_bar.hide()
                QApplication.restoreOverrideCursor()
                return success
            except Exception as e:
                self._app_instance.progress_bar.hide()
                QApplication.restoreOverrideCursor()
                print(f"Error recreating table for insert columns: {e}")
                self._app_instance.show_operation_status(f"列の挿入に失敗しました: {e}", is_error=True)
                return False
        
        self.beginInsertColumns(parent, column, column + count - 1)
        for i in range(count):
            if names and i < len(names):
                final_col_name = names[i]
            else:
                new_col_name_base = "new_column"
                counter = 1
                while f"{new_col_name_base}_{counter}" in self._headers: counter += 1
                final_col_name = f"{new_col_name_base}_{counter}"
            
            self._headers.insert(column + i, final_col_name)
            if self._dataframe is not None: self._dataframe.insert(column + i, final_col_name, "")
        self.endInsertColumns()
        return True

    def removeColumns(self, column, count, parent=QModelIndex()):
        if column < 0 or column + count > len(self._headers): return False
        
        cols_to_drop_names = self._headers[column : column + count]
        
        if self._backend and hasattr(self._backend, 'recreate_table_with_new_columns'):
            old_headers_current = list(self._headers)
            new_headers_after_delete = [h for h in old_headers_current if h not in cols_to_drop_names]
            
            QApplication.setOverrideCursor(Qt.WaitCursor)
            self._app_instance.progress_bar.setRange(0, self.rowCount())
            self._app_instance.progress_bar.setValue(0)
            self._app_instance.progress_bar.show()
            self._app_instance.show_operation_status("列の削除: テーブルを再構築中...", duration=0)

            try:
                success = self._backend.recreate_table_with_new_columns(new_headers_after_delete, old_headers_current,
                                                                         progress_callback=lambda p: self._app_instance.progress_bar_update_signal.emit(p))
                if success:
                    self.beginResetModel()
                    self._headers = new_headers_after_delete
                    self.endResetModel()

                self._app_instance.progress_bar.hide()
                QApplication.restoreOverrideCursor()
                return success
            except Exception as e:
                self._app_instance.progress_bar.hide()
                QApplication.restoreOverrideCursor()
                print(f"Error recreating table for remove columns: {e}")
                self._app_instance.show_operation_status(f"列の削除に失敗しました: {e}", is_error=True)
                return False
        
        self.beginRemoveColumns(parent, column, column + count - 1)
        if self._dataframe is not None:
            self._dataframe.drop(columns=cols_to_drop_names, inplace=True)
        del self._headers[column : column + count]
        self.endRemoveColumns()
        return True
    
    def insertRows(self, row, count, parent=QModelIndex()):
        self.beginInsertRows(parent, row, row + count - 1)
        
        if self._backend and hasattr(self._backend, 'insert_rows'):
            self._backend.insert_rows(row, count, self._headers)
        elif self._dataframe is not None:
            for i in range(count):
                new_row_df = pd.DataFrame([[""] * len(self._headers)], columns=self._headers)
                self._dataframe = pd.concat([self._dataframe.iloc[:row + i], new_row_df, self._dataframe.iloc[row + i:]]).reset_index(drop=True)

        self.endInsertRows()
        return True

    def removeRows(self, row, count, parent=QModelIndex()):
        if row < 0 or row + count > self.rowCount():
            return False
            
        rows_to_delete_indices = list(range(row, row + count))
        
        self.beginRemoveRows(parent, row, row + count - 1)
        
        if self._backend and hasattr(self._backend, 'remove_rows'):
            self._backend.remove_rows(rows_to_delete_indices)
        elif self._dataframe is not None:
            self._dataframe.drop(self._dataframe.index[row : row + count], inplace=True)
            self._dataframe.reset_index(drop=True, inplace=True)
        
        self.endRemoveRows()
        return True

    def sort(self, column, order):
        if self._backend:
            if hasattr(self._backend, 'set_sort_order'):
                col_name = self.headerData(column, Qt.Horizontal) if column != -1 else None
                self._backend.set_sort_order(col_name, order)
                self.beginResetModel()
                self.endResetModel()
        elif self._dataframe is not None:
            self.beginResetModel()
            if column == -1:
                self._dataframe.sort_index(inplace=True)
            else:
                try:
                    col_name = self.headerData(column, Qt.Horizontal)
                    self._dataframe.sort_values(
                        by=col_name,
                        ascending=(order == Qt.AscendingOrder),
                        inplace=True,
                        kind='mergesort'
                    )
                except Exception as e:
                    print(f"DataFrame sort error: {e}")
            self.endResetModel()

    def get_column_data(self, col_index):
        if col_index < 0 or col_index >= self.columnCount():
            return []

        col_name = self.headerData(col_index, Qt.Horizontal)

        if self._backend:
            if self.rowCount() > 500000:
                if self._app_instance:
                    QMessageBox.warning(self._app_instance, "パフォーマンス警告",
                                      "巨大な列データをメモリにロードします。この処理には時間がかかる場合があります。")
            df = self.get_dataframe()
            if not df.empty and col_name in df.columns:
                return df[col_name].tolist()

        elif self._dataframe is not None:
            if col_name in self._dataframe.columns:
                return self._dataframe[col_name].tolist()

        return []

    def get_dataframe(self):
        if self._backend:
            if self._app_instance: QApplication.setOverrideCursor(Qt.WaitCursor)
            try:
                if hasattr(self._backend, 'get_all_indices'):
                    all_indices = self._backend.get_all_indices()
                    df = self._backend.get_rows_by_ids(all_indices)
                    return df[self._headers] if not df.empty and set(self._headers).issubset(df.columns) else df
                elif hasattr(self._backend, 'get_total_rows'):
                    pass
            finally:
                if self._app_instance: QApplication.restoreOverrideCursor()
            return pd.DataFrame(columns=self._headers)
        return self._dataframe.copy()

    def get_rows_as_dataframe(self, row_indices: list[int]) -> pd.DataFrame:
        if not row_indices: return pd.DataFrame(columns=self._headers)
        if self._backend:
            if self._app_instance: QApplication.setOverrideCursor(Qt.WaitCursor)
            try: 
                df = self._backend.get_rows_by_ids(row_indices)
                return df[self._headers] if not df.empty and set(self._headers).issubset(df.columns) else df
            finally:
                if self._app_instance: QApplication.restoreOverrideCursor()
        if self._dataframe is not None:
            valid_indices = [idx for idx in row_indices if 0 <= idx < len(self._dataframe)]
            return self._dataframe.iloc[valid_indices].copy()
        return pd.DataFrame(columns=self._headers)