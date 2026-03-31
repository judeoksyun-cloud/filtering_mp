import sys
import os
import io
import re
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QPushButton, QLabel, QFileDialog, 
                             QTableWidget, QTableWidgetItem, QHeaderView, QMessageBox, 
                             QProgressDialog, QDialog, QSpinBox)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QPoint
from PyQt5.QtGui import QKeySequence, QIcon

class DataLoaderThread(QThread):
    progress = pyqtSignal(int)
    finished = pyqtSignal(object)
    error = pyqtSignal(str)
    
    def __init__(self, file_paths):
        super().__init__()
        self.file_paths = file_paths
        
    def run(self):
        try:
            import pandas as pd
            import csv
            
            result_data = {} # {filepath: (df, meta_lines)}
            total_files = len(self.file_paths)
            
            for file_idx, file_path in enumerate(self.file_paths):
                # 1. Read metadata
                with open(file_path, 'r', encoding='cp949', errors='replace') as f:
                    lines = f.readlines()
                
                meta_data_lines = []
                data_start_idx = 0
                for i, line in enumerate(lines):
                    if line.strip().startswith('!'):
                        data_start_idx = i
                        break
                    meta_data_lines.append(line)
                
                total_lines_for_file = 1000000
                for line in meta_data_lines:
                    if line.startswith('NUMLINES,'):
                        match = re.search(r'\d+', line)
                        if match:
                            total_lines_for_file = int(match.group())
                        break
                
                chunksize = 100000
                chunks = []
                rows_read = 0
                
                with pd.read_csv(file_path, skiprows=data_start_idx, low_memory=False, 
                                 encoding='cp949', encoding_errors='replace', dtype=str, quoting=csv.QUOTE_NONE, chunksize=chunksize) as reader:
                    for chunk in reader:
                        chunks.append(chunk)
                        rows_read += len(chunk)
                        
                        base_prog = (file_idx / total_files) * 100
                        file_prog = (min(int((rows_read / total_lines_for_file) * 100), 100) / total_files)
                        prog = int(base_prog + file_prog)
                        self.progress.emit(prog)
                
                if chunks:
                    df = pd.concat(chunks, ignore_index=True)
                else:
                    df = pd.DataFrame()
                    
                df.fillna("", inplace=True)
                result_data[file_path] = (df, meta_data_lines)
            
            self.finished.emit(result_data)
            
        except Exception as e:
            self.error.emit(str(e))

class DataFilterThread(QThread):
    progress = pyqtSignal(int)
    finished = pyqtSignal(str)
    error = pyqtSignal(str)

    def __init__(self, raw_data_dict, mode, sample_n, filter_criteria, sample_filters):
        super().__init__()
        self.raw_data_dict = raw_data_dict
        self.mode = mode
        self.sample_n = sample_n
        self.filter_criteria = filter_criteria
        self.sample_filters = sample_filters

    def run(self):
        try:
            import pandas as pd
            import csv
            
            total_files = len(self.raw_data_dict)
            file_idx = 0
            
            # Merge filters
            merged_filters = {}
            for col, vals in self.filter_criteria:
                if col not in merged_filters:
                    merged_filters[col] = set()
                merged_filters[col].update(vals)

            total_filtered_rows = 0

            for file_path, (raw_df, meta_lines) in self.raw_data_dict.items():
                df_filtered = raw_df.copy()
                
                # Apply Filtering
                primary_filter_cols = []
                if merged_filters:
                    for col, vals in merged_filters.items():
                        vals_list = list(vals)
                        
                        final_col = None
                        if col in df_filtered.columns:
                            final_col = col
                        else:
                            for df_col in df_filtered.columns:
                                c_df = df_col.strip().lstrip('!')
                                c_in = col.strip().lstrip('!')
                                if c_df.lower() == c_in.lower():
                                    final_col = df_col
                                    break
                        
                        if not final_col:
                            continue
                            
                        primary_filter_cols.append(final_col)
                        series_str = df_filtered[final_col].str.strip().str.strip('\'"')
                        vals_clean = [str(v).strip().strip('\'"') for v in vals_list]
                        df_filtered = df_filtered[series_str.isin(vals_clean)]

                # Random Sampling (Stage 2)
                if self.mode == 'random' and not df_filtered.empty:
                    merged_sample_filters = {}
                    if self.sample_filters:
                        for col, vals in self.sample_filters:
                            if col not in merged_sample_filters:
                                merged_sample_filters[col] = set()
                            merged_sample_filters[col].update(vals)
                    
                    target_cols_for_grouping = list(primary_filter_cols)
                    
                    if merged_sample_filters:
                        for col, vals in merged_sample_filters.items():
                            vals_list = list(vals)
                            final_col = None
                            if col in df_filtered.columns:
                                final_col = col
                            else:
                                for df_col in df_filtered.columns:
                                    if df_col.strip().lstrip('!').lower() == col.strip().lstrip('!').lower():
                                        final_col = df_col
                                        break
                            
                            if not final_col:
                                continue
                                
                            if final_col not in target_cols_for_grouping:
                                target_cols_for_grouping.append(final_col)
                            series_str = df_filtered[final_col].str.strip().str.strip('\'"')
                            vals_clean = [str(v).strip().strip('\'"') for v in vals_list]
                            df_filtered = df_filtered[series_str.isin(vals_clean)]
                    
                    if target_cols_for_grouping and not df_filtered.empty:
                        sampled_dfs = []
                        for name, group in df_filtered.groupby(target_cols_for_grouping):
                            sampled_dfs.append(group.sample(n=min(len(group), self.sample_n)))
                        if sampled_dfs:
                            df_filtered = pd.concat(sampled_dfs, ignore_index=True)
                        else:
                            df_filtered = pd.DataFrame(columns=df_filtered.columns)
                    
                    # Assign Sequential SPCODE
                    if not df_filtered.empty:
                        spcode_col = 'SPCODE' if 'SPCODE' in df_filtered.columns else '"SPCODE"' if '"SPCODE"' in df_filtered.columns else None
                        if spcode_col:
                            new_spcodes = [str(x) for x in range(1001, 1001 + len(df_filtered))]
                            df_filtered[spcode_col] = new_spcodes

                # Calculate rows starting with '*'
                first_col = df_filtered.columns[0] if not df_filtered.empty else None
                star_count = 0
                if first_col and not df_filtered.empty:
                    star_count = int(df_filtered[first_col].astype(str).str.strip().str.startswith('*').sum())

                # Save new file
                dir_name = os.path.dirname(file_path)
                base_name = os.path.basename(file_path)
                new_dir = os.path.join(dir_name, "Filtering")
                os.makedirs(new_dir, exist_ok=True)
                save_name = os.path.join(new_dir, base_name)
                
                updated_meta = []
                for line in meta_lines:
                    if line.startswith('NUMLINES,'):
                        line = re.sub(r'(^NUMLINES,\s*)\d+', rf'\g<1>{star_count}', line)
                    updated_meta.append(line)
                    
                with open(save_name, 'w', encoding='cp949', errors='replace', newline='\r\n') as f:
                    f.writelines(updated_meta)
                
                if not df_filtered.empty:
                    df_filtered.to_csv(save_name, mode='a', index=False, encoding='cp949', errors='replace', lineterminator='\r\n', quoting=csv.QUOTE_NONE)
                
                total_filtered_rows += len(df_filtered)
                
                file_idx += 1
                prog = int((file_idx / total_files) * 100)
                self.progress.emit(prog)
                
            self.finished.emit(f"필터링 완료!\n총 저장된 파일 개수: {total_files}개\n추출된 총 행 수: {total_filtered_rows}행\n저장 위치: {new_dir}")
        except Exception as e:
            self.error.emit(str(e))


class SamplingDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("샘플추출 조건 설정")
        self.setGeometry(150, 150, 600, 500)
        
        layout = QVBoxLayout(self)
        
        info_label = QLabel("▼ 추가로 샘플링할 조건 조합을 입력하세요.\n"
                            "   [1열: 속성명, 2열: 값(콤마/탭/줄바꿈 구분)]\n"
                            "   * 엑셀에서 복사 후 표를 클릭하고 붙여넣기(Ctrl+V) 하세요.")
        info_label.setStyleSheet("background-color: #f0f0f0; padding: 5px; border-radius: 3px;")
        layout.addWidget(info_label)
        
        self.table = QTableWidget(50, 2)
        self.table.setHorizontalHeaderLabels(["샘플링할 속성명", "샘플링할 값"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Interactive)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table.setColumnWidth(0, 200)
        layout.addWidget(self.table)
        
        bottom_layout = QHBoxLayout()
        lbl_count = QLabel("각 조건 조합당 샘플링 수:")
        self.spin_count = QSpinBox()
        self.spin_count.setRange(1, 1000000)
        self.spin_count.setValue(5)
        
        btn_run = QPushButton("실행")
        btn_run.clicked.connect(self.accept)
        btn_cancel = QPushButton("취소")
        btn_cancel.clicked.connect(self.reject)
        
        bottom_layout.addWidget(lbl_count)
        bottom_layout.addWidget(self.spin_count)
        bottom_layout.addStretch()
        bottom_layout.addWidget(btn_run)
        bottom_layout.addWidget(btn_cancel)
        
        layout.addLayout(bottom_layout)

    def get_data(self):
        filters = []
        last_col_name = None
        for row in range(self.table.rowCount()):
            col_item = self.table.item(row, 0)
            val_item = self.table.item(row, 1)

            current_col = None
            if col_item and col_item.text().strip():
                current_col = col_item.text().strip()
            elif last_col_name:
                if val_item and val_item.text().strip():
                    current_col = last_col_name
            
            if current_col:
                last_col_name = current_col
                if val_item and val_item.text().strip():
                    raw_val = val_item.text().strip()
                    val_list = [v.strip().strip("'\"") for v in re.split(r'[,\t\n]+', raw_val) if v.strip()]
                    if val_list:
                        filters.append((current_col, val_list))
        
        return filters, self.spin_count.value()

    def keyPressEvent(self, event):
        if event.matches(QKeySequence.Paste) and self.table.hasFocus():
            self.paste_from_clipboard()
        else:
            super().keyPressEvent(event)

    def paste_from_clipboard(self):
        clipboard = QApplication.clipboard()
        text = clipboard.text()
        if not text: return
        rows = text.strip().split('\n')
        current_row = self.table.currentRow()
        if current_row < 0: current_row = 0
        current_col = self.table.currentColumn()
        if current_col < 0: current_col = 0

        for r_idx, row_text in enumerate(rows):
            if current_row + r_idx >= self.table.rowCount():
                self.table.insertRow(self.table.rowCount())
            columns = row_text.split('\t')
            for c_idx, col_text in enumerate(columns):
                target_col = current_col + c_idx
                if target_col < self.table.columnCount():
                    item = QTableWidgetItem(col_text.strip())
                    self.table.setItem(current_row + r_idx, target_col, item)

class DataFilterApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.raw_data_dict = {} # {filepath: (df, meta_lines)}
        self.initUI()

    def resource_path(self, relative_path):
        import os, sys
        try:
            base_path = sys._MEIPASS
        except Exception:
            base_path = os.path.abspath(".")
        return os.path.join(base_path, relative_path)

    def initUI(self):
        self.setWindowTitle('Prophet MP Filter Tool')
        try:
            self.setWindowIcon(QIcon(self.resource_path("icon.ico")))
        except:
            pass
        self.setGeometry(100, 100, 1000, 700)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # 1. File Load
        load_layout = QHBoxLayout()
        self.lbl_status = QLabel("파일이 로드되지 않았습니다.")
        self.lbl_status.setStyleSheet("color: red; font-weight: bold;")
        btn_load = QPushButton("Raw 데이터 파일 열기 (.RPT, 다중 선택 가능)")
        btn_load.clicked.connect(self.load_file)
        load_layout.addWidget(btn_load)
        load_layout.addWidget(self.lbl_status)
        main_layout.addLayout(load_layout)

        # 2. Info
        info_label = QLabel("▼ 아래 표에 필터링 조건을 입력하거나 엑셀에서 복사하여 붙여넣으세요 (Ctrl+V).\n"
                            "   [1열: 속성명(Header), 2열: 속성값(콤마/탭/줄바꿈으로 구분)]\n"
                            "   * 엑셀에서 '속성명'과 '값' 두 열을 드래그해서 복사 후 붙여넣으세요.")
        info_label.setStyleSheet("background-color: #f0f0f0; padding: 5px; border-radius: 3px;")
        main_layout.addWidget(info_label)

        # 3. Table
        self.table = QTableWidget(100, 2)
        self.table.setHorizontalHeaderLabels(["필터링할 속성명 (Column Name)", "필터링할 값 (Values)"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Interactive)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table.setColumnWidth(0, 250)
        main_layout.addWidget(self.table)

        # 4. Run Button
        btn_run = QPushButton("필터링 적용 및 저장하기")
        btn_run.setStyleSheet("background-color: #0078d7; color: white; font-size: 14px; padding: 10px;")
        btn_run.clicked.connect(self.show_run_options)
        main_layout.addWidget(btn_run)

    def show_run_options(self):
        if not self.raw_data_dict:
            QMessageBox.warning(self, "Warning", "먼저 데이터 파일을 로드해주세요.")
            return

        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("추출 방식 선택")
        msg_box.setText("어떤 방식으로 데이터를 추출하시겠습니까?")
        
        btn_all = msg_box.addButton("1. 전체추출(All)", QMessageBox.ActionRole)
        btn_random = msg_box.addButton("2. 샘플추출(Random)", QMessageBox.ActionRole)
        msg_box.addButton("취소", QMessageBox.RejectRole)
        
        msg_box.exec_()
        
        if msg_box.clickedButton() == btn_all:
            self.run_filtering(mode='all')
        elif msg_box.clickedButton() == btn_random:
            dialog = SamplingDialog(self)
            if dialog.exec_():
                sample_filters, sample_n = dialog.get_data()
                self.run_filtering(mode='random', sample_n=sample_n, sample_filters=sample_filters)

    def keyPressEvent(self, event):
        if event.matches(QKeySequence.Paste) and self.table.hasFocus():
            self.paste_from_clipboard()
        else:
            super().keyPressEvent(event)

    def paste_from_clipboard(self):
        clipboard = QApplication.clipboard()
        text = clipboard.text()
        if not text: return

        rows = text.strip().split('\n')
        current_row = self.table.currentRow()
        if current_row < 0: current_row = 0
        current_col = self.table.currentColumn()
        if current_col < 0: current_col = 0

        for r_idx, row_text in enumerate(rows):
            if current_row + r_idx >= self.table.rowCount():
                self.table.insertRow(self.table.rowCount())
            columns = row_text.split('\t')
            for c_idx, col_text in enumerate(columns):
                target_col = current_col + c_idx
                if target_col < self.table.columnCount():
                    item = QTableWidgetItem(col_text.strip())
                    self.table.setItem(current_row + r_idx, target_col, item)

    def load_file(self):
        file_names, _ = QFileDialog.getOpenFileNames(self, "Open Files", "", "RPT Files (*.RPT);;All Files (*)")
        if file_names:
            self.progress_dialog = QProgressDialog("데이터를 불러오는 중입니다...", "취소", 0, 100, self)
            self.progress_dialog.setWindowTitle("로딩 중")
            self.progress_dialog.setWindowModality(Qt.WindowModal)
            self.progress_dialog.setCancelButton(None)
            self.progress_dialog.setAutoClose(True)
            self.progress_dialog.setValue(0)
            self.progress_dialog.show()
            
            self.loader_thread = DataLoaderThread(file_names)
            self.loader_thread.progress.connect(self.progress_dialog.setValue)
            self.loader_thread.finished.connect(self.on_load_finished)
            self.loader_thread.error.connect(self.on_load_error)
            self.loader_thread.start()

    def on_load_finished(self, result_dict):
        self.raw_data_dict = result_dict
        if hasattr(self, 'progress_dialog'):
            self.progress_dialog.setValue(100)
            self.progress_dialog.close()
            
        total_rows = 0
        total_star_rows = 0
        for filepath, (df, meta) in self.raw_data_dict.items():
            total_rows += len(df)
            first_col = df.columns[0] if not df.empty else None
            if first_col and not df.empty:
                star_count = int(df[first_col].astype(str).str.strip().str.startswith('*').sum())
                total_star_rows += star_count
        
        self.lbl_status.setText(f"파일 {len(self.raw_data_dict)}개 로드 완료 (전체: {total_star_rows}행)")
        self.lbl_status.setStyleSheet("color: green; font-weight: bold;")
        
    def on_load_error(self, err_msg):
        if hasattr(self, 'progress_dialog'):
            self.progress_dialog.close()
        QMessageBox.critical(self, "Error", f"파일 시스템 읽기 중 오류 발생:\n{err_msg}")

    def run_filtering(self, mode='all', sample_n=None, sample_filters=None):
        if not self.raw_data_dict:
            QMessageBox.warning(self, "Warning", "먼저 데이터 파일을 로드해주세요.")
            return

        filter_criteria = []
        last_col_name = None
        for row in range(self.table.rowCount()):
            col_item = self.table.item(row, 0)
            val_item = self.table.item(row, 1)

            current_col = None
            if col_item and col_item.text().strip():
                current_col = col_item.text().strip()
            elif last_col_name:
                if val_item and val_item.text().strip():
                    current_col = last_col_name
            
            if current_col:
                last_col_name = current_col
                if val_item and val_item.text().strip():
                    raw_val = val_item.text().strip()
                    val_list = [v.strip().strip("'\"") for v in re.split(r'[,\t\n]+', raw_val) if v.strip()]
                    if val_list:
                        filter_criteria.append((current_col, val_list))

        if not filter_criteria:
            QMessageBox.information(self, "Info", "입력된 필터 조건이 없습니다. 원본 그대로/샘플링하여 저장합니다.")
            
        self.progress_dialog = QProgressDialog("데이터 필터링 및 저장 중입니다...", "취소", 0, 100, self)
        self.progress_dialog.setWindowTitle("실행 중")
        self.progress_dialog.setWindowModality(Qt.WindowModal)
        self.progress_dialog.setCancelButton(None)
        self.progress_dialog.setAutoClose(True)
        self.progress_dialog.setValue(0)
        self.progress_dialog.show()

        self.filter_thread = DataFilterThread(self.raw_data_dict, mode, sample_n, filter_criteria, sample_filters)
        self.filter_thread.progress.connect(self.progress_dialog.setValue)
        self.filter_thread.finished.connect(self.on_filter_finished)
        self.filter_thread.error.connect(self.on_filter_error)
        self.filter_thread.start()

    def on_filter_finished(self, msg):
        if hasattr(self, 'progress_dialog'):
            self.progress_dialog.setValue(100)
            self.progress_dialog.close()
        QMessageBox.about(self, "Success", msg)
        
    def on_filter_error(self, err_msg):
        if hasattr(self, 'progress_dialog'):
            self.progress_dialog.close()
        QMessageBox.critical(self, "Error", f"처리 중 오류: {err_msg}")


if __name__ == '__main__':
    try:
        import ctypes
        myappid = 'mycompany.myproduct.subproduct.version'
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)
    except:
        pass
    app = QApplication(sys.argv)
    ex = DataFilterApp()
    
    try:
        import pyi_splash
        if pyi_splash.is_alive():
            pyi_splash.close()
    except Exception:
        pass
        
    ex.show()
    sys.exit(app.exec_())