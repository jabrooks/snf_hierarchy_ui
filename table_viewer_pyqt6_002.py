import sys
import requests # For making HTTP requests
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QTableWidget, QTableWidgetItem,
    QHeaderView, QStatusBar, QMessageBox
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont

# --- Worker Thread for Network Requests ---
class FetchWorker(QThread):
    """
    Worker thread to handle network requests asynchronously,
    preventing the UI from freezing.
    """
    data_ready = pyqtSignal(list)
    error_occurred = pyqtSignal(str)

    def __init__(self, base_url, params=None): # Accept params
        super().__init__()
        self.base_url = base_url
        self.params = params if params is not None else {}
        self._is_running = True

    def run(self):
        """
        Executes the network request.
        """
        if not self._is_running:
            return
        try:
            print(f"Worker: Fetching data from {self.base_url} with params {self.params}")
            response = requests.get(self.base_url, params=self.params, timeout=15) # Pass params here
            response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
            
            if not self._is_running: # Check again in case stop was called during request
                return

            data = response.json()
            if isinstance(data, list):
                self.data_ready.emit(data)
            else:
                self.error_occurred.emit("Received unexpected data format from the server.")
        except requests.exceptions.Timeout:
            if self._is_running:
                self.error_occurred.emit("The request timed out. Please check the backend server.")
        except requests.exceptions.HTTPError as http_err:
            if self._is_running:
                error_message = f"HTTP error: {http_err.response.status_code} {http_err.response.reason}"
                try:
                    # Try to get more details from the JSON response
                    err_details = http_err.response.json()
                    if "error" in err_details:
                        error_message += f" - {err_details.get('error')}"
                    if "details" in err_details:
                        error_message += f" (Details: {err_details.get('details')})"
                except ValueError: # If response is not JSON
                    pass # Use the generic HTTP error message
                self.error_occurred.emit(error_message)
        except requests.exceptions.RequestException as e:
            if self._is_running:
                self.error_occurred.emit(f"Error fetching data: {e}")
        except ValueError as e: # JSON decoding error
             if self._is_running:
                self.error_occurred.emit(f"Error decoding JSON response: {e}")

    def stop(self):
        self._is_running = False


# --- Main Application Window ---
class SnowflakeViewerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Snowflake Object Explorer (PyQt)")
        self.setGeometry(100, 100, 1000, 750)

        self.fetch_worker = None

        # --- Central Widget and Layout ---
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(15) 
        main_layout.setContentsMargins(20, 20, 20, 20)

        # --- Title ---
        title_label = QLabel("Snowflake Object Explorer")
        # Use a common font like Arial, Verdana, or Tahoma for better cross-platform compatibility
        title_font = QFont("Arial", 20, QFont.Weight.Bold) # Changed from Inter to Arial
        title_label.setFont(title_font)
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(title_label)

        # --- Input Fields Layout using QGridLayout ---
        input_grid_layout = QGridLayout()
        input_grid_layout.setSpacing(10)
        input_grid_layout.setColumnStretch(1, 1) 
        input_grid_layout.setColumnStretch(3, 1)
        input_grid_layout.setColumnStretch(5, 1)

        backend_url_label = QLabel("Backend API URL:")
        self.backend_url_input = QLineEdit("http://127.0.0.1:5001/api/snowflake-tables")
        self.backend_url_input.setToolTip("The URL of your Flask backend API endpoint.")
        input_grid_layout.addWidget(backend_url_label, 0, 0)
        input_grid_layout.addWidget(self.backend_url_input, 0, 1, 1, 5)

        db_filter_label = QLabel("Database Filter:")
        self.db_filter_input = QLineEdit()
        self.db_filter_input.setPlaceholderText("e.g., SALES_DB")
        input_grid_layout.addWidget(db_filter_label, 1, 0)
        input_grid_layout.addWidget(self.db_filter_input, 1, 1)

        schema_filter_label = QLabel("Schema Filter:")
        self.schema_filter_input = QLineEdit()
        self.schema_filter_input.setPlaceholderText("e.g., PUBLIC")
        input_grid_layout.addWidget(schema_filter_label, 1, 2)
        input_grid_layout.addWidget(self.schema_filter_input, 1, 3)

        table_filter_label = QLabel("Table Name Filter:")
        self.table_filter_input = QLineEdit()            
        self.table_filter_input.setPlaceholderText("e.g., CUSTOMER or %_TEMP")
        self.table_filter_input.setToolTip("Enter a substring to filter table names (case-insensitive, uses LIKE on backend).")
        input_grid_layout.addWidget(table_filter_label, 1, 4)
        input_grid_layout.addWidget(self.table_filter_input, 1, 5)
        
        main_layout.addLayout(input_grid_layout)

        # --- Fetch Button ---
        self.fetch_button = QPushButton("Fetch Table List")
        # Corrected to DemiBold and using Arial
        self.fetch_button.setFont(QFont("Arial", 10, QFont.Weight.DemiBold)) # Changed from Inter, SemiBold to Arial, DemiBold
        self.fetch_button.setStyleSheet("""
            QPushButton {
                background-color: #3B82F6;
                color: white;
                padding: 10px 15px;
                border: none;
                border-radius: 5px;
                min-width: 150px;
            }
            QPushButton:hover {
                background-color: #2563EB;
            }
            QPushButton:pressed {
                background-color: #1D4ED8;
            }
            QPushButton:disabled {
                background-color: #9CA3AF;
                color: #E5E7EB;
            }
        """)
        self.fetch_button.clicked.connect(self.start_fetch_data)
        
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        button_layout.addWidget(self.fetch_button)
        button_layout.addStretch()
        main_layout.addLayout(button_layout)

        # --- Results Table ---
        self.table_widget = QTableWidget()
        self.table_widget.setColumnCount(5)
        self.table_widget.setHorizontalHeaderLabels(
            ["Database Name", "Schema Name", "Table Name", "Owner", "Created On"]
        )
        header = self.table_widget.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        header.setStyleSheet("""
            QHeaderView::section { 
                background-color: #4A5568;
                color: white; 
                padding: 4px; 
                border: 1px solid #6B7280;
                font-weight: bold; /* Defaulting to bold for header */
            }
        """)
        # If you want to set a specific font for the header:
        # header_font = QFont("Arial", 9, QFont.Weight.Bold)
        # header.setFont(header_font)

        self.table_widget.setAlternatingRowColors(True)
        self.table_widget.setStyleSheet("""
            QTableWidget { 
                gridline-color: #D1D5DB; 
                alternate-background-color: #F3F4F6; 
                background-color: white;
                font-size: 9pt; /* Using default system font or Arial if set globally */
            }
            QTableWidget::item {
                padding: 3px;
            }
        """)
        # If you want to set a specific font for table items:
        # table_item_font = QFont("Arial", 9)
        # self.table_widget.setFont(table_item_font)

        self.table_widget.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        main_layout.addWidget(self.table_widget)

        # --- Status Bar ---
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready. Enter filters and click 'Fetch Table List'.")

    def start_fetch_data(self):
        if self.fetch_worker and self.fetch_worker.isRunning():
            self.status_bar.showMessage("A fetch operation is already in progress...", 3000)
            return

        base_url = self.backend_url_input.text().strip()
        db_name = self.db_filter_input.text().strip()
        schema_name = self.schema_filter_input.text().strip()
        table_name_filter = self.table_filter_input.text().strip()

        if not base_url:
            QMessageBox.warning(self, "Input Error", "Backend API URL cannot be empty.")
            return

        params = {}
        if db_name:
            params['table_database'] = db_name
        if schema_name:
            params['table_schema'] = schema_name
        if table_name_filter: 
            params['table_name'] = table_name_filter 
        
        self.status_bar.showMessage(f"Fetching data...")
        self.fetch_button.setEnabled(False)
        self.table_widget.setRowCount(0) 

        self.fetch_worker = FetchWorker(base_url, params)
        self.fetch_worker.data_ready.connect(self.populate_table)
        self.fetch_worker.error_occurred.connect(self.show_fetch_error)
        self.fetch_worker.finished.connect(self.on_fetch_finished) 
        self.fetch_worker.start()

    def populate_table(self, data):
        self.table_widget.setRowCount(0) 
        if not data:
            self.status_bar.showMessage("No tables found matching your criteria.", 5000)
            return

        self.table_widget.setRowCount(len(data))
        for row_index, row_data in enumerate(data):
            self.table_widget.setItem(row_index, 0, QTableWidgetItem(row_data.get('database', 'N/A')))
            self.table_widget.setItem(row_index, 1, QTableWidgetItem(row_data.get('schema', 'N/A')))
            self.table_widget.setItem(row_index, 2, QTableWidgetItem(row_data.get('table', 'N/A')))
            self.table_widget.setItem(row_index, 3, QTableWidgetItem(row_data.get('owner', 'N/A')))
            self.table_widget.setItem(row_index, 4, QTableWidgetItem(row_data.get('created_on', 'N/A')))
        
        self.status_bar.showMessage(f"Successfully fetched {len(data)} items.", 5000)

    def show_fetch_error(self, error_message):
        self.status_bar.showMessage(f"Error: {error_message}", 7000)
        QMessageBox.critical(self, "Fetch Error", error_message)

    def on_fetch_finished(self):
        self.fetch_button.setEnabled(True)
        if self.fetch_worker: 
            self.fetch_worker.deleteLater() 
            self.fetch_worker = None
        print("Fetch operation finished.")

    def closeEvent(self, event):
        if self.fetch_worker and self.fetch_worker.isRunning():
            print("Window closing, stopping worker thread...")
            self.fetch_worker.stop() 
            self.fetch_worker.wait(2000) 
        event.accept()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    # You can set a global application font here if desired:
    # global_font = QFont("Arial", 9) # Example: Arial, 9pt
    # app.setFont(global_font)
    
    viewer = SnowflakeViewerApp()
    viewer.show()
    sys.exit(app.exec())
