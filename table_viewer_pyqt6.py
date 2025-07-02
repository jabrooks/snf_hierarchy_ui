#table_viewer_pyqt6.py

import sys
import requests # For making HTTP requests
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
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

    def __init__(self, api_url):
        super().__init__()
        self.api_url = api_url
        self._is_running = True

    def run(self):
        """
        Executes the network request.
        """
        if not self._is_running:
            return

        try:
            print(f"Worker: Fetching data from {self.api_url}")
            response = requests.get(self.api_url, timeout=15) # 15-second timeout
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
        self.setGeometry(100, 100, 900, 700) # x, y, width, height

        self.fetch_worker = None # To hold the worker thread instance

        # --- Central Widget and Layout ---
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(15) # Spacing between widgets
        main_layout.setContentsMargins(20, 20, 20, 20) # Margins for the main layout

        # --- Title ---
        title_label = QLabel("Snowflake Object Explorer")
        title_font = QFont("Inter", 20, QFont.Weight.Bold)
        title_label.setFont(title_font)
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(title_label)

        # --- Input Fields Layout ---
        input_layout = QHBoxLayout()
        input_layout.setSpacing(10)

        # Backend URL
        backend_url_label = QLabel("Backend API URL:")
        self.backend_url_input = QLineEdit("http://127.0.0.1:5001/api/snowflake-tables")
        self.backend_url_input.setToolTip("The URL of your Flask backend API endpoint.")
        input_layout.addWidget(backend_url_label)
        input_layout.addWidget(self.backend_url_input, 2) # Stretch factor for URL input

        # Database Filter
        db_filter_label = QLabel("Database Filter (Optional):")
        self.db_filter_input = QLineEdit()
        self.db_filter_input.setPlaceholderText("e.g., SALES_DB")
        input_layout.addWidget(db_filter_label)
        input_layout.addWidget(self.db_filter_input, 1)

        # Schema Filter
        schema_filter_label = QLabel("Schema Filter (Optional):")
        self.schema_filter_input = QLineEdit()
        self.schema_filter_input.setPlaceholderText("e.g., PUBLIC")
        input_layout.addWidget(schema_filter_label)
        input_layout.addWidget(self.schema_filter_input, 1)
        
        main_layout.addLayout(input_layout)

        # --- Fetch Button ---
        self.fetch_button = QPushButton("Fetch Table List")
        self.fetch_button.setFont(QFont("Inter", 10, QFont.Weight.DemiBold))
        self.fetch_button.setStyleSheet("""
            QPushButton {
                background-color: #3B82F6; /* Tailwind blue-500 */
                color: white;
                padding: 10px 15px;
                border: none;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #2563EB; /* Tailwind blue-600 */
            }
            QPushButton:pressed {
                background-color: #1D4ED8; /* Tailwind blue-700 */
            }
            QPushButton:disabled {
                background-color: #9CA3AF; /* Tailwind gray-400 */
                color: #E5E7EB; /* Tailwind gray-200 */
            }
        """)
        self.fetch_button.clicked.connect(self.start_fetch_data)
        main_layout.addWidget(self.fetch_button, 0, Qt.AlignmentFlag.AlignCenter) # Center the button

        # --- Results Table ---
        self.table_widget = QTableWidget()
        self.table_widget.setColumnCount(5)
        self.table_widget.setHorizontalHeaderLabels(
            ["Database Name", "Schema Name", "Table Name", "Owner", "Created On"]
        )
        self.table_widget.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table_widget.horizontalHeader().setStyleSheet("""
            QHeaderView::section { 
                background-color: #4A5568; /* Tailwind gray-700 */
                color: white; 
                padding: 4px; 
                border: 1px solid #6B7280; /* Tailwind gray-500 */
                font-weight: bold;
            }
        """)
        self.table_widget.setAlternatingRowColors(True)
        self.table_widget.setStyleSheet("""
            QTableWidget { 
                gridline-color: #D1D5DB; /* Tailwind gray-300 */
                alternate-background-color: #F3F4F6; /* Tailwind gray-100 */
                background-color: white;
            }
        """)
        self.table_widget.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers) # Make table read-only
        main_layout.addWidget(self.table_widget)

        # --- Status Bar ---
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready. Enter filters and click 'Fetch Table List'.")

    def start_fetch_data(self):
        """
        Initiates the data fetching process by starting the worker thread.
        """
        if self.fetch_worker and self.fetch_worker.isRunning():
            # Optionally, you could try to stop the previous worker or just ignore the new request
            self.status_bar.showMessage("A fetch operation is already in progress...", 3000)
            return

        base_url = self.backend_url_input.text().strip()
        db_name = self.db_filter_input.text().strip()
        schema_name = self.schema_filter_input.text().strip()

        if not base_url:
            QMessageBox.warning(self, "Input Error", "Backend API URL cannot be empty.")
            return

        params = {}
        if db_name:
            params['table_database'] = db_name
        if schema_name:
            params['table_schema'] = schema_name
        
        # Construct URL with query parameters
        # requests library handles URL encoding for params
        api_url = base_url 
        if params: # Add '?' only if there are params
            api_url += "?" + "&".join([f"{key}={value}" for key, value in params.items()])


        self.status_bar.showMessage(f"Fetching data from {api_url}...")
        self.fetch_button.setEnabled(False)
        self.table_widget.setRowCount(0) # Clear previous results

        # Create and start the worker thread
        self.fetch_worker = FetchWorker(api_url)
        self.fetch_worker.data_ready.connect(self.populate_table)
        self.fetch_worker.error_occurred.connect(self.show_fetch_error)
        self.fetch_worker.finished.connect(self.on_fetch_finished) # Re-enable button etc.
        self.fetch_worker.start()

    def populate_table(self, data):
        """
        Populates the QTableWidget with data received from the backend.
        """
        self.table_widget.setRowCount(0) # Clear previous results just in case
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
        self.table_widget.resizeColumnsToContents() # Adjust column widths

    def show_fetch_error(self, error_message):
        """
        Displays an error message in the status bar and a QMessageBox.
        """
        self.status_bar.showMessage(f"Error: {error_message}", 7000)
        QMessageBox.critical(self, "Fetch Error", error_message)

    def on_fetch_finished(self):
        """
        Called when the fetch worker thread has finished.
        """
        self.fetch_button.setEnabled(True)
        if self.fetch_worker: # Ensure worker exists
            self.fetch_worker.deleteLater() # Schedule the worker for deletion
            self.fetch_worker = None
        print("Fetch operation finished.")

    def closeEvent(self, event):
        """
        Handle the window close event to stop any running worker thread.
        """
        if self.fetch_worker and self.fetch_worker.isRunning():
            print("Window closing, stopping worker thread...")
            self.fetch_worker.stop() # Signal the worker to stop
            self.fetch_worker.wait(2000) # Wait for up to 2 seconds for the thread to finish
        event.accept()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    # Apply a global font for consistency (optional)
    # default_font = QFont("Inter", 9)
    # app.setFont(default_font)
    
    viewer = SnowflakeViewerApp()
    viewer.show()
    sys.exit(app.exec())
