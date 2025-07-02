import sys
import requests
import threading # For running Flask in a separate thread
import os
from datetime import datetime

# --- PyQt6 Imports ---
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QTableWidget, QTableWidgetItem,
    QHeaderView, QStatusBar, QMessageBox, QMenu, QFileDialog # Added QMenu, QFileDialog
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont, QGuiApplication, QAction # Added QAction

# --- Flask Imports ---
from flask import Flask, jsonify, request
from flask_cors import CORS
import snowflake.connector

# --- Configuration ---
FLASK_HOST = '127.0.0.1'
FLASK_PORT = 5001 # Ensure this port is free
BACKEND_API_BASE_URL = f"http://{FLASK_HOST}:{FLASK_PORT}"

# ==============================================================================
# FLASK BACKEND APPLICATION (to be run in a thread)
# ==============================================================================
flask_app = Flask(__name__)
CORS(flask_app)

@flask_app.route('/api/role-hierarchy', methods=['GET'])
def get_role_hierarchy_route():
    """
    API endpoint to get the role hierarchy for a given Snowflake user using a CTE.
    Expects 'user_name' as a query parameter.
    """
    user_name_param = request.args.get('user_name')
    if not user_name_param:
        return jsonify({"error": "Missing 'user_name' parameter"}), 400

    user_name_upper = user_name_param.strip().upper() # Normalize to uppercase
    
    cte_query = """
    WITH RECURSIVE user_role_hierarchy (
        level,
        role_name,
        granted_via_type,
        granted_via_name,
        path_string
    ) AS (
        -- Anchor Member: Roles directly granted to the user
        SELECT
            0 AS level,
            gtu."ROLE" AS role_name, 
            'USER' AS granted_via_type,
            gtu."GRANTEE_NAME" AS granted_via_name, 
            gtu."ROLE" AS path_string
        FROM
            snowflake.account_usage.grants_to_users gtu
        WHERE
            gtu."GRANTEE_NAME" = %s 
            AND gtu."DELETED_ON" IS NULL

        UNION ALL

        -- Recursive Member: Roles granted to the roles found in the previous iteration
        SELECT
            urh.level + 1,
            gtr."NAME" AS role_name,                 
            'ROLE' AS granted_via_type,
            gtr."GRANTEE_NAME" AS granted_via_name,  
            urh.path_string || ' -> ' || gtr."NAME" AS path_string
        FROM
            snowflake.account_usage.grants_to_roles gtr
        JOIN
            user_role_hierarchy urh ON gtr."GRANTEE_NAME" = urh.role_name
        WHERE
            gtr."PRIVILEGE" = 'USAGE'
            AND gtr."GRANTED_ON" = 'ROLE' 
            AND gtr."DELETED_ON" IS NULL
            AND urh.level < 20 
    )
    -- Final SELECT statement
    SELECT
        level,
        LPAD(' ', level * 4) || role_name AS indented_role_name,
        granted_via_type,
        granted_via_name AS granted_directly_to,
        path_string
    FROM
        user_role_hierarchy
    ORDER BY
        path_string;
    """

    grant_chains = []

    try:
        # Ensure you have a valid Snowflake connection configuration
        # For named connections, it must be defined in your ~/.snowflake/connections.toml
        connection = snowflake.connector.connect(
            connection_name="bsabwew-sj64889" 
            # user=os.getenv('SNOWFLAKE_APP_USER'), 
            # password=os.getenv('SNOWFLAKE_APP_PASSWORD'),
            # account=os.getenv('SNOWFLAKE_ACCOUNT_LOCATOR'),
            # warehouse=os.getenv('SNOWFLAKE_APP_WAREHOUSE'), # Recommended
            # database=os.getenv('SNOWFLAKE_APP_DATABASE'),   # Recommended
            # schema=os.getenv('SNOWFLAKE_APP_SCHEMA'),     # Recommended
            # role=os.getenv('SNOWFLAKE_APP_ROLE') 
        )
        cur = connection.cursor()
        
        try:
            print(f"Backend: Executing CTE for user: {user_name_upper}")
            cur.execute(cte_query, (user_name_upper,))
            results = cur.fetchall()

            if not results:
                return jsonify([f"No role hierarchy found for user '{user_name_param}'. User may not exist, have no roles, or grants are not visible."])

            for row in results:
                grant_chains.append(row[4]) # path_string is at index 4
        
        finally:
            if cur: cur.close()
            if connection: connection.close()
        
        return jsonify(grant_chains)

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Backend: Snowflake Programming Error: {e}")
        error_detail = str(e)
        if "object does not exist or not authorized" in error_detail.lower():
             return jsonify({"error": f"User '{user_name_param}' may not exist, or the connecting role lacks permissions to view its grants or the ACCOUNT_USAGE schema.", "details": error_detail}), 404
        if "connection_name" in error_detail.lower() and "not found" in error_detail.lower():
            return jsonify({"error": f"Snowflake connection name 'bsabwew-sj64889' not found. Please configure it in ~/.snowflake/connections.toml", "details": error_detail}), 500
        return jsonify({"error": "Error querying Snowflake for role hierarchy (CTE).", "details": error_detail}), 500
    except ValueError as e: 
        print(f"Backend: Configuration error: {e}")
        return jsonify({"error": "Server configuration error (e.g., Snowflake connection).", "details": str(e)}), 500
    except Exception as e:
        print(f"Backend: An unexpected error occurred: {e}")
        return jsonify({"error": "An unexpected error occurred on the server while fetching role hierarchy (CTE).", "details": str(e)}), 500


def run_flask_in_thread():
    """Runs the Flask app in a separate thread."""
    print(f"Starting Flask server on {FLASK_HOST}:{FLASK_PORT}...")
    # Set use_reloader to False when running in a thread to avoid issues
    flask_app.run(host=FLASK_HOST, port=FLASK_PORT, debug=False, use_reloader=False)

# ==============================================================================
# PYQT6 FRONTEND APPLICATION
# ==============================================================================

class FetchWorker(QThread):
    data_ready = pyqtSignal(list)
    error_occurred = pyqtSignal(str)

    def __init__(self, full_api_url, params=None): 
        super().__init__()
        self.full_api_url = full_api_url
        self.params = params if params is not None else {}
        self._is_running = True

    def run(self):
        if not self._is_running: return
        try:
            print(f"PyQt Worker: Fetching data from {self.full_api_url} with params {self.params}")
            response = requests.get(self.full_api_url, params=self.params, timeout=45) 
            response.raise_for_status()
            
            if not self._is_running: return # Check again after request
            data = response.json()
            if isinstance(data, list): 
                self.data_ready.emit(data)
            else: # Handle cases where backend might return an error object not in a list
                if isinstance(data, dict) and 'error' in data:
                     self.error_occurred.emit(f"Backend error: {data.get('error')} (Details: {data.get('details', 'N/A')})")
                else:
                    self.error_occurred.emit("Received unexpected data format from the server.")
        except requests.exceptions.Timeout:
            if self._is_running: self.error_occurred.emit("The request timed out. Please check the backend server or query complexity.")
        except requests.exceptions.HTTPError as http_err:
            if self._is_running:
                error_message = f"HTTP error: {http_err.response.status_code} {http_err.response.reason}"
                try: # Try to get more details from JSON response if possible
                    err_details = http_err.response.json()
                    if "error" in err_details: error_message += f" - {err_details.get('error')}"
                    if "details" in err_details: error_message += f" (Details: {err_details.get('details')})"
                except ValueError: # response was not JSON
                    pass # error_message remains as is
                self.error_occurred.emit(error_message)
        except requests.exceptions.RequestException as e: # General network/connection error
            if self._is_running: self.error_occurred.emit(f"Error fetching data: {e}. Is the backend server running correctly?")
        except ValueError as e: # JSON decoding error
             if self._is_running: self.error_occurred.emit(f"Error decoding JSON response: {e}")

    def stop(self):
        self._is_running = False

class SnowflakeViewerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Snowflake Role Hierarchy Explorer") 
        self.setGeometry(100, 100, 900, 700) 
        self.fetch_worker = None

        # --- Menu Bar ---
        self._create_menu_bar()

        self.flask_thread = threading.Thread(target=run_flask_in_thread, daemon=True)
        self.flask_thread.start()
        print("Flask backend thread started for role hierarchy.")

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(15) 
        main_layout.setContentsMargins(20, 20, 20, 20)

        title_label = QLabel("Snowflake Role Hierarchy Explorer")
        title_font = QFont("Arial", 20, QFont.Weight.Bold)
        title_label.setFont(title_font)
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(title_label)

        input_layout = QHBoxLayout() 
        input_layout.setSpacing(10)

        user_name_label = QLabel("Snowflake User Name:")
        self.user_name_input = QLineEdit()
        self.user_name_input.setPlaceholderText("e.g., ALICE_GORDON (case-insensitive)")
        self.user_name_input.setToolTip("Enter the Snowflake username to see their role hierarchy.")
        self.user_name_input.returnPressed.connect(self.start_fetch_data) # Allow Enter to trigger fetch
        input_layout.addWidget(user_name_label)
        input_layout.addWidget(self.user_name_input, 1) 
        main_layout.addLayout(input_layout)
        
        backend_info_layout = QHBoxLayout()
        backend_url_label = QLabel("Backend API (Embedded):")
        self.backend_url_display = QLineEdit(f"{BACKEND_API_BASE_URL}/api/role-hierarchy")
        self.backend_url_display.setReadOnly(True)
        self.backend_url_display.setStyleSheet("background-color: #f0f0f0;") 
        backend_info_layout.addWidget(backend_url_label)
        backend_info_layout.addWidget(self.backend_url_display,1)
        main_layout.addLayout(backend_info_layout)

        # --- Buttons Layout (Fetch and Copy) ---
        buttons_main_layout = QHBoxLayout() 
        buttons_main_layout.addStretch()

        buttons_group_layout = QHBoxLayout() 
        buttons_group_layout.setSpacing(10)

        self.fetch_button = QPushButton("Get Role Hierarchy") 
        self.fetch_button.setFont(QFont("Arial", 10, QFont.Weight.DemiBold))
        self.fetch_button.setStyleSheet("""
            QPushButton { background-color: #3B82F6; color: white; padding: 10px 15px; border: none; border-radius: 5px; min-width: 180px; }
            QPushButton:hover { background-color: #2563EB; }
            QPushButton:pressed { background-color: #1D4ED8; }
            QPushButton:disabled { background-color: #9CA3AF; color: #E5E7EB; }
        """)
        self.fetch_button.clicked.connect(self.start_fetch_data)
        buttons_group_layout.addWidget(self.fetch_button)

        self.copy_button = QPushButton("Copy Results") 
        self.copy_button.setFont(QFont("Arial", 10, QFont.Weight.DemiBold))
        self.copy_button.setStyleSheet("""
            QPushButton { background-color: #10B981; color: white; padding: 10px 15px; border: none; border-radius: 5px; min-width: 120px; }
            QPushButton:hover { background-color: #059669; }
            QPushButton:pressed { background-color: #047857; }
            QPushButton:disabled { background-color: #9CA3AF; color: #E5E7EB; }
        """)
        self.copy_button.clicked.connect(self.copy_table_to_clipboard)
        self.copy_button.setEnabled(False) 
        buttons_group_layout.addWidget(self.copy_button)
        
        buttons_main_layout.addLayout(buttons_group_layout)
        buttons_main_layout.addStretch()
        main_layout.addLayout(buttons_main_layout)


        self.table_widget = QTableWidget()
        self.table_widget.setColumnCount(1) 
        self.table_widget.setHorizontalHeaderLabels(["Role Grant Chain"]) 
        header = self.table_widget.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch) 
        header.setStyleSheet("""
            QHeaderView::section { background-color: #4A5568; color: white; padding: 4px; border: 1px solid #6B7280; font-weight: bold; }
        """)
        self.table_widget.setAlternatingRowColors(True)
        self.table_widget.setStyleSheet("""
            QTableWidget { gridline-color: #D1D5DB; alternate-background-color: #F3F4F6; background-color: white; font-size: 9pt; }
            QTableWidget::item { padding: 3px; }
        """)
        self.table_widget.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        main_layout.addWidget(self.table_widget)

        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready. Enter Snowflake User Name and click 'Get Role Hierarchy'.")

    def _create_menu_bar(self):
        menu_bar = self.menuBar()
        # File Menu
        file_menu = menu_bar.addMenu("&File")

        self.save_action = QAction("&Save Output to File...", self)
        self.save_action.setStatusTip("Save the current table output to a text file")
        self.save_action.triggered.connect(self.save_output_to_file)
        self.save_action.setEnabled(False) # Initially disabled
        file_menu.addAction(self.save_action)

        file_menu.addSeparator()

        exit_action = QAction("&Exit", self)
        exit_action.setStatusTip("Exit application")
        exit_action.triggered.connect(self.close) # QMainWindow's close method
        file_menu.addAction(exit_action)

    def save_output_to_file(self):
        if self.table_widget.rowCount() == 0:
            self.status_bar.showMessage("Nothing to save.", 3000)
            return

        # Suggest a default filename including the username if available
        user_name = self.user_name_input.text().strip().replace(" ", "_")
        default_filename = f"snowflake_role_hierarchy_{user_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt" if user_name else f"snowflake_role_hierarchy_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Output As",
            default_filename,
            "Text Files (*.txt);;All Files (*)"
        )

        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    for row in range(self.table_widget.rowCount()):
                        item = self.table_widget.item(row, 0) # Only one column
                        if item and item.text():
                            f.write(item.text() + "\n")
                self.status_bar.showMessage(f"Output successfully saved to {file_path}", 5000)
            except IOError as e:
                self.status_bar.showMessage(f"Error saving file: {e}", 5000)
                QMessageBox.critical(self, "Save Error", f"Could not save file to '{file_path}':\n{e}")
        else:
            self.status_bar.showMessage("Save operation cancelled.", 3000)


    def start_fetch_data(self):
        if self.fetch_worker and self.fetch_worker.isRunning():
            self.status_bar.showMessage("A fetch operation is already in progress...", 3000)
            return

        user_name = self.user_name_input.text().strip()
        if not user_name:
            QMessageBox.warning(self, "Input Error", "Snowflake User Name cannot be empty.")
            return
        
        full_api_url = f"{BACKEND_API_BASE_URL}/api/role-hierarchy"
        params = {'user_name': user_name}
        
        self.status_bar.showMessage(f"Fetching role hierarchy for '{user_name}'...")
        self.fetch_button.setEnabled(False)
        self.copy_button.setEnabled(False) 
        self.save_action.setEnabled(False) # Disable save action during fetch
        self.table_widget.setRowCount(0) 

        self.fetch_worker = FetchWorker(full_api_url, params) 
        self.fetch_worker.data_ready.connect(self.populate_table)
        self.fetch_worker.error_occurred.connect(self.show_fetch_error)
        self.fetch_worker.finished.connect(self.on_fetch_finished) 
        self.fetch_worker.start()

    def populate_table(self, grant_chains_list): 
        self.table_widget.setRowCount(0) 
        if not grant_chains_list or not grant_chains_list[0] or "No role hierarchy found" in grant_chains_list[0] or "has no direct role grants" in grant_chains_list[0]:
            message = grant_chains_list[0] if grant_chains_list and grant_chains_list[0] else "No role grant chains found or user has no roles."
            self.status_bar.showMessage(message, 5000)
            self.copy_button.setEnabled(False) 
            self.save_action.setEnabled(False) 
            if grant_chains_list and grant_chains_list[0] and ("No role hierarchy found" in grant_chains_list[0] or "has no direct role grants" in grant_chains_list[0]):
                self.table_widget.setRowCount(1)
                self.table_widget.setItem(0, 0, QTableWidgetItem(grant_chains_list[0]))
            return

        self.table_widget.setRowCount(len(grant_chains_list))
        for row_index, chain_string in enumerate(grant_chains_list):
            self.table_widget.setItem(row_index, 0, QTableWidgetItem(chain_string))
        
        self.status_bar.showMessage(f"Successfully fetched {len(grant_chains_list)} role grant chains.", 5000)
        self.copy_button.setEnabled(True) 
        self.save_action.setEnabled(True) # Enable save action when data is populated

    def show_fetch_error(self, error_message):
        self.status_bar.showMessage(f"Error: {error_message}", 10000)
        self.copy_button.setEnabled(False) 
        self.save_action.setEnabled(False) # Disable save on error
        QMessageBox.critical(self, "Fetch Error", error_message)

    def on_fetch_finished(self):
        self.fetch_button.setEnabled(True)
        # Enable copy/save only if there's actual data in the table
        enable_actions = False
        if self.table_widget.rowCount() > 0:
            first_item = self.table_widget.item(0,0)
            if first_item and not ("No role hierarchy found" in first_item.text() or "has no direct role grants" in first_item.text()):
                enable_actions = True
        
        self.copy_button.setEnabled(enable_actions)
        self.save_action.setEnabled(enable_actions) # Correctly set save action state

        if self.fetch_worker: 
            self.fetch_worker.deleteLater() 
            self.fetch_worker = None
        print("PyQt Fetch operation for role hierarchy finished.")

    def copy_table_to_clipboard(self):
        """Copies the content of the table to the clipboard."""
        if self.table_widget.rowCount() == 0:
            self.status_bar.showMessage("Nothing to copy.", 3000)
            return

        clipboard_text = []
        for row in range(self.table_widget.rowCount()):
            item = self.table_widget.item(row, 0) # Only one column
            if item and item.text():
                clipboard_text.append(item.text())
        
        if clipboard_text:
            QGuiApplication.clipboard().setText("\n".join(clipboard_text))
            self.status_bar.showMessage(f"Copied {len(clipboard_text)} rows to clipboard.", 3000)
        else:
            self.status_bar.showMessage("No data in table to copy.", 3000)


    def closeEvent(self, event):
        print("Attempting to close application (role hierarchy viewer)...")
        if self.fetch_worker and self.fetch_worker.isRunning():
            print("Window closing, stopping fetch worker thread...")
            self.fetch_worker.stop() 
            self.fetch_worker.wait(1000) # Wait for a bit for the thread to finish
        # No explicit Flask server shutdown here as it's a daemon thread
        # and will exit when the main application exits.
        print("Exiting application.")
        event.accept()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    viewer = SnowflakeViewerApp()
    viewer.show()
    sys.exit(app.exec())