# trigger_silver_dbt_gui.py

import json
import shlex
import sys

from PyQt6.QtCore import QProcess
from PyQt6.QtWidgets import (
    QApplication,
    QFormLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QTextEdit,
    QToolButton,
    QVBoxLayout,
    QWidget,
)


class DbtTriggerWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Silver Layer dbt Trigger")
        self.setMinimumWidth(700)

        self.process = None
        self.debug_log = ""

        self.create_ui()

    def create_ui(self):
        central_widget = QWidget()
        main_layout = QVBoxLayout()

        form_layout = QFormLayout()

        self.insert_statement_input = QLineEdit()
        self.insert_statement_input.setMinimumWidth(300)
        self.insert_statement_input.setPlaceholderText("Name des dbt Models")

        self.inr_fkey_input = QLineEdit()
        self.inr_fkey_input.setMinimumWidth(300)
        self.inr_fkey_input.setPlaceholderText("Name der DB-Schema")

        self.bdat_input = QLineEdit()
        self.bdat_input.setMinimumWidth(300)
        self.bdat_input.setPlaceholderText("Optional Buchungstag, z.B. 2026-04-24")

        form_layout.addRow("INSERT_STATEMENT", self.insert_statement_input)
        form_layout.addRow("INR_FKEY", self.inr_fkey_input)
        form_layout.addRow("BDAT (Optional)", self.bdat_input)

        main_layout.addLayout(form_layout)

        self.run_button = QPushButton("Run dbt")
        self.run_button.clicked.connect(self.run_dbt)
        main_layout.addWidget(self.run_button)

        self.result_label = QLabel("dbt has not been run yet.")
        self.result_label.setStyleSheet("color: gray;")
        main_layout.addWidget(self.result_label)

        self.debug_toggle_button = QToolButton()
        self.debug_toggle_button.setText("Show dbt debug log ▼")
        self.debug_toggle_button.setCheckable(True)
        self.debug_toggle_button.setEnabled(False)
        self.debug_toggle_button.clicked.connect(self.toggle_debug_log)
        main_layout.addWidget(self.debug_toggle_button)

        self.debug_text = QTextEdit()
        self.debug_text.setReadOnly(True)
        self.debug_text.setVisible(False)
        main_layout.addWidget(self.debug_text)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setVisible(False)
        main_layout.addWidget(self.progress_bar)

        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)

    def build_dbt_command(self):
        insert_statement = self.insert_statement_input.text().strip()
        inr_fkey = self.inr_fkey_input.text().strip()
        bdat = self.bdat_input.text().strip()

        dbt_vars = {
            "INR_FKEY": inr_fkey
        }

        if bdat:
            dbt_vars["BDAT"] = bdat

        command = [
            "dbt",
            "--debug",
            "run",
            "--select",
            insert_statement,
            "--vars",
            json.dumps(dbt_vars)
        ]

        return command

    def run_dbt(self):
        insert_statement = self.insert_statement_input.text().strip()
        inr_fkey = self.inr_fkey_input.text().strip()

        if not insert_statement:
            QMessageBox.critical(
                self,
                "Missing parameter",
                "INSERT_STATEMENT is required."
            )
            return

        if not inr_fkey:
            QMessageBox.critical(
                self,
                "Missing parameter",
                "INR_FKEY is required."
            )
            return

        command = self.build_dbt_command()

        self.prepare_gui_for_run(command)

        self.process = QProcess(self)
        self.process.readyReadStandardOutput.connect(self.capture_stdout)
        self.process.readyReadStandardError.connect(self.capture_stderr)
        self.process.finished.connect(self.finish_dbt_run)

        program = command[0]
        arguments = command[1:]

        self.process.start(program, arguments)

    def prepare_gui_for_run(self, command):
        self.debug_log = ""

        readable_command = shlex.join(command)
        self.debug_log += "Executed command:\n"
        self.debug_log += readable_command
        self.debug_log += "\n\n"

        self.debug_text.clear()
        self.debug_text.setVisible(False)

        self.debug_toggle_button.setChecked(False)
        self.debug_toggle_button.setText("Show dbt debug log ▼")
        self.debug_toggle_button.setEnabled(False)

        self.result_label.setText("dbt is running...")
        self.result_label.setStyleSheet("color: blue;")

        self.run_button.setEnabled(False)
        self.progress_bar.setVisible(True)

    def capture_stdout(self):
        output = bytes(self.process.readAllStandardOutput()).decode("utf-8", errors="replace")

        self.debug_log += "\n"
        self.debug_log += output
        self.debug_log += "\n"

    def capture_stderr(self):
        error_output = bytes(self.process.readAllStandardError()).decode("utf-8", errors="replace")

        self.debug_log += "\n"
        self.debug_log += error_output
        self.debug_log += "\n"

    def finish_dbt_run(self, exit_code, exit_status):
        self.progress_bar.setVisible(False)
        self.run_button.setEnabled(True)
        self.debug_toggle_button.setEnabled(True)

        self.debug_text.setPlainText(self.debug_log)

        if exit_code == 0:
            self.result_label.setText("dbt operation erfolgreich abgeschlossen.")
            self.result_label.setStyleSheet("color: green;")
        else:
            self.result_label.setText(f"dbt operation fehlgeschlagen. Rückgabecode: {exit_code}")
            self.result_label.setStyleSheet("color: red;")

    def toggle_debug_log(self):
        if self.debug_toggle_button.isChecked():
            self.debug_text.setVisible(True)
            self.debug_toggle_button.setText("Hide dbt debug log ▲")
        else:
            self.debug_text.setVisible(False)
            self.debug_toggle_button.setText("Show dbt debug log ▼")


def main():
    app = QApplication(sys.argv)

    window = DbtTriggerWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()