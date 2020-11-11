
from datetime import date
from functools import partial
from os.path import abspath, dirname, join
import sys

from PyQt4 import QtGui, QtCore, uic

from sciafeed import DESIGNER_PATH
from sciafeed.hiscentral import REGION_IDS_MAP
from sciafeed.db_utils import DEFAULT_DB_URI
from sciafeed.designer.download_hiscentral_window import Ui_download_hiscentral_form
from sciafeed.designer.download_er_window import Ui_download_er_form
from sciafeed.designer.make_report import Ui_make_report_form
from sciafeed.designer.make_reports import Ui_make_reports_form
from sciafeed.designer.find_new_stations import Ui_find_new_stations_form
from sciafeed.designer.upsert_stations import Ui_upsert_stations_form
from sciafeed.designer.compute_daily_indicators import Ui_compute_daily_indicators_form
from sciafeed.designer.insert_daily_indicators import Ui_insert_daily_indicators_form
from sciafeed.designer.check_chain import Ui_check_chain_form
from sciafeed.designer.compute_daily_indicators2 import Ui_compute_indicators2_form
from sciafeed.designer.load_unique_data import Ui_load_unique_data_form

import pdb
def import_pdb():
  from PyQt4.QtCore import pyqtRemoveInputHook
  pyqtRemoveInputHook()


class LoadUniqueData(QtGui.QMainWindow, Ui_load_unique_data_form):
    bin_name = 'load_unique_data'
    close_signal = QtCore.pyqtSignal()
    run_signal = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(LoadUniqueData, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.base_setup()

    def terminal_redirect(self):
        cursor = self.output_console.textCursor()
        cursor.movePosition(cursor.End)
        cursor.insertText(str(self.process.readAll(), 'utf-8'))
        self.output_console.ensureCursorVisible()
        scrollbar = self.output_console.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def closeEvent(self, event):
        self.close_signal.emit()

    def run_script(self):
        self.run_signal.emit()

    def base_setup(self):
        # terminal stuff
        self.output_console.setStyleSheet("background-color: rgb(0, 0, 0)")
        self.process = QtCore.QProcess(self)
        self.process.readyRead.connect(self.terminal_redirect)
        self.process.started.connect(lambda: self.script_run.setEnabled(False))
        self.process.finished.connect(lambda: self.script_run.setEnabled(True))
        self.process.setProcessChannelMode(QtCore.QProcess.MergedChannels)

        # connect run/close buttons
        self.script_close.clicked.connect(self.close)
        self.script_run.clicked.connect(self.run_script)

    def setupUi(self, Ui_load_unique_data_form):
        super(LoadUniqueData, self).setupUi(Ui_load_unique_data_form)
        # buttons for selecting inputs
        self.select_report_button.clicked.connect(self.on_select_report_clicked)
        # other ui setup
        self.input_dburi.setText(DEFAULT_DB_URI)
        self.input_source_schema.setText('dailypdbanpacarica')
        self.input_target_schema.setText('dailypdbanpaclima')

    def on_select_report_clicked(self):
        caption = 'seleziona report destinazione'
        filepath = str(QtGui.QFileDialog.getSaveFileName(self, caption))
        self.input_report.setText(filepath)

    def collect_inputs(self):
        kwargs = dict()
        kwargs['dburi'] = str(self.input_dburi.text().strip())
        kwargs['startschema'] = str(self.input_source_schema.text().strip())
        kwargs['targetschema'] = str(self.input_target_schema.text().strip())
        report_path = str(self.input_report.text()).strip()
        kwargs['report_path'] = report_path
        return kwargs

    def generate_cmd(self, bin_path, kwargs):
        cmd = join(bin_path, self.bin_name)
        args = []
        if kwargs['startschema']:
            args += ['-s', kwargs['startschema']]
        if kwargs['targetschema']:
            args += ['-t', kwargs['targetschema']]
        if kwargs['dburi']:
            args += ['-d', kwargs['dburi']]
        if kwargs['report_path']:
            args += ['-r', kwargs['report_path']]
        return cmd, args


class ComputeDailyIndicators2(QtGui.QMainWindow, Ui_compute_indicators2_form):
    bin_name = 'compute_daily_indicators2'
    close_signal = QtCore.pyqtSignal()
    run_signal = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(ComputeDailyIndicators2, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.base_setup()

    def terminal_redirect(self):
        cursor = self.output_console.textCursor()
        cursor.movePosition(cursor.End)
        cursor.insertText(str(self.process.readAll(), 'utf-8'))
        self.output_console.ensureCursorVisible()
        scrollbar = self.output_console.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def closeEvent(self, event):
        self.close_signal.emit()

    def run_script(self):
        self.run_signal.emit()

    def base_setup(self):
        # terminal stuff
        self.output_console.setStyleSheet("background-color: rgb(0, 0, 0)")
        self.process = QtCore.QProcess(self)
        self.process.readyRead.connect(self.terminal_redirect)
        self.process.started.connect(lambda: self.script_run.setEnabled(False))
        self.process.finished.connect(lambda: self.script_run.setEnabled(True))
        self.process.setProcessChannelMode(QtCore.QProcess.MergedChannels)

        # connect run/close buttons
        self.script_close.clicked.connect(self.close)
        self.script_run.clicked.connect(self.run_script)

    def setupUi(self, Ui_compute_indicators2_form):
        super(ComputeDailyIndicators2, self).setupUi(Ui_compute_indicators2_form)
        # buttons for selecting inputs
        self.select_report_button.clicked.connect(self.on_select_report_clicked)
        # other ui setup
        self.input_dburi.setText(DEFAULT_DB_URI)
        self.input_schema.setText('dailypdbanpacarica')

    def on_select_report_clicked(self):
        caption = 'seleziona report destinazione'
        filepath = str(QtGui.QFileDialog.getSaveFileName(self, caption))
        self.input_report.setText(filepath)

    def collect_inputs(self):
        kwargs = dict()
        kwargs['dburi'] = str(self.input_dburi.text().strip())
        kwargs['schema'] = str(self.input_schema.text().strip())
        report_path = str(self.input_report.text()).strip()
        kwargs['report_path'] = report_path
        return kwargs

    def generate_cmd(self, bin_path, kwargs):
        cmd = join(bin_path, self.bin_name)
        args = []
        if kwargs['schema']:
            args += ['-s', kwargs['schema']]
        if kwargs['dburi']:
            args += ['-d', kwargs['dburi']]
        if kwargs['report_path']:
            args += ['-r', kwargs['report_path']]
        return cmd, args


class CheckChain(QtGui.QMainWindow, Ui_check_chain_form):
    bin_name = 'check_chain'
    close_signal = QtCore.pyqtSignal()
    run_signal = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(CheckChain, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.base_setup()

    def terminal_redirect(self):
        cursor = self.output_console.textCursor()
        cursor.movePosition(cursor.End)
        cursor.insertText(str(self.process.readAll(), 'utf-8'))
        self.output_console.ensureCursorVisible()
        scrollbar = self.output_console.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def closeEvent(self, event):
        self.close_signal.emit()

    def run_script(self):
        self.run_signal.emit()

    def base_setup(self):
        # terminal stuff
        self.output_console.setStyleSheet("background-color: rgb(0, 0, 0)")
        self.process = QtCore.QProcess(self)
        self.process.readyRead.connect(self.terminal_redirect)
        self.process.started.connect(lambda: self.script_run.setEnabled(False))
        self.process.finished.connect(lambda: self.script_run.setEnabled(True))
        self.process.setProcessChannelMode(QtCore.QProcess.MergedChannels)

        # connect run/close buttons
        self.script_close.clicked.connect(self.close)
        self.script_run.clicked.connect(self.run_script)

    def setupUi(self, Ui_check_chain_form):
        super(CheckChain, self).setupUi(Ui_check_chain_form)
        # buttons for selecting inputs
        self.select_report_button.clicked.connect(self.on_select_report_clicked)
        # other ui setup
        self.input_dburi.setText(DEFAULT_DB_URI)
        self.input_schema.setText('dailypdbanpacarica')

    def on_select_report_clicked(self):
        caption = 'seleziona report destinazione'
        filepath = str(QtGui.QFileDialog.getSaveFileName(self, caption))
        self.input_report.setText(filepath)

    def collect_inputs(self):
        kwargs = dict()
        kwargs['dburi'] = str(self.input_dburi.text().strip())
        kwargs['station_where'] = str(self.input_where.text().strip())
        kwargs['schema'] = str(self.input_schema.text().strip())
        kwargs['omit_flagsync'] = self.input_omitflag.isChecked()
        report_path = str(self.input_report.text()).strip()
        kwargs['report_path'] = report_path
        return kwargs

    def generate_cmd(self, bin_path, kwargs):
        cmd = join(bin_path, self.bin_name)
        args = []
        if kwargs['station_where']:
            args += ['-w', kwargs['station_where']]
        if kwargs['schema']:
            args += ['-s', kwargs['schema']]
        if kwargs['dburi']:
            args += ['-d', kwargs['dburi']]
        if kwargs['report_path']:
            args += ['-r', kwargs['report_path']]
        if kwargs['omit_flagsync']:
            args += ['--omit_flagsync']
        return cmd, args


class InsertDailyIndicators(QtGui.QMainWindow, Ui_insert_daily_indicators_form):
    bin_name = 'insert_daily_indicators'
    close_signal = QtCore.pyqtSignal()
    run_signal = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(InsertDailyIndicators, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.base_setup()

    def terminal_redirect(self):
        cursor = self.output_console.textCursor()
        cursor.movePosition(cursor.End)
        cursor.insertText(str(self.process.readAll(), 'utf-8'))
        self.output_console.ensureCursorVisible()
        scrollbar = self.output_console.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def closeEvent(self, event):
        self.close_signal.emit()

    def run_script(self):
        self.run_signal.emit()

    def base_setup(self):
        # terminal stuff
        self.output_console.setStyleSheet("background-color: rgb(0, 0, 0)")
        self.process = QtCore.QProcess(self)
        self.process.readyRead.connect(self.terminal_redirect)
        self.process.started.connect(lambda: self.script_run.setEnabled(False))
        self.process.finished.connect(lambda: self.script_run.setEnabled(True))
        self.process.setProcessChannelMode(QtCore.QProcess.MergedChannels)

        # connect run/close buttons
        self.script_close.clicked.connect(self.close)
        self.script_run.clicked.connect(self.run_script)

    def setupUi(self, Ui_insert_daily_indicators_form):
        super(InsertDailyIndicators, self).setupUi(Ui_insert_daily_indicators_form)
        # buttons for selecting inputs
        self.select_input_folder_button.clicked.connect(self.on_select_input_folder_clicked)
        self.select_report_button.clicked.connect(self.on_select_report_clicked)
        # other ui setup
        self.input_dburi.setText(DEFAULT_DB_URI)
        self.input_schema.setText('dailypdbanpacarica')

    def on_select_input_folder_clicked(self):
        caption = 'seleziona cartella di input'
        folderpath = str(QtGui.QFileDialog.getExistingDirectory(self, caption))
        self.input_folder.setText(folderpath)

    def on_select_report_clicked(self):
        caption = 'seleziona report destinazione'
        filepath = str(QtGui.QFileDialog.getSaveFileName(self, caption))
        self.input_report.setText(filepath)

    def collect_inputs(self):
        kwargs = dict()
        kwargs['dburi'] = str(self.input_dburi.text().strip())
        kwargs['data_folder'] = str(self.input_folder.text().strip())
        kwargs['schema'] = str(self.input_schema.text().strip())
        policy = self.input_policy.currentText()
        if policy == 'SOLO INSERT':
            kwargs['policy'] = 'onlyinsert'
        else:
            kwargs['policy'] = 'upsert'
        report_path = str(self.input_report.text()).strip()
        kwargs['report_path'] = report_path
        return kwargs

    def generate_cmd(self, bin_path, kwargs):
        cmd = join(bin_path, self.bin_name)
        args = []
        if kwargs['data_folder']:
            args += [kwargs['data_folder']]
        if kwargs['schema']:
            args += ['-s', kwargs['schema']]
        if kwargs['dburi']:
            args += ['-d', kwargs['dburi']]
        if kwargs['report_path']:
            args += ['-r', kwargs['report_path']]
        if kwargs['policy']:
            args += ['-p', kwargs['policy']]
        return cmd, args


class ComputeDailyIndicators(QtGui.QMainWindow, Ui_compute_daily_indicators_form):
    bin_name = 'compute_daily_indicators'
    close_signal = QtCore.pyqtSignal()
    run_signal = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(ComputeDailyIndicators, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.base_setup()

    def terminal_redirect(self):
        cursor = self.output_console.textCursor()
        cursor.movePosition(cursor.End)
        cursor.insertText(str(self.process.readAll(), 'utf-8'))
        self.output_console.ensureCursorVisible()
        scrollbar = self.output_console.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def closeEvent(self, event):
        self.close_signal.emit()

    def run_script(self):
        self.run_signal.emit()

    def base_setup(self):
        # terminal stuff
        self.output_console.setStyleSheet("background-color: rgb(0, 0, 0)")
        self.process = QtCore.QProcess(self)
        self.process.readyRead.connect(self.terminal_redirect)
        self.process.started.connect(lambda: self.script_run.setEnabled(False))
        self.process.finished.connect(lambda: self.script_run.setEnabled(True))
        self.process.setProcessChannelMode(QtCore.QProcess.MergedChannels)

        # connect run/close buttons
        self.script_close.clicked.connect(self.close)
        self.script_run.clicked.connect(self.run_script)

    def setupUi(self, Ui_compute_daily_indicators_form):
        super(ComputeDailyIndicators, self).setupUi(Ui_compute_daily_indicators_form)
        # buttons for selecting inputs
        self.select_input_folder_button.clicked.connect(self.on_select_input_folder_clicked)
        self.select_output_folder_button.clicked.connect(self.on_select_output_folder_clicked)
        self.select_report_button.clicked.connect(self.on_select_report_clicked)
        # other ui setup
        self.input_dburi.setText(DEFAULT_DB_URI)

    def on_select_input_folder_clicked(self):
        caption = 'seleziona cartella di input'
        folderpath = str(QtGui.QFileDialog.getExistingDirectory(self, caption))
        self.input_folder.setText(folderpath)

    def on_select_output_folder_clicked(self):
        caption = 'seleziona cartella di output'
        folderpath = str(QtGui.QFileDialog.getExistingDirectory(self, caption))
        self.output_folder.setText(folderpath)

    def on_select_report_clicked(self):
        caption = 'seleziona report destinazione'
        filepath = str(QtGui.QFileDialog.getSaveFileName(self, caption))
        self.input_report.setText(filepath)

    def collect_inputs(self):
        kwargs = dict()
        kwargs['dburi'] = str(self.input_dburi.text().strip())
        kwargs['data_folder'] = str(self.input_folder.text().strip())
        kwargs['indicators_folder'] = str(self.output_folder.text().strip())
        report_path = str(self.input_report.text()).strip()
        kwargs['report_path'] = report_path
        return kwargs

    def generate_cmd(self, bin_path, kwargs):
        cmd = join(bin_path, self.bin_name)
        args = []
        if kwargs['data_folder']:
            args += [kwargs['data_folder']]
        if kwargs['indicators_folder']:
            args += [kwargs['indicators_folder']]
        if kwargs['dburi']:
            args += ['-d', kwargs['dburi']]
        if kwargs['report_path']:
            args += ['-r', kwargs['report_path']]
        return cmd, args


class UpsertStations(QtGui.QMainWindow, Ui_upsert_stations_form):
    bin_name = 'upsert_stations'
    close_signal = QtCore.pyqtSignal()
    run_signal = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(UpsertStations, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.base_setup()

    def terminal_redirect(self):
        cursor = self.output_console.textCursor()
        cursor.movePosition(cursor.End)
        cursor.insertText(str(self.process.readAll(), 'utf-8'))
        self.output_console.ensureCursorVisible()
        scrollbar = self.output_console.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def closeEvent(self, event):
        self.close_signal.emit()

    def run_script(self):
        self.run_signal.emit()

    def base_setup(self):
        # terminal stuff
        self.output_console.setStyleSheet("background-color: rgb(0, 0, 0)")
        self.process = QtCore.QProcess(self)
        self.process.readyRead.connect(self.terminal_redirect)
        self.process.started.connect(lambda: self.script_run.setEnabled(False))
        self.process.finished.connect(lambda: self.script_run.setEnabled(True))
        self.process.setProcessChannelMode(QtCore.QProcess.MergedChannels)

        # connect run/close buttons
        self.script_close.clicked.connect(self.close)
        self.script_run.clicked.connect(self.run_script)

    def setupUi(self, Ui_upsert_stations_form):
        super(UpsertStations, self).setupUi(Ui_upsert_stations_form)
        # buttons for selecting inputs
        self.select_input_file_button.clicked.connect(self.on_select_input_file_clicked)
        self.select_report_button.clicked.connect(self.on_select_report_clicked)
        # other ui setup
        self.input_dburi.setText(DEFAULT_DB_URI)

    def on_select_input_file_clicked(self):
        caption = 'seleziona file di input'
        filepath = str(QtGui.QFileDialog.getOpenFileName(self, caption))
        self.input_file.setText(filepath)

    def on_select_report_clicked(self):
        caption = 'seleziona report destinazione'
        filepath = str(QtGui.QFileDialog.getSaveFileName(self, caption))
        self.input_report.setText(filepath)

    def collect_inputs(self):
        kwargs = dict()
        kwargs['dburi'] = str(self.input_dburi.text().strip())
        kwargs['stations_path'] = str(self.input_file.text().strip())
        report_path = str(self.input_report.text()).strip()
        kwargs['report_path'] = report_path
        return kwargs

    def generate_cmd(self, bin_path, kwargs):
        cmd = join(bin_path, self.bin_name)
        args = []
        if kwargs['stations_path']:
            args += [kwargs['stations_path']]
        if kwargs['dburi']:
            args += ['-d', kwargs['dburi']]
        if kwargs['report_path']:
            args += ['-r', kwargs['report_path']]
        return cmd, args


class FindNewStations(QtGui.QMainWindow, Ui_find_new_stations_form):
    bin_name = 'find_new_stations'
    close_signal = QtCore.pyqtSignal()
    run_signal = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(FindNewStations, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.base_setup()

    def terminal_redirect(self):
        cursor = self.output_console.textCursor()
        cursor.movePosition(cursor.End)
        cursor.insertText(str(self.process.readAll(), 'utf-8'))
        self.output_console.ensureCursorVisible()
        scrollbar = self.output_console.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def closeEvent(self, event):
        self.close_signal.emit()

    def run_script(self):
        self.run_signal.emit()

    def base_setup(self):
        # terminal stuff
        self.output_console.setStyleSheet("background-color: rgb(0, 0, 0)")
        self.process = QtCore.QProcess(self)
        self.process.readyRead.connect(self.terminal_redirect)
        self.process.started.connect(lambda: self.script_run.setEnabled(False))
        self.process.finished.connect(lambda: self.script_run.setEnabled(True))
        self.process.setProcessChannelMode(QtCore.QProcess.MergedChannels)

        # connect run/close buttons
        self.script_close.clicked.connect(self.close)
        self.script_run.clicked.connect(self.run_script)

    def setupUi(self, Ui_find_new_stations_form):
        super(FindNewStations, self).setupUi(Ui_find_new_stations_form)
        # buttons for selecting inputs
        self.select_input_folder_button.clicked.connect(self.on_select_input_folder_clicked)
        self.select_output_file_button.clicked.connect(self.on_select_output_file_clicked)
        self.select_report_button.clicked.connect(self.on_select_report_clicked)
        # other ui setup
        self.input_dburi.setText(DEFAULT_DB_URI)

    def on_select_input_folder_clicked(self):
        caption = 'seleziona cartella di input'
        folderpath = str(QtGui.QFileDialog.getExistingDirectory(self, caption))
        self.input_folder.setText(folderpath)

    def on_select_output_file_clicked(self):
        caption = 'seleziona file di output'
        filepath = str(QtGui.QFileDialog.getSaveFileName(self, caption))
        self.output_file.setText(filepath)

    def on_select_report_clicked(self):
        caption = 'seleziona report destinazione'
        filepath = str(QtGui.QFileDialog.getSaveFileName(self, caption))
        self.input_report.setText(filepath)

    def collect_inputs(self):
        kwargs = dict()
        kwargs['dburi'] = str(self.input_dburi.text().strip())
        kwargs['data_folder'] = str(self.input_folder.text().strip())
        kwargs['stations_path'] = str(self.output_file.text().strip())
        report_path = str(self.input_report.text()).strip()
        kwargs['report_path'] = report_path
        return kwargs

    def generate_cmd(self, bin_path, kwargs):
        args = []
        if kwargs['data_folder']:
            args += [kwargs['data_folder']]
        cmd = join(bin_path, self.bin_name)
        if kwargs['dburi']:
            args += ['-d', kwargs['dburi']]
        if kwargs['stations_path']:
            args += ['-s', kwargs['stations_path']]
        if kwargs['report_path']:
            args += ['-r', kwargs['report_path']]
        return cmd, args


class MakeReports(QtGui.QMainWindow, Ui_make_reports_form):
    bin_name1 = 'make_report'
    bin_name2 = 'make_reports'
    close_signal = QtCore.pyqtSignal()
    run_signal = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(MakeReports, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.base_setup()

    def terminal_redirect(self):
        cursor = self.output_console.textCursor()
        cursor.movePosition(cursor.End)
        cursor.insertText(str(self.process.readAll(), 'utf-8'))
        self.output_console.ensureCursorVisible()
        scrollbar = self.output_console.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def closeEvent(self, event):
        self.close_signal.emit()

    def run_script(self):
        self.run_signal.emit()

    def base_setup(self):
        # terminal stuff
        self.output_console.setStyleSheet("background-color: rgb(0, 0, 0)")
        self.process = QtCore.QProcess(self)
        self.process.readyRead.connect(self.terminal_redirect)
        self.process.started.connect(lambda: self.script_run.setEnabled(False))
        self.process.finished.connect(lambda: self.script_run.setEnabled(True))
        self.process.setProcessChannelMode(QtCore.QProcess.MergedChannels)

        # connect run/close buttons
        self.script_close.clicked.connect(self.close)
        self.script_run.clicked.connect(self.run_script)

    def setupUi(self, Ui_make_reports_form):
        super(MakeReports, self).setupUi(Ui_make_reports_form)
        # buttons for selecting inputs
        self.select_input_file_button.clicked.connect(self.on_select_input_file_clicked)
        self.select_output_file_button.clicked.connect(self.on_select_output_file_clicked)
        self.select_input_folder_button.clicked.connect(self.on_select_input_folder_clicked)
        self.select_output_folder_button.clicked.connect(self.on_select_output_folder_clicked)
        self.select_report_button.clicked.connect(self.on_select_report_clicked)
        # other ui setup
        self.input_folder.setDisabled(True)
        self.output_folder.setDisabled(True)
        self.select_input_folder_button.setDisabled(True)
        self.select_output_folder_button.setDisabled(True)
        self.select_file.clicked.connect(self.on_radio_file_clicked)
        self.select_folder.clicked.connect(self.on_radio_folder_clicked)

    def on_radio_file_clicked(self):
        self.select_folder.setChecked(not self.select_file.isChecked())
        self.input_folder.setDisabled(True)
        self.output_folder.setDisabled(True)
        self.select_input_folder_button.setDisabled(True)
        self.select_output_folder_button.setDisabled(True)
        self.input_file.setDisabled(False)
        self.select_input_file_button.setDisabled(False)
        self.output_file.setDisabled(False)
        self.select_output_file_button.setDisabled(False)

    def on_radio_folder_clicked(self):
        self.select_file.setChecked(not self.select_folder.isChecked())
        self.input_folder.setDisabled(False)
        self.output_folder.setDisabled(False)
        self.select_input_folder_button.setDisabled(False)
        self.select_output_folder_button.setDisabled(False)
        self.input_file.setDisabled(True)
        self.select_input_file_button.setDisabled(True)
        self.output_file.setDisabled(True)
        self.select_output_file_button.setDisabled(True)

    def on_select_input_file_clicked(self):
        caption = 'seleziona file di input'
        filepath = str(QtGui.QFileDialog.getOpenFileName(self, caption))
        self.input_file.setText(filepath)

    def on_select_output_file_clicked(self):
        caption = 'seleziona file di output'
        filepath = str(QtGui.QFileDialog.getSaveFileName(self, caption))
        self.output_file.setText(filepath)

    def on_select_input_folder_clicked(self):
        caption = 'seleziona cartella di input'
        folderpath = str(QtGui.QFileDialog.getExistingDirectory(self, caption))
        self.input_folder.setText(folderpath)

    def on_select_output_folder_clicked(self):
        caption = 'seleziona cartella di output'
        folderpath = str(QtGui.QFileDialog.getExistingDirectory(self, caption))
        self.output_folder.setText(folderpath)

    def on_select_report_clicked(self):
        caption = 'seleziona report destinazione'
        filepath = str(QtGui.QFileDialog.getSaveFileName(self, caption))
        self.input_report.setText(filepath)

    def collect_inputs(self):
        kwargs = dict()
        kwargs['in_folder'] = str(self.input_folder.text().strip())
        kwargs['in_filepath'] = str(self.input_file.text().strip())
        kwargs['outdata_folder'] = str(self.output_folder.text().strip())
        kwargs['outdata_filepath'] = str(self.output_file.text().strip())
        report_path = str(self.input_report.text()).strip()
        kwargs['report_path'] = report_path
        return kwargs

    def generate_cmd(self, bin_path, kwargs):
        args = []
        if self.select_file.isChecked():
            bin_name = self.bin_name1
            cmd = join(bin_path, bin_name)
            if kwargs['in_filepath']:
                args += [kwargs['in_filepath']]
            if kwargs['outdata_filepath']:
                args += ['-d', kwargs['outdata_filepath']]
        else:
            bin_name = self.bin_name2
            cmd = join(bin_path, bin_name)
            if kwargs['in_folder']:
                args += [kwargs['in_folder']]
            if kwargs['outdata_folder']:
                args += ['-d', kwargs['outdata_folder']]
        if kwargs['report_path']:
            args += ['-r', kwargs['report_path']]
        return cmd, args


class MakeReport(QtGui.QMainWindow, Ui_make_report_form):
    bin_name1 = 'make_report'
    bin_name2 = 'make_reports'
    close_signal = QtCore.pyqtSignal()
    run_signal = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(MakeReport, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.base_setup()

    def terminal_redirect(self):
        cursor = self.output_console.textCursor()
        cursor.movePosition(cursor.End)
        cursor.insertText(str(self.process.readAll(), 'utf-8'))
        self.output_console.ensureCursorVisible()
        scrollbar = self.output_console.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def closeEvent(self, event):
        self.close_signal.emit()

    def run_script(self):
        self.run_signal.emit()

    def base_setup(self):
        # terminal stuff
        self.output_console.setStyleSheet("background-color: rgb(0, 0, 0)")
        self.process = QtCore.QProcess(self)
        self.process.readyRead.connect(self.terminal_redirect)
        self.process.started.connect(lambda: self.script_run.setEnabled(False))
        self.process.finished.connect(lambda: self.script_run.setEnabled(True))
        self.process.setProcessChannelMode(QtCore.QProcess.MergedChannels)

        # connect run/close buttons
        self.script_close.clicked.connect(self.close)
        self.script_run.clicked.connect(self.run_script)

    def setupUi(self, Ui_make_report_form):
        super(MakeReport, self).setupUi(Ui_make_report_form)
        # buttons for selecting inputs
        self.select_input_file_button.clicked.connect(self.on_select_input_file_clicked)
        self.select_input_folder_button.clicked.connect(self.on_select_input_folder_clicked)
        self.select_report_button.clicked.connect(self.on_select_report_clicked)
        # other ui setup
        self.input_folder.setDisabled(True)
        self.select_input_folder_button.setDisabled(True)
        self.select_file.clicked.connect(self.on_radio_file_clicked)
        self.select_folder.clicked.connect(self.on_radio_folder_clicked)

    def on_radio_file_clicked(self):
        self.select_folder.setChecked(not self.select_file.isChecked())
        self.input_folder.setDisabled(True)
        self.select_input_folder_button.setDisabled(True)
        self.input_file.setDisabled(False)
        self.select_input_file_button.setDisabled(False)

    def on_radio_folder_clicked(self):
        self.select_file.setChecked(not self.select_folder.isChecked())
        self.input_file.setDisabled(True)
        self.select_input_file_button.setDisabled(True)
        self.input_folder.setDisabled(False)
        self.select_input_folder_button.setDisabled(False)

    def on_select_input_file_clicked(self):
        caption = 'seleziona file di input'
        filepath = str(QtGui.QFileDialog.getOpenFileName(self, caption))
        self.input_file.setText(filepath)

    def on_select_input_folder_clicked(self):
        caption = 'seleziona cartella di input'
        folderpath = str(QtGui.QFileDialog.getExistingDirectory(self, caption))
        self.input_folder.setText(folderpath)

    def on_select_report_clicked(self):
        caption = 'seleziona report destinazione'
        filepath = str(QtGui.QFileDialog.getSaveFileName(self, caption))
        self.input_report.setText(filepath)

    def collect_inputs(self):
        kwargs = dict()
        kwargs['in_folder'] = str(self.input_folder.text().strip())
        kwargs['in_filepath'] = str(self.input_file.text().strip())
        report_path = str(self.input_report.text()).strip()
        kwargs['report_path'] = report_path
        return kwargs

    def generate_cmd(self, bin_path, kwargs):
        args = []
        if self.select_file.isChecked():
            bin_name = self.bin_name1
            cmd = join(bin_path, bin_name)
            if kwargs['in_filepath']:
                args += [kwargs['in_filepath']]
        else:
            bin_name = self.bin_name2
            cmd = join(bin_path, bin_name)
            if kwargs['in_folder']:
                args += [kwargs['in_folder']]
        if kwargs['report_path']:
            args += ['-r', kwargs['report_path']]
        return cmd, args


class DownloadEr(QtGui.QMainWindow, Ui_download_er_form):
    bin_name = 'download_er'
    close_signal = QtCore.pyqtSignal()
    run_signal = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(DownloadEr, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.base_setup()

    def terminal_redirect(self):
        cursor = self.output_console.textCursor()
        cursor.movePosition(cursor.End)
        cursor.insertText(str(self.process.readAll(), 'utf-8'))
        self.output_console.ensureCursorVisible()
        scrollbar = self.output_console.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def closeEvent(self, event):
        self.close_signal.emit()

    def run_script(self):
        self.run_signal.emit()

    def base_setup(self):
        # terminal stuff
        self.output_console.setStyleSheet("background-color: rgb(0, 0, 0)")
        self.process = QtCore.QProcess(self)
        self.process.readyRead.connect(self.terminal_redirect)
        self.process.started.connect(lambda: self.script_run.setEnabled(False))
        self.process.finished.connect(lambda: self.script_run.setEnabled(True))
        self.process.setProcessChannelMode(QtCore.QProcess.MergedChannels)

        # connect run/close buttons
        self.script_close.clicked.connect(self.close)
        self.script_run.clicked.connect(self.run_script)

    def setupUi(self, Ui_download_er_form):
        super(DownloadEr, self).setupUi(Ui_download_er_form)
        # buttons for selecting inputs
        self.select_destination_button.clicked.connect(self.on_select_dest_clicked)
        self.select_report_button.clicked.connect(self.on_select_report_clicked)
        # other ui setup
        today = date.today()
        yearseve_tokens = [today.year, 1, 1]
        today_tokens = [today.year, today.month, today.day]
        self.input_start.setDate(QtCore.QDate(*yearseve_tokens))
        self.input_end.setDate(QtCore.QDate(*today_tokens))

    def on_select_dest_clicked(self):
        caption = 'seleziona cartella destinazione'
        folderpath = str(QtGui.QFileDialog.getExistingDirectory(self, caption))
        self.input_destination.setText(folderpath)

    def on_select_report_clicked(self):
        caption = 'seleziona report destinazione'
        filepath = str(QtGui.QFileDialog.getSaveFileName(self, caption))
        self.input_report.setText(filepath)

    def collect_inputs(self):
        kwargs = dict()
        kwargs['out_csv_folder'] = self.input_destination.text().strip()
        report_path = self.input_report.text().strip()
        report_path = str(self.input_report.text()).strip()
        kwargs['report_path'] = report_path
        kwargs['start'] = date(*self.input_start.date().getDate()).strftime('%Y-%m-%d')
        kwargs['end'] = date(*self.input_end.date().getDate()).strftime('%Y-%m-%d')
        return kwargs

    def generate_cmd(self, bin_path, kwargs):
        cmd = join(bin_path, self.bin_name)
        args = []
        if kwargs['report_path']:
            args += ["-r", kwargs['report_path']]
        if kwargs['out_csv_folder']:
            args += [kwargs['out_csv_folder']]
        args += ['-s', kwargs['start']]
        args += ["-e", kwargs['end']]
        return cmd, args


class DownloadHiscentral(QtGui.QMainWindow, Ui_download_hiscentral_form):
    bin_name = 'download_hiscentral'
    close_signal = QtCore.pyqtSignal()
    run_signal = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(DownloadHiscentral, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.base_setup()

    def terminal_redirect(self):
        cursor = self.output_console.textCursor()
        cursor.movePosition(cursor.End)
        cursor.insertText(str(self.process.readAll(), 'utf-8'))
        self.output_console.ensureCursorVisible()
        scrollbar = self.output_console.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def closeEvent(self, event):
        self.close_signal.emit()

    def run_script(self):
        self.run_signal.emit()

    def base_setup(self):
        # terminal stuff
        self.output_console.setStyleSheet("background-color: rgb(0, 0, 0)")
        self.process = QtCore.QProcess(self)
        self.process.readyRead.connect(self.terminal_redirect)
        self.process.started.connect(lambda: self.script_run.setEnabled(False))
        self.process.finished.connect(lambda: self.script_run.setEnabled(True))
        self.process.setProcessChannelMode(QtCore.QProcess.MergedChannels)

        # connect run/close buttons
        self.script_close.clicked.connect(self.close)
        self.script_run.clicked.connect(self.run_script)

    def setupUi(self, Ui_Download_hiscentral_form):
        super(DownloadHiscentral, self).setupUi(Ui_Download_hiscentral_form)
        # buttons for selecting inputs
        self.select_destination_button.clicked.connect(self.on_select_dest_clicked)
        self.select_report_button.clicked.connect(self.on_select_report_clicked)
        # other ui setup
        for region_id, region in REGION_IDS_MAP.items():
            self.input_region.addItem(region, region_id)

    def on_select_dest_clicked(self):
        caption = 'seleziona cartella destinazione'
        folderpath = str(QtGui.QFileDialog.getExistingDirectory(self, caption))
        self.input_destination.setText(folderpath)

    def on_select_report_clicked(self):
        caption = 'seleziona report destinazione'
        filepath = str(QtGui.QFileDialog.getSaveFileName(self, caption))
        self.input_report.setText(filepath)

    def collect_inputs(self):
        kwargs = dict()
        kwargs['out_csv_folder'] = str(self.input_destination.text()).strip()
        kwargs['region_id'] = '%02d' % (self.input_region.currentIndex() + 1)
        report_path = str(self.input_report.text()).strip()
        kwargs['report_path'] = report_path
        locations = [
            r.strip() for r in self.input_locations.toPlainText().split('\n') if r.strip()]
        kwargs['locations'] = locations
        kwargs['variables'] = [v.text() for v in self.input_variables.selectedItems()]
        return kwargs

    def generate_cmd(self, bin_path, kwargs):
        cmd = join(bin_path, self.bin_name)
        args = []
        if kwargs['region_id']:
            args += ['-R', kwargs['region_id']]
        for var in kwargs['variables']:
            args += ['-v', var]
        for loc in kwargs['locations']:
            args += ["-l", loc]
        if kwargs['report_path']:
            args += ["-r", kwargs['report_path']]
        if kwargs['out_csv_folder']:
            args += [kwargs['out_csv_folder']]
        return cmd, args


class SciaFeedMainWindow(QtGui.QMainWindow):
    open_hiscentral_signal = QtCore.pyqtSignal()
    open_er_signal = QtCore.pyqtSignal()
    open_makereport_signal = QtCore.pyqtSignal()
    open_makereports_signal = QtCore.pyqtSignal()
    open_find_new_stations_signal = QtCore.pyqtSignal()
    open_upsert_stations_signal = QtCore.pyqtSignal()
    open_compute_daily_indicators_signal = QtCore.pyqtSignal()
    open_insert_daily_indicators_signal = QtCore.pyqtSignal()
    open_check_chain_signal = QtCore.pyqtSignal()
    open_compute_daily_indicators2_signal = QtCore.pyqtSignal()
    open_load_unique_data_signal = QtCore.pyqtSignal()

    def __init__(self):
        super(SciaFeedMainWindow, self).__init__()
        uic.loadUi(join(DESIGNER_PATH, 'sciafeed_main.ui.xml'), self)
        # connect all the open buttons to the corresponding functions
        for button, signal_name in [
            (self.OpenWindow_download_hiscentral, 'open_hiscentral_signal'),
            (self.OpenWindow_download_er, 'open_er_signal'),
            (self.OpenWindow_make_report, 'open_makereport_signal'),
            (self.OpenWindow_make_reports, 'open_makereports_signal'),
            (self.OpenWindow_find_new_stations, 'open_find_new_stations_signal'),
            (self.OpenWindow_upsert_stations, 'open_upsert_stations_signal'),
            (self.OpenWindow_compute_daily_indicators, 'open_compute_daily_indicators_signal'),
            (self.OpenWindow_insert_indicators, 'open_insert_daily_indicators_signal'),
            (self.OpenWindow_check_chain, 'open_check_chain_signal'),
            (self.OpenWindow_compute_daily_indicators2, 'open_compute_daily_indicators2_signal'),
            (self.OpenWindow_load_unique_data, 'open_load_unique_data_signal'),

        ]:
            signal_obj = getattr(self, signal_name)
            open_function = partial(self.show_window, button, signal_obj)
            button.clicked.connect(open_function)

    def show_window(self, button, signal_obj):
        button.setEnabled(False)
        signal_obj.emit()


class Controller:
    def __init__(self, bin_path):
        self.bin_path = bin_path

    def show_main(self):
        self.main_window = SciaFeedMainWindow()
        # connect each signal of main window to the show function of the controller
        for klass, controller_attribute, signal_name, main_window_button in [
            (DownloadHiscentral, 'download_hiscentral_window', 'open_hiscentral_signal',
             self.main_window.OpenWindow_download_hiscentral),
            (DownloadEr, 'download_er_window', 'open_er_signal',
             self.main_window.OpenWindow_download_er),
            (MakeReport, 'makereport_window', 'open_makereport_signal',
             self.main_window.OpenWindow_make_report),
            (MakeReports, 'makereports_window', 'open_makereports_signal',
             self.main_window.OpenWindow_make_reports),
            (FindNewStations, 'findnewstations_window', 'open_find_new_stations_signal',
             self.main_window.OpenWindow_find_new_stations),
            (UpsertStations, 'upsertstations_window', 'open_upsert_stations_signal',
             self.main_window.OpenWindow_upsert_stations),
            (ComputeDailyIndicators, 'computeind_window', 'open_compute_daily_indicators_signal',
             self.main_window.OpenWindow_compute_daily_indicators),
            (InsertDailyIndicators, 'insertind_window', 'open_insert_daily_indicators_signal',
             self.main_window.OpenWindow_insert_indicators),
            (CheckChain, 'checkchain_window', 'open_check_chain_signal',
             self.main_window.OpenWindow_check_chain),
            (ComputeDailyIndicators2, 'compind2_window', 'open_compute_daily_indicators2_signal',
             self.main_window.OpenWindow_compute_daily_indicators2),
            (LoadUniqueData, 'loaduniquedata_window', 'open_load_unique_data_signal',
             self.main_window.OpenWindow_load_unique_data),
        ]:
            signal_obj = getattr(self.main_window, signal_name)
            show_funct = partial(self.show_window, klass, controller_attribute, main_window_button)
            signal_obj.connect(show_funct)

        self.main_window.show()

    def run_script(self, window):
        kwargs = window.collect_inputs()
        cmd, args = window.generate_cmd(self.bin_path, kwargs)
        print('run cmd: %r with args: %r' % (cmd, args))
        window.process.start(cmd, args)

    def close_window(self, window, main_window_button):
        window.process.kill()
        window.close()
        main_window_button.setEnabled(True)

    def show_window(self, theklass, controller_attribute, main_window_button):
        setattr(self, controller_attribute, theklass(self.main_window))
        window = getattr(self, controller_attribute)
        close_window_funct = partial(self.close_window, window, main_window_button)
        window.close_signal.connect(close_window_funct)
        run_window_funct = partial(self.run_script, window)
        window.run_signal.connect(run_window_funct)
        window.show()


def run_sciafeed_gui():
    app = QtGui.QApplication(sys.argv)
    bin_path = dirname(abspath(sys.argv[0]))
    controller = Controller(bin_path)
    controller.show_main()
    sys.exit(app.exec_())
