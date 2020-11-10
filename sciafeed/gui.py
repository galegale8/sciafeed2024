
from functools import partial
from os.path import abspath, dirname, join
import sys

from PyQt4 import QtGui, QtCore, uic

from sciafeed import DESIGNER_PATH
from sciafeed import hiscentral
from sciafeed.designer.download_hiscentral_window import Ui_download_hiscentral_form
from sciafeed.designer.download_er_window import Ui_download_er_form


def debug_trace():
  from PyQt4.QtCore import pyqtRemoveInputHook
  from pdb import set_trace
  pyqtRemoveInputHook()
  set_trace()


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
        for region_id, region in hiscentral.REGION_IDS_MAP.items():
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
        kwargs['out_csv_folder'] = self.input_destination.text().strip()
        kwargs['region_id'] = '%02d' % (self.input_region.currentIndex() + 1)
        report_path = self.input_report.text().strip()
        if report_path:
            kwargs['report_path'] = report_path
        locations = [
            r.strip() for r in self.input_locations.toPlainText().split('\n') if r.strip()]
        if locations:
            kwargs['locations'] = locations
        kwargs['variables'] = [v.text() for v in self.input_variables.selectedItems()]
        return kwargs

    def generate_cmd(self, bin_path, kwargs):
        #script = join(bin_path, self.bin_name)
        # TODO
        pass


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
        for region_id, region in hiscentral.REGION_IDS_MAP.items():
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
        script = join(bin_path, self.bin_name)
        arguments = []
        if kwargs['region_id']:
            arguments.extend(['-R', kwargs['region_id']])
        for var in kwargs['variables']:
            arguments.extend(['-v', var])
        for loc in kwargs['locations']:
            arguments.extend(['-l', loc])
        if kwargs['report_path']:
            arguments.extend(['-r', kwargs['report_path']])
        if [kwargs['out_csv_folder']]:
            arguments.extend([kwargs['out_csv_folder']])
        cmd = "%s %s" % (script, ' '.join(arguments))
        return cmd


class SciaFeedMainWindow(QtGui.QMainWindow):
    open_hiscentral_signal = QtCore.pyqtSignal()
    open_er_signal = QtCore.pyqtSignal()

    def __init__(self):
        super(SciaFeedMainWindow, self).__init__()
        uic.loadUi(join(DESIGNER_PATH, 'sciafeed_main.ui.xml'), self)
        # connect all the open buttons to the corresponding functions
        for button, signal_name in [
            (self.OpenWindow_download_hiscentral, 'open_hiscentral_signal'),
            (self.OpenWindow_download_er, 'open_er_signal'),
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
        ]:
            signal_obj = getattr(self.main_window, signal_name)
            show_funct = partial(self.show_window, klass, controller_attribute, main_window_button)
            signal_obj.connect(show_funct)

        self.main_window.show()

    def run_script(self, window):
        kwargs = window.collect_inputs()
        cmd = window.generate_cmd(self.bin_path, kwargs)
        print('run cmd: %r' % cmd)
        window.process.start(cmd)

    def close_window(self, window, main_window_button):
        window.process.kill()
        window.close()
        main_window_button.setEnabled(True)

    def show_window(self, theklass, controller_attribute, main_window_button):
        setattr(self, controller_attribute, theklass(self.main_window))
        window = getattr(self, controller_attribute)
        close_download_hiscentral = partial(self.close_window, window, main_window_button)
        window.close_signal.connect(close_download_hiscentral)
        run_download_hiscentral = partial(self.run_script, window)
        window.run_signal.connect(run_download_hiscentral)
        window.show()


def run_sciafeed_gui():
    app = QtGui.QApplication(sys.argv)
    bin_path = dirname(abspath(sys.argv[0]))
    controller = Controller(bin_path)
    controller.show_main()
    sys.exit(app.exec_())
