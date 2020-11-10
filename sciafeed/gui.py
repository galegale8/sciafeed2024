
from os.path import join
import sys

from PyQt4 import QtGui, QtCore, uic

from sciafeed import DESIGNER_PATH
from sciafeed.designer.download_hiscentral_window import _fromUtf8, \
    Ui_Form as Ui_Download_hiscentral_form
from sciafeed.designer.download_er_window import Ui_Form as Ui_Download_er_form


class DownloadEr(QtGui.QWidget, Ui_Download_er_form):
    close_download_er_signal = QtCore.pyqtSignal()
    run_download_er_signal = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(DownloadEr, self).__init__(*args, **kwargs)
        self.setupUi(self)

    def setupUi(self, Ui_Download_er_form):
        super(DownloadEr, self).setupUi(Ui_Download_er_form)
        # connect all the buttons of the widget to the corresponding functions
        self.download_er_close.clicked.connect(self.close)
        self.download_er_run.clicked.connect(self.run_download_er)

    def closeEvent(self, event):
        self.close_download_er_signal.emit()

    def run_download_er(self):
        # emit the run signal
        self.run_download_er_signal.emit()


class DownloadHiscentral(QtGui.QMainWindow, Ui_Download_hiscentral_form):
    close_download_hiscentral_signal = QtCore.pyqtSignal()
    run_download_hiscentral_signal = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(DownloadHiscentral, self).__init__(*args, **kwargs)
        self.setupUi(self)

    def setupUi(self, Ui_Download_hiscentral_form):
        super(DownloadHiscentral, self).setupUi(Ui_Download_hiscentral_form)
        # connect all the buttons of the widget to the corresponding functions
        self.download_hiscentral_close.clicked.connect(self.close)
        self.download_hiscentral_run.clicked.connect(self.run_download_hiscentral)

    def closeEvent(self, event):
        self.close_download_hiscentral_signal.emit()

    def run_download_hiscentral(self):
        # emit the run signal
        self.run_download_hiscentral_signal.emit()


class SciaFeedGui(QtGui.QMainWindow):
    open_hiscentral_signal = QtCore.pyqtSignal()
    open_er_signal = QtCore.pyqtSignal()

    def __init__(self):
        super(SciaFeedGui, self).__init__()
        uic.loadUi(join(DESIGNER_PATH, 'sciafeed_main.ui.xml'), self)
        # connect all the buttons with the corresponding functions for opening the windows
        self.OpenWindow_download_hiscentral.clicked.connect(self.open_download_hiscentral)
        self.OpenWindow_download_er.clicked.connect(self.open_download_er)

    def open_download_hiscentral(self):
        self.open_hiscentral_signal.emit()

    def open_download_er(self):
        self.open_er_signal.emit()


class Controller:
    def show_main(self):
        self.main_window = SciaFeedGui()
        # collego tutte le emissioni di segnali della main window
        # alle funzioni corrispondenti show_... del controller
        # self.main_window.open_er_signal.connect(self.show_download_er)
        self.main_window.open_hiscentral_signal.connect(self.show_download_hiscentral)
        self.main_window.open_er_signal.connect(self.show_download_er)
        self.main_window.show()

    def show_download_hiscentral(self):
        self.download_hiscentral_window = DownloadHiscentral()
        # connect all run and close signals of the window to the controller functions
        self.download_hiscentral_window.close_download_hiscentral_signal.connect(
            self.close_download_hiscentral)
        self.download_hiscentral_window.run_download_hiscentral_signal.connect(
            self.run_download_hiscentral)
        self.download_hiscentral_window.show()

    def show_download_er(self):
        self.download_er_window = DownloadEr()
        # connect all run and close signals of the window to the controller functions
        self.download_er_window.close_download_er_signal.connect(self.close_download_er)
        self.download_er_window.run_download_er_signal.connect(self.run_download_er)

        self.download_er_window.show()

    def run_download_hiscentral(self):
        # execute the script, interacting with the widgets of the window
        print('run hiscentral!')

    def close_download_hiscentral(self):
        print('close hiscentral!')
        self.download_hiscentral_window.close()

    def run_download_er(self):
        # execute the script, interacting with the widgets of the window
        print('run er!')

    def close_download_er(self):
        print('close er!')
        self.download_er_window.close()


def run_sciafeed_gui():
    app = QtGui.QApplication(sys.argv)
    controller = Controller()
    controller.show_main()
    sys.exit(app.exec_())
