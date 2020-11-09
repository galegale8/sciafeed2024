
from os.path import join
import sys

from PyQt4 import QtGui, QtCore, uic

from sciafeed import this_path


try:
    _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
    def _fromUtf8(s):
        return s

try:
    _encoding = QtGui.QApplication.UnicodeUTF8
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig, _encoding)
except AttributeError:
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig)


class SciaFeedGui(QtGui.QMainWindow):
    def __init__(self):
        super(SciaFeedGui, self).__init__()
        self.download_hiscentral_window = QtGui.QMainWindow()
        uic.loadUi(join(this_path, 'designer/sciafeed_main.ui.xml'), self)
        self.download_hiscentral_window = uic.loadUi(
            join(this_path, 'designer/download_hiscentral.ui.xml'))
        self.set_connections()

    def set_connections(self):
        QtCore.QObject.connect(self.OpenWindow_download_hiscentral,
                               QtCore.SIGNAL(_fromUtf8("clicked(bool)")),
                               self.download_hiscentral_window.show)


def run_sciafeed_gui():
    app = QtGui.QApplication(sys.argv)
    main_window = SciaFeedGui()
    main_window.show()
    sys.exit(app.exec_())
