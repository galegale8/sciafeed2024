# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'process_dma.ui.xml'
#
# Created by: PyQt4 UI code generator 4.12.1
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

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

class Ui_process_dma_form(object):
    def setupUi(self, process_dma_form):
        process_dma_form.setObjectName(_fromUtf8("process_dma_form"))
        process_dma_form.resize(490, 640)
        sizePolicy = QtGui.QSizePolicy(QtGui.QSizePolicy.Fixed, QtGui.QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(process_dma_form.sizePolicy().hasHeightForWidth())
        process_dma_form.setSizePolicy(sizePolicy)
        process_dma_form.setMinimumSize(QtCore.QSize(490, 640))
        process_dma_form.setMaximumSize(QtCore.QSize(490, 640))
        self.layoutWidget = QtGui.QWidget(process_dma_form)
        self.layoutWidget.setGeometry(QtCore.QRect(288, 600, 178, 29))
        self.layoutWidget.setObjectName(_fromUtf8("layoutWidget"))
        self.horizontalLayout_4 = QtGui.QHBoxLayout(self.layoutWidget)
        self.horizontalLayout_4.setMargin(0)
        self.horizontalLayout_4.setObjectName(_fromUtf8("horizontalLayout_4"))
        self.script_close = QtGui.QPushButton(self.layoutWidget)
        self.script_close.setEnabled(True)
        self.script_close.setCheckable(True)
        self.script_close.setChecked(False)
        self.script_close.setAutoExclusive(False)
        self.script_close.setAutoDefault(False)
        self.script_close.setFlat(False)
        self.script_close.setObjectName(_fromUtf8("script_close"))
        self.horizontalLayout_4.addWidget(self.script_close)
        self.script_run = QtGui.QPushButton(self.layoutWidget)
        self.script_run.setCheckable(True)
        self.script_run.setObjectName(_fromUtf8("script_run"))
        self.horizontalLayout_4.addWidget(self.script_run)
        self.layoutWidget_2 = QtGui.QWidget(process_dma_form)
        self.layoutWidget_2.setGeometry(QtCore.QRect(30, 350, 431, 241))
        self.layoutWidget_2.setObjectName(_fromUtf8("layoutWidget_2"))
        self.verticalLayout_3 = QtGui.QVBoxLayout(self.layoutWidget_2)
        self.verticalLayout_3.setMargin(0)
        self.verticalLayout_3.setObjectName(_fromUtf8("verticalLayout_3"))
        self.label_7 = QtGui.QLabel(self.layoutWidget_2)
        self.label_7.setObjectName(_fromUtf8("label_7"))
        self.verticalLayout_3.addWidget(self.label_7)
        self.output_console = QtGui.QTextEdit(self.layoutWidget_2)
        font = QtGui.QFont()
        font.setPointSize(5)
        font.setBold(False)
        font.setWeight(50)
        self.output_console.setFont(font)
        self.output_console.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        self.output_console.setReadOnly(True)
        self.output_console.setObjectName(_fromUtf8("output_console"))
        self.verticalLayout_3.addWidget(self.output_console)
        self.download_er_description = QtGui.QLabel(process_dma_form)
        self.download_er_description.setGeometry(QtCore.QRect(30, 75, 441, 17))
        self.download_er_description.setObjectName(_fromUtf8("download_er_description"))
        self.title_frame = QtGui.QFrame(process_dma_form)
        self.title_frame.setGeometry(QtCore.QRect(28, 20, 431, 51))
        self.title_frame.setFrameShape(QtGui.QFrame.StyledPanel)
        self.title_frame.setFrameShadow(QtGui.QFrame.Raised)
        self.title_frame.setObjectName(_fromUtf8("title_frame"))
        self.Title = QtGui.QLabel(self.title_frame)
        self.Title.setGeometry(QtCore.QRect(20, 10, 341, 41))
        self.Title.setObjectName(_fromUtf8("Title"))
        self.widget = QtGui.QWidget(process_dma_form)
        self.widget.setGeometry(QtCore.QRect(30, 170, 201, 65))
        self.widget.setObjectName(_fromUtf8("widget"))
        self.verticalLayout_4 = QtGui.QVBoxLayout(self.widget)
        self.verticalLayout_4.setMargin(0)
        self.verticalLayout_4.setObjectName(_fromUtf8("verticalLayout_4"))
        self.horizontalLayout_7 = QtGui.QHBoxLayout()
        self.horizontalLayout_7.setObjectName(_fromUtf8("horizontalLayout_7"))
        self.label_4 = QtGui.QLabel(self.widget)
        self.label_4.setObjectName(_fromUtf8("label_4"))
        self.horizontalLayout_7.addWidget(self.label_4)
        spacerItem = QtGui.QSpacerItem(40, 20, QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Minimum)
        self.horizontalLayout_7.addItem(spacerItem)
        self.verticalLayout_4.addLayout(self.horizontalLayout_7)
        self.horizontalLayout_8 = QtGui.QHBoxLayout()
        self.horizontalLayout_8.setObjectName(_fromUtf8("horizontalLayout_8"))
        self.input_source_schema = QtGui.QLineEdit(self.widget)
        self.input_source_schema.setObjectName(_fromUtf8("input_source_schema"))
        self.horizontalLayout_8.addWidget(self.input_source_schema)
        self.verticalLayout_4.addLayout(self.horizontalLayout_8)
        self.layoutWidget_3 = QtGui.QWidget(process_dma_form)
        self.layoutWidget_3.setGeometry(QtCore.QRect(250, 170, 211, 65))
        self.layoutWidget_3.setObjectName(_fromUtf8("layoutWidget_3"))
        self.verticalLayout_5 = QtGui.QVBoxLayout(self.layoutWidget_3)
        self.verticalLayout_5.setMargin(0)
        self.verticalLayout_5.setObjectName(_fromUtf8("verticalLayout_5"))
        self.horizontalLayout_9 = QtGui.QHBoxLayout()
        self.horizontalLayout_9.setObjectName(_fromUtf8("horizontalLayout_9"))
        self.label_6 = QtGui.QLabel(self.layoutWidget_3)
        self.label_6.setObjectName(_fromUtf8("label_6"))
        self.horizontalLayout_9.addWidget(self.label_6)
        spacerItem1 = QtGui.QSpacerItem(40, 20, QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Minimum)
        self.horizontalLayout_9.addItem(spacerItem1)
        self.verticalLayout_5.addLayout(self.horizontalLayout_9)
        self.horizontalLayout_10 = QtGui.QHBoxLayout()
        self.horizontalLayout_10.setObjectName(_fromUtf8("horizontalLayout_10"))
        self.input_target_schema = QtGui.QLineEdit(self.layoutWidget_3)
        self.input_target_schema.setObjectName(_fromUtf8("input_target_schema"))
        self.horizontalLayout_10.addWidget(self.input_target_schema)
        self.verticalLayout_5.addLayout(self.horizontalLayout_10)
        self.layoutWidget_4 = QtGui.QWidget(process_dma_form)
        self.layoutWidget_4.setGeometry(QtCore.QRect(200, 250, 257, 29))
        self.layoutWidget_4.setObjectName(_fromUtf8("layoutWidget_4"))
        self.horizontalLayout_11 = QtGui.QHBoxLayout(self.layoutWidget_4)
        self.horizontalLayout_11.setMargin(0)
        self.horizontalLayout_11.setObjectName(_fromUtf8("horizontalLayout_11"))
        self.label_2 = QtGui.QLabel(self.layoutWidget_4)
        self.label_2.setObjectName(_fromUtf8("label_2"))
        self.horizontalLayout_11.addWidget(self.label_2)
        self.input_policy = QtGui.QComboBox(self.layoutWidget_4)
        self.input_policy.setObjectName(_fromUtf8("input_policy"))
        self.input_policy.addItem(_fromUtf8(""))
        self.input_policy.addItem(_fromUtf8(""))
        self.horizontalLayout_11.addWidget(self.input_policy)
        self.widget1 = QtGui.QWidget(process_dma_form)
        self.widget1.setGeometry(QtCore.QRect(30, 280, 431, 59))
        self.widget1.setObjectName(_fromUtf8("widget1"))
        self.verticalLayout_2 = QtGui.QVBoxLayout(self.widget1)
        self.verticalLayout_2.setMargin(0)
        self.verticalLayout_2.setObjectName(_fromUtf8("verticalLayout_2"))
        self.horizontalLayout_6 = QtGui.QHBoxLayout()
        self.horizontalLayout_6.setObjectName(_fromUtf8("horizontalLayout_6"))
        self.label_5 = QtGui.QLabel(self.widget1)
        self.label_5.setObjectName(_fromUtf8("label_5"))
        self.horizontalLayout_6.addWidget(self.label_5)
        spacerItem2 = QtGui.QSpacerItem(40, 20, QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Minimum)
        self.horizontalLayout_6.addItem(spacerItem2)
        self.verticalLayout_2.addLayout(self.horizontalLayout_6)
        self.horizontalLayout_3 = QtGui.QHBoxLayout()
        self.horizontalLayout_3.setObjectName(_fromUtf8("horizontalLayout_3"))
        self.input_report = QtGui.QLineEdit(self.widget1)
        self.input_report.setObjectName(_fromUtf8("input_report"))
        self.horizontalLayout_3.addWidget(self.input_report)
        self.select_report_button = QtGui.QPushButton(self.widget1)
        self.select_report_button.setObjectName(_fromUtf8("select_report_button"))
        self.horizontalLayout_3.addWidget(self.select_report_button)
        self.verticalLayout_2.addLayout(self.horizontalLayout_3)
        self.widget2 = QtGui.QWidget(process_dma_form)
        self.widget2.setGeometry(QtCore.QRect(31, 101, 429, 67))
        self.widget2.setObjectName(_fromUtf8("widget2"))
        self.horizontalLayout_2 = QtGui.QHBoxLayout(self.widget2)
        self.horizontalLayout_2.setMargin(0)
        self.horizontalLayout_2.setObjectName(_fromUtf8("horizontalLayout_2"))
        self.gridLayout = QtGui.QGridLayout()
        self.gridLayout.setObjectName(_fromUtf8("gridLayout"))
        self.horizontalLayout = QtGui.QHBoxLayout()
        self.horizontalLayout.setObjectName(_fromUtf8("horizontalLayout"))
        self.label = QtGui.QLabel(self.widget2)
        self.label.setObjectName(_fromUtf8("label"))
        self.horizontalLayout.addWidget(self.label)
        spacerItem3 = QtGui.QSpacerItem(288, 20, QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Minimum)
        self.horizontalLayout.addItem(spacerItem3)
        self.gridLayout.addLayout(self.horizontalLayout, 0, 0, 1, 1)
        self.input_dburi = QtGui.QLineEdit(self.widget2)
        sizePolicy = QtGui.QSizePolicy(QtGui.QSizePolicy.Preferred, QtGui.QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.input_dburi.sizePolicy().hasHeightForWidth())
        self.input_dburi.setSizePolicy(sizePolicy)
        self.input_dburi.setMinimumSize(QtCore.QSize(220, 0))
        self.input_dburi.setObjectName(_fromUtf8("input_dburi"))
        self.gridLayout.addWidget(self.input_dburi, 1, 0, 1, 1)
        self.horizontalLayout_2.addLayout(self.gridLayout)
        self.layoutWidget.raise_()
        self.layoutWidget_2.raise_()
        self.download_er_description.raise_()
        self.title_frame.raise_()
        self.layoutWidget.raise_()
        self.input_policy.raise_()
        self.label_2.raise_()
        self.layoutWidget_3.raise_()
        self.layoutWidget_3.raise_()
        self.layoutWidget_4.raise_()

        self.retranslateUi(process_dma_form)
        QtCore.QMetaObject.connectSlotsByName(process_dma_form)
        process_dma_form.setTabOrder(self.input_report, self.select_report_button)
        process_dma_form.setTabOrder(self.select_report_button, self.output_console)
        process_dma_form.setTabOrder(self.output_console, self.script_close)
        process_dma_form.setTabOrder(self.script_close, self.script_run)

    def retranslateUi(self, process_dma_form):
        process_dma_form.setWindowTitle(_translate("process_dma_form", "Aggiornamento dati DMA", None))
        self.script_close.setText(_translate("process_dma_form", "Chiudi", None))
        self.script_run.setText(_translate("process_dma_form", "Avvia", None))
        self.label_7.setText(_translate("process_dma_form", "Output processamento", None))
        self.output_console.setHtml(_translate("process_dma_form", "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
"<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
"p, li { white-space: pre-wrap; }\n"
"</style></head><body style=\" font-family:\'Ubuntu\'; font-size:5pt; font-weight:400; font-style:normal;\">\n"
"<p style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:9pt; color:#ffffff;\">READY</span></p>\n"
"<p style=\"-qt-paragraph-type:empty; margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px; font-size:9pt; color:#ffffff;\"><br /></p></body></html>", None))
        self.download_er_description.setText(_translate("process_dma_form", "Aggiornamento dati decadali/mensili/annuali nel database", None))
        self.Title.setText(_translate("process_dma_form", "<html><head/><body><p><span style=\" font-family:\'Sans Serif\'; font-size:16pt;\">Aggiornamento dati DMA</span></p></body></html>", None))
        self.label_4.setText(_translate("process_dma_form", "Schema di input", None))
        self.label_6.setText(_translate("process_dma_form", "Schema di output", None))
        self.label_2.setText(_translate("process_dma_form", "Tipo di inserimento", None))
        self.input_policy.setItemText(0, _translate("process_dma_form", "UPSERT", None))
        self.input_policy.setItemText(1, _translate("process_dma_form", "SOLO INSERT", None))
        self.label_5.setText(_translate("process_dma_form", "File di report", None))
        self.select_report_button.setText(_translate("process_dma_form", "Cerca...", None))
        self.label.setText(_translate("process_dma_form", "Database URI", None))

