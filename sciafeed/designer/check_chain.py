# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'check_chain.ui.xml'
#
# Created by: PyQt5 UI code generator 5.15.6
#
# WARNING! All changes made in this file will be lost!

from PyQt5 import QtCore, QtGui, QtWidgets

try:
    _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
    def _fromUtf8(s):
        return s

try:
    _encoding = QtWidgets.QApplication.UnicodeUTF8
    def _translate(context, text, disambig):
        return QtWidgets.QApplication.translate(context, text, disambig, _encoding)
except AttributeError:
    def _translate(context, text, disambig):
        return QtWidgets.QApplication.translate(context, text, disambig)

class Ui_check_chain_form(object):
    def setupUi(self, check_chain_form):
        check_chain_form.setObjectName(_fromUtf8("check_chain_form"))
        check_chain_form.resize(490, 640)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(check_chain_form.sizePolicy().hasHeightForWidth())
        check_chain_form.setSizePolicy(sizePolicy)
        check_chain_form.setMinimumSize(QtCore.QSize(490, 640))
        check_chain_form.setMaximumSize(QtCore.QSize(490, 640))
        self.layoutWidget = QtWidgets.QWidget(check_chain_form)
        self.layoutWidget.setGeometry(QtCore.QRect(288, 600, 178, 29))
        self.layoutWidget.setObjectName(_fromUtf8("layoutWidget"))
        self.horizontalLayout_4 = QtWidgets.QHBoxLayout(self.layoutWidget)
        #self.horizontalLayout_4.setMargin(0)
        self.horizontalLayout_4.setObjectName(_fromUtf8("horizontalLayout_4"))
        self.script_close = QtWidgets.QPushButton(self.layoutWidget)
        self.script_close.setEnabled(True)
        self.script_close.setCheckable(True)
        self.script_close.setChecked(False)
        self.script_close.setAutoExclusive(False)
        self.script_close.setAutoDefault(False)
        self.script_close.setFlat(False)
        self.script_close.setObjectName(_fromUtf8("script_close"))
        self.horizontalLayout_4.addWidget(self.script_close)
        self.script_run = QtWidgets.QPushButton(self.layoutWidget)
        self.script_run.setCheckable(True)
        self.script_run.setObjectName(_fromUtf8("script_run"))
        self.horizontalLayout_4.addWidget(self.script_run)
        self.layoutWidget_2 = QtWidgets.QWidget(check_chain_form)
        self.layoutWidget_2.setGeometry(QtCore.QRect(30, 340, 431, 251))
        self.layoutWidget_2.setObjectName(_fromUtf8("layoutWidget_2"))
        self.verticalLayout_3 = QtWidgets.QVBoxLayout(self.layoutWidget_2)
        #self.verticalLayout_3.setMargin(0)
        self.verticalLayout_3.setObjectName(_fromUtf8("verticalLayout_3"))
        self.label_7 = QtWidgets.QLabel(self.layoutWidget_2)
        self.label_7.setObjectName(_fromUtf8("label_7"))
        self.verticalLayout_3.addWidget(self.label_7)
        self.output_console = QtWidgets.QTextEdit(self.layoutWidget_2)
        font = QtGui.QFont()
        font.setPointSize(5)
        font.setBold(False)
        font.setWeight(50)
        self.output_console.setFont(font)
        self.output_console.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        self.output_console.setReadOnly(True)
        self.output_console.setObjectName(_fromUtf8("output_console"))
        self.verticalLayout_3.addWidget(self.output_console)
        self.download_er_description = QtWidgets.QLabel(check_chain_form)
        self.download_er_description.setGeometry(QtCore.QRect(30, 75, 441, 17))
        self.download_er_description.setObjectName(_fromUtf8("download_er_description"))
        self.title_frame = QtWidgets.QFrame(check_chain_form)
        self.title_frame.setGeometry(QtCore.QRect(28, 20, 431, 51))
        self.title_frame.setFrameShape(QtWidgets.QFrame.StyledPanel)
        self.title_frame.setFrameShadow(QtWidgets.QFrame.Raised)
        self.title_frame.setObjectName(_fromUtf8("title_frame"))
        self.Title = QtWidgets.QLabel(self.title_frame)
        self.Title.setGeometry(QtCore.QRect(20, 10, 341, 41))
        self.Title.setObjectName(_fromUtf8("Title"))
        self.widget = QtWidgets.QWidget(check_chain_form)
        self.widget.setGeometry(QtCore.QRect(30, 100, 431, 221))
        self.widget.setObjectName(_fromUtf8("widget"))
        self.verticalLayout = QtWidgets.QVBoxLayout(self.widget)
        #self.verticalLayout.setMargin(0)
        self.verticalLayout.setObjectName(_fromUtf8("verticalLayout"))
        self.horizontalLayout_2 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_2.setObjectName(_fromUtf8("horizontalLayout_2"))
        self.gridLayout = QtWidgets.QGridLayout()
        self.gridLayout.setObjectName(_fromUtf8("gridLayout"))
        self.horizontalLayout = QtWidgets.QHBoxLayout()
        self.horizontalLayout.setObjectName(_fromUtf8("horizontalLayout"))
        self.label = QtWidgets.QLabel(self.widget)
        self.label.setObjectName(_fromUtf8("label"))
        self.horizontalLayout.addWidget(self.label)
        spacerItem = QtWidgets.QSpacerItem(288, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout.addItem(spacerItem)
        self.gridLayout.addLayout(self.horizontalLayout, 0, 0, 1, 1)
        self.input_dburi = QtWidgets.QLineEdit(self.widget)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Preferred, QtWidgets.QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.input_dburi.sizePolicy().hasHeightForWidth())
        self.input_dburi.setSizePolicy(sizePolicy)
        self.input_dburi.setMinimumSize(QtCore.QSize(220, 0))
        self.input_dburi.setObjectName(_fromUtf8("input_dburi"))
        self.gridLayout.addWidget(self.input_dburi, 1, 0, 1, 1)
        self.horizontalLayout_2.addLayout(self.gridLayout)
        self.verticalLayout_4 = QtWidgets.QVBoxLayout()
        self.verticalLayout_4.setObjectName(_fromUtf8("verticalLayout_4"))
        self.horizontalLayout_7 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_7.setObjectName(_fromUtf8("horizontalLayout_7"))
        self.label_4 = QtWidgets.QLabel(self.widget)
        self.label_4.setObjectName(_fromUtf8("label_4"))
        self.horizontalLayout_7.addWidget(self.label_4)
        spacerItem1 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_7.addItem(spacerItem1)
        self.verticalLayout_4.addLayout(self.horizontalLayout_7)
        self.horizontalLayout_8 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_8.setObjectName(_fromUtf8("horizontalLayout_8"))
        self.input_schema = QtWidgets.QLineEdit(self.widget)
        self.input_schema.setObjectName(_fromUtf8("input_schema"))
        self.horizontalLayout_8.addWidget(self.input_schema)
        self.verticalLayout_4.addLayout(self.horizontalLayout_8)
        self.horizontalLayout_2.addLayout(self.verticalLayout_4)
        self.verticalLayout.addLayout(self.horizontalLayout_2)
        self.verticalLayout_5 = QtWidgets.QVBoxLayout()
        self.verticalLayout_5.setObjectName(_fromUtf8("verticalLayout_5"))
        self.horizontalLayout_11 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_11.setObjectName(_fromUtf8("horizontalLayout_11"))
        self.label_6 = QtWidgets.QLabel(self.widget)
        self.label_6.setObjectName(_fromUtf8("label_6"))
        self.horizontalLayout_11.addWidget(self.label_6)
        spacerItem2 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_11.addItem(spacerItem2)
        self.verticalLayout_5.addLayout(self.horizontalLayout_11)
        self.horizontalLayout_12 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_12.setObjectName(_fromUtf8("horizontalLayout_12"))
        self.input_where = QtWidgets.QLineEdit(self.widget)
        self.input_where.setObjectName(_fromUtf8("input_where"))
        self.horizontalLayout_12.addWidget(self.input_where)
        self.verticalLayout_5.addLayout(self.horizontalLayout_12)
        self.verticalLayout.addLayout(self.verticalLayout_5)
        self.input_omitflag = QtWidgets.QCheckBox(self.widget)
        self.input_omitflag.setObjectName(_fromUtf8("input_omitflag"))
        self.verticalLayout.addWidget(self.input_omitflag)
        self.verticalLayout_2 = QtWidgets.QVBoxLayout()
        self.verticalLayout_2.setObjectName(_fromUtf8("verticalLayout_2"))
        self.horizontalLayout_6 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_6.setObjectName(_fromUtf8("horizontalLayout_6"))
        self.label_5 = QtWidgets.QLabel(self.widget)
        self.label_5.setObjectName(_fromUtf8("label_5"))
        self.horizontalLayout_6.addWidget(self.label_5)
        spacerItem3 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_6.addItem(spacerItem3)
        self.verticalLayout_2.addLayout(self.horizontalLayout_6)
        self.horizontalLayout_3 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_3.setObjectName(_fromUtf8("horizontalLayout_3"))
        self.input_report = QtWidgets.QLineEdit(self.widget)
        self.input_report.setObjectName(_fromUtf8("input_report"))
        self.horizontalLayout_3.addWidget(self.input_report)
        self.select_report_button = QtWidgets.QPushButton(self.widget)
        self.select_report_button.setObjectName(_fromUtf8("select_report_button"))
        self.horizontalLayout_3.addWidget(self.select_report_button)
        self.verticalLayout_2.addLayout(self.horizontalLayout_3)
        self.verticalLayout.addLayout(self.verticalLayout_2)

        self.retranslateUi(check_chain_form)
        QtCore.QMetaObject.connectSlotsByName(check_chain_form)
        check_chain_form.setTabOrder(self.input_report, self.select_report_button)
        check_chain_form.setTabOrder(self.select_report_button, self.output_console)
        check_chain_form.setTabOrder(self.output_console, self.script_close)
        check_chain_form.setTabOrder(self.script_close, self.script_run)

    def retranslateUi(self, check_chain_form):
        check_chain_form.setWindowTitle(_translate("check_chain_form", "Catena di controllo", None))
        self.script_close.setText(_translate("check_chain_form", "Chiudi", None))
        self.script_run.setText(_translate("check_chain_form", "Avvia", None))
        self.label_7.setText(_translate("check_chain_form", "Output processamento", None))
        self.output_console.setHtml(_translate("check_chain_form", "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
"<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
"p, li { white-space: pre-wrap; }\n"
"</style></head><body style=\" font-family:\'Ubuntu\'; font-size:5pt; font-weight:400; font-style:normal;\">\n"
"<p style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:9pt; color:#ffffff;\">READY</span></p>\n"
"<p style=\"-qt-paragraph-type:empty; margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px; font-size:9pt; color:#ffffff;\"><br /></p></body></html>", None))
        self.download_er_description.setText(_translate("check_chain_form", "Aggiornamento flag di controllo", None))
        self.Title.setText(_translate("check_chain_form", "<html><head/><body><p><span style=\" font-family:\'Sans Serif\'; font-size:16pt;\">Catena di controllo indicatori</span></p></body></html>", None))
        self.label.setText(_translate("check_chain_form", "Database URI", None))
        self.label_4.setText(_translate("check_chain_form", "Schema del database", None))
        self.label_6.setText(_translate("check_chain_form", "condizione WHERE sulle anagrafiche stazioni", None))
        self.input_omitflag.setText(_translate("check_chain_form", "Ometti la sincronizzazione preliminare dei flag +5/-9", None))
        self.label_5.setText(_translate("check_chain_form", "File di report", None))
        self.select_report_button.setText(_translate("check_chain_form", "Cerca...", None))

