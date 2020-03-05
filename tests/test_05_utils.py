
from datetime import datetime

from os.path import join

import xlrd

from sciafeed import utils

from . import TEST_DATA_PATH


def test_cell_str():
    for cell_type, cell_value in [
            (xlrd.XL_CELL_TEXT, 'ciao'),
            (xlrd.XL_CELL_NUMBER, 8),
            (xlrd.XL_CELL_BLANK, ''),
    ]:
        cell = xlrd.sheet.Cell(cell_type, cell_value)
        assert utils.cell_str(cell, datemode=0) == str(cell_value)

    # corner cases
    cell = xlrd.sheet.Cell(xlrd.XL_CELL_BOOLEAN, True)
    assert utils.cell_str(cell, datemode=0) == '1'

    cell = xlrd.sheet.Cell(xlrd.XL_CELL_BOOLEAN, False)
    assert utils.cell_str(cell, datemode=0) == '0'

    cell = xlrd.sheet.Cell(xlrd.XL_CELL_EMPTY, None)
    assert utils.cell_str(cell, datemode=0) is None

    cell = xlrd.sheet.Cell(xlrd.XL_CELL_DATE, 43892)
    assert utils.cell_str(cell, datemode=0, datepattern="%Y-%m-%d") == '2020-03-02'


def test_load_excel():
    filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.xls')
    expected_rows = [
        [
            '',
            '',
            ' AUTONOME PROVINZ BOZEN          ',
            '          PROVINCIA AUTONOMA DI BOLZANO',
            ''],
        [
            '',
            '',
            '26. Brand- und Zivilschutz             ',
            '             26. Protezione antincendi e civile',
            ''],
        [
            '',
            '',
            '26.4 - Hydrographisches Amt              ',
            '             26.4 - ufficio idrografico',
            ''],
        ['', '', '', '', ''],
        ['', 'Station - stazione :', 'Marienberg - Monte Maria', '', ''],
        ['', 'Nummer - codice :', '02500MS', '', ''],
        ['', 'Rechtswert - X UTM :', '616288', '', ''],
        ['', 'Hochwert - Y UTM :', '5173583', '', ''],
        ['', 'Höhe - quota :', '1310', '', ''],
        ['', 'Zeitraum - periodo :', '01.01.1981', '31.03.2019', ''],
        ['', '', '', '', ''],
        [
            '',
            'Data\nDatum',
            'Precipitazione Niederschlag\n'
            '[mm]\n'
            'ora osservazione\n'
            'Beobachtungszeitpunkt\n'
            '09:00',
            'Temperatura\nTemperatur \n[°C]\n00:00  -  24:00',
            ''],
        ['', '', '', 'massima Maximum', 'minima Minimum'],
        ['', '01.01.1981', '0,0', '9,0', '3,0'],
        ['', '02.01.1981', '0,4', '5,0', '-4,0'],
        ['', '03.01.1981', '0,0', '5,0', '-4,0'],
        ['', '04.01.1981', '14,5', '9,0', '1,0'],
        ['', '05.01.1981', '5,1', '3,0', '-8,0'],
        ['', '06.01.1981', '1,0', '-5,0', '-8,0'],
        ['', '07.01.1981', '6,1', '-5,0', '-9,0'],
        ['', '08.01.1981', '0,0', '-7,0', '-13,0']
    ]
    rows = utils.load_excel(filepath, sheet_index=0)
    assert rows == expected_rows
