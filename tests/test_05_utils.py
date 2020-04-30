
import csv
from io import TextIOWrapper
from os import mkdir
from os.path import exists, join

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


def test_folder2props():
    for folder_name, exp_result in [
        ('random_folder', {}),
        ('11_Sinottica', {'cod_rete': '11'}),
        ('14_Mareografica', {'cod_rete': '14'}),
        ('20_ARPAEmiliaRomagna', {'cod_rete': '20'}),
        ('21_ARPAFriuliVeneziaGiulia', {'cod_rete': '21'}),
        ('22_ARPALiguria', {'cod_rete': '22'}),
        ('23_ARPAValleAosta', {'cod_rete': '23'}),
        ('25_ARPALombardia', {'cod_rete': '25'}),
        ('27_ARPAVeneto', {'cod_rete': '27'}),
        ('28_ARPAPiemonte', {'cod_rete': '28'}),
        ('30_SIASSicilia', {'cod_rete': '30'}),
        ('31_ARPACalabria', {'cod_rete': '31'}),
        ('32_MeteoTrentino', {'cod_rete': '32'}),
        ('33_ASSAMMarche', {'cod_rete': '33'}),
        ('34_ARPACampania', {'cod_rete': '34'}),
        ('35_ProvinciaAutonomaBolzano', {'cod_rete': '35'}),
        ('36_ALSIABasilicata', {'cod_rete': '36'}),
        ('38_ARSIALLazio', {'cod_rete': '38'}),
        ('15_IDROGRAFICA_Lazio_12', {'cod_rete': '15', 'cod_utente_prefix': '12'}),
        ('15_IDROGRAFICA_Sicilia_19', {'cod_rete': '15', 'cod_utente_prefix': '19'}),
        ('15_IDROGRAFICA_Umbria_10', {'cod_rete': '15', 'cod_utente_prefix': '10'})
    ]:
        eff_result = utils.folder2props(folder_name)
        assert eff_result == exp_result


def test_csv_writers(tmpdir):
    parent_folder = str(tmpdir.join('parent_folder'))
    mkdir(parent_folder)
    tables_map = {
        'table1': ['col11', 'col12', 'col13'],
        'table2': ['col21', 'col22', 'col23'],
    }
    writers = utils.open_csv_writers(parent_folder, tables_map)
    for key, cols in tables_map.items():
        csv_path = join(parent_folder, '%s.csv' % key)
        assert exists(csv_path)
        assert key in writers
        writer, fp = writers[key]
        assert isinstance(writer, csv.DictWriter)
        assert writer.fieldnames == cols
        assert isinstance(fp, TextIOWrapper)
        assert fp.name == csv_path
        assert fp.mode == 'w'
        assert not fp.closed

    utils.close_csv_writers(writers)
    for key, cols in tables_map.items():
        csv_path = join(parent_folder, '%s.csv' % key)
        writer, fp = writers[key]
        assert fp.closed
        with open(csv_path) as csv_file:
            assert csv_file.read() == ';'.join(cols) + '\n'

    writers = utils.open_csv_writers(parent_folder, tables_map)
    for key, _ in tables_map.items():
        writer, fp = writers[key]
        assert fp.mode == 'a'
    utils.close_csv_writers(writers)
