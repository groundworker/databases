# -*- coding: utf-8 -*-
"""
@author: Andreas Sorgatz-Wenzel
"""

import psycopg2 as pg2
import xml.etree.cElementTree as ET


def db_select(sqlquery):
    cur.execute(sqlquery)
    return cur.fetchone()


def db_select_all(sqlquery):
    cur.execute(sqlquery)
    return cur.fetchall()


def list_idschicht():
    list_idschicht = []
    for element in db_select_all(sqlquery='SELECT DISTINCT(ID_Schichtdaten) FROM schichtdaten;'):
        list_idschicht.append(element[0])
    return list_idschicht


conn = pg2.connect(database='HH_Bohrarchiv_2020', user='postgres', password="XXXX")
cur = conn.cursor()
print('Verbindung zur Datenbank hergestellt')

data = []
count_insert = 0
count_updateted = 0

file = 'BOHRIS_S.P_SCHICHTDATEN_TG_2.gml'
path = 'D:\\Datenbanken\\HH_Bohrarchiv\\2022\\'

# XML Datei öffnen
tree = ET.parse(path + file)
FeatureCollection = tree.getroot()
print('Datenquelle ' + file + ' geladen')

# Auslesen der XML Datei
data_index = 0
for index, featureMember in enumerate(FeatureCollection):
    for daten in featureMember:
        if len(daten) > 2:
            data.append([])
            for j, element in enumerate(daten):
                if element.text is None:
                    data[data_index].append('NULL')
                else:
                    data[data_index].append(element.text.replace('\'', ''))
            data_index += 1
print('Daten ('+str(len(data))+') aus ' + file + ' geladen')

# ist die Datenbank leer: alles einfügen
if db_select(sqlquery="SELECT COUNT(ID_Stammdaten) FROM schichtdaten")[0] == 0:
    print('Datenbank wird erstmalig befüllt')
    for element in data:
        sql_insert_tab_column = "INSERT INTO schichtdaten(ObjectID, Bezirk_Nr, ID_Stammdaten, ID_Schichtdaten, TK25, DGK5, Archivbezeichnung,E_DK5,E_Archivbezeichnung, Vertraulichkeit, Aufschlussbezeichnung, Bohrdatum, X_ETRS89, Y_ETRS89, RW_GK, HW_GK, Ansatzhoehe, GW_Flur, Endteufe, Obere_Teufe, Untere_Teufe, Stratigraphie, Hauptanteil, Nebenanteil, Farbe, Genese, Zusatz, Hydrographie)"
        sql_insert_values = (" VALUES (%s,%s,%s,%s,%s,%s,\'%s\',%s,\'%s\',\'%s\',\'%s\',\'%s\',%s,%s,%s,%s,%s,%s,%s,%s,%s,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\')"  % (element[0],element[1],element[2],element[3],element[4],element[5],element[6],element[7],element[8],element[9],element[10],element[11],element[12],element[13],element[14],element[15],element[16],element[17],element[18],element[19],element[20],element[21],element[22],element[23],element[24],element[25],element[26],element[27]))
        sql = sql_insert_tab_column + sql_insert_values
        cur.execute(sql)
        count_insert += 1
# ist die Datenbank nicht leer: Prüfung, welcher Wert neu ist
else:
    print('Datenbank wird aktualisiert')
    list_idschicht = list_idschicht()
    for index, element in enumerate(data):
        print(f'Element {index } von {len(data)}')
        # ist die BohrungsID bereits in der Datenbank
        if int(element[3]) not in list_idschicht:
            count_updateted += 1
            print('Bohrung '+str(element[2])+' mit Schicht '+str(element[3])+' wird nachgetragen ('+str(count_updateted)+')')
            sql_insert_tab_column = "INSERT INTO schichtdaten(ObjectID, Bezirk_Nr, ID_Stammdaten, ID_Schichtdaten, TK25, DGK5, Archivbezeichnung,E_DK5,E_Archivbezeichnung, Vertraulichkeit, Aufschlussbezeichnung, Bohrdatum, X_ETRS89, Y_ETRS89, RW_GK, HW_GK, Ansatzhoehe, GW_Flur, Endteufe, Obere_Teufe, Untere_Teufe, Stratigraphie, Hauptanteil, Nebenanteil, Farbe, Genese, Zusatz, Hydrographie)"
            sql_insert_values = (" VALUES (%s,%s,%s,%s,%s,%s,\'%s\',%s,\'%s\',\'%s\',\'%s\',\'%s\',%s,%s,%s,%s,%s,%s,%s,%s,%s,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\')"  % (element[0],element[1],element[2],element[3],element[4],element[5],element[6],element[7],element[8],element[9],element[10],element[11],element[12],element[13],element[14],element[15],element[16],element[17],element[18],element[19],element[20],element[21],element[22],element[23],element[24],element[25],element[26],element[27]))
            sql = sql_insert_tab_column + sql_insert_values
            cur.execute(sql)

print(str(count_insert)+' eingetragen')
print(str(count_updateted)+' aktualisiert')
conn.commit()
conn.close()
print('Verbindung zur Datenbank getrennt')
