# -*- coding: utf-8 -*-
"""
@author: Andreas
"""

import psycopg2 as pg2
import xml.etree.cElementTree as ET

conn = pg2.connect(database='Bohrarchiv', user='db_user', password='xxxx')

cur = conn.cursor()
print('Datenverbindung hergestellt')

data = []
i = 0

file = 'BOHRIS_S.P_SCHICHTDATEN_TG_7.gml'
path = ''

tree = ET.parse(path + file)
FeatureCollection = tree.getroot()
print('Datenquelle: ' + path + file + ' geladen')

for index, featureMember in enumerate(FeatureCollection):
    for daten in featureMember:
        if len(daten) > 2:
            data.append([])
            for j, element in enumerate(daten):
                # att[i].append(element.tag)
                if element.text == None:
                    data[i].append('NULL')
                else:    
                    data[i].append(element.text.replace('\'',''))
            sql_insert_tab_column = "INSERT INTO schichtdaten(ObjectID, Bezirk_Nr, ID_Stammdaten, ID_Schichtdaten, TK25, DGK5, Archivbezeichnung,E_DK5,E_Archivbezeichnung, Vertraulichkeit, Aufschlussbezeichnung, Bohrdatum, X_ETRS89, Y_ETRS89, RW_GK, HW_GK, Ansatzhoehe, GW_Flur, Endteufe, Obere_Teufe, Untere_Teufe, Stratigraphie, Hauptanteil, Nebenanteil, Farbe, Genese, Zusatz, Hydrographie)"
            sql_insert_values = (" VALUES (%s,%s,%s,%s,%s,%s,\'%s\',%s,\'%s\',\'%s\',\'%s\',\'%s\',%s,%s,%s,%s,%s,%s,%s,%s,%s,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\')"  % (data[i][0],data[i][1],data[i][2],data[i][3],data[i][4],data[i][5],data[i][6],data[i][7],data[i][8],data[i][9],data[i][10],data[i][11],data[i][12],data[i][13],data[i][14],data[i][15],data[i][16],data[i][17],data[i][18],data[i][19],data[i][20],data[i][21],data[i][22],data[i][23],data[i][24],data[i][25],data[i][26],data[i][27]))
            sql = sql_insert_tab_column + sql_insert_values
            cur.execute(sql)
            # print("Objekt:" + data[i][0] + " eingetragen")
            i+=1 

conn.commit()
conn.close()
print('Datenverbindung erfolgreich getrennt')
