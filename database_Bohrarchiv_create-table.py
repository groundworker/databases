# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 15:35:57 2021

@author: Andreas Sorgatz-Wenzel
"""

import psycopg2 as pg2

conn = pg2.connect(database='Bohrarchiv', user='db_user', password='xxxx')
cur = conn.cursor()

query="""
CREATE TABLE Schichtdaten (
        ObjectID            INT, 
        Bezirk_Nr           INT, 
        ID_Stammdaten       INT, 
        ID_Schichtdaten     INT, 
        TK25                INT, 
        DGK5                INT, 
        Archivbezeichnung   TEXT,
        E_DK5               TEXT,
        E_Archivbezeichnung TEXT, 
        Vertraulichkeit     TEXT, 
        Aufschlussbezeichnung TEXT, 
        Bohrdatum           TEXT, 
        X_ETRS89            REAL, 
        Y_ETRS89            REAL, 
        RW_GK               REAL, 
        HW_GK               REAL, 
        Ansatzhoehe         REAL, 
        GW_Flur             REAL, 
        Endteufe            REAL, 
        Obere_Teufe         REAL, 
        Untere_Teufe        REAL, 
        Stratigraphie       TEXT, 
        Hauptanteil         TEXT, 
        Nebenanteil         TEXT, 
        Farbe               TEXT, 
        Genese              TEXT, 
        Zusatz              TEXT, 
        Hydrographie        TEXT
            )"""

cur.execute(query)

conn.commit()
conn.close()