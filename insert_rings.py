#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

**insertbuffer.py**

* *Purpose:* Insert buffer in the database.

* *python version:* 3.4
* *author:* Pedro Montero
* *license:* INTECMAR
* *requires:* intecmar.fichero, shapefile,intecmar,
        psycopg (download from http://www.stickpeople.com/projects/python/win-psycopg/)
* *date:* 2018/07/23
* *version:* 0.5.0
* *date version:* 2020/02/05


This software was developed by INTECMAR.

**Project:** CleanAtlantic

"""


import psycopg2
import shapefile
import sys

import json
from collections import OrderedDict

from osgeo import ogr
from shapely.wkt import loads


def read_connection(input_file):
    try:
        with open(input_file, 'r') as f:
            return json.load(f, object_pairs_hook=OrderedDict)
    except FileNotFoundError:
        print(f'File not found: {input_file} ')
        if input('Do you want to create one (y/n)?') == 'n':
            quit()


def main():
    """
    Main program.
    This program begins reading a keyword file: insertbuffer.dat


    # WARNING:
    #
    #           Production server host: svr_ide_1
    #           Developing server host: svr_dev_1
    #
    # CHANGE IF NEEDED"""

    database_data = read_connection(r'../pass/svr_dev_1.json')
    file_in = r'./rings/SILL_2PG.shp'

    connection_string = 'host={0} port={1} dbname={2} user={3} password={4}'.format(database_data['host'],
                                                                                    database_data['port'],
                                                                                    database_data['dbname'],
                                                                                    database_data['user'],
                                                                                    database_data['password'])
    try:
        conn = psycopg2.connect(connection_string)
    except psycopg2.OperationalError as e:
        print('CAUTION: ERROR WHEN CONNECTING TO {0}'.format(database_data['host']))
        sys.exit()

    cur = conn.cursor()


    # le os buffers
    basins = ogr.Open(file_in)
    layer = basins.GetLayer()

    geoms = []

    for feature in layer:
        id_site = feature.GetField("fk_site")
        id_range = feature.GetField("id_range")
        print(id_site, id_range)
        geom = feature.GetGeometryRef()
        polygon_wkt = geom.ExportToWkt()
        sql = '''INSERT INTO  waves.range_cells( fk_site, range_id, polygon)
                        VALUES (%s, %s ,ST_GeomFromText(%s, 4326))'''
        params = (id_site, id_range, polygon_wkt)
        cur.execute(sql, params)
        conn.commit()

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()