#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import csv
import sys
from textwrap import wrap

def fetch_gatekeeper_projects(db_name='data.db'):
    connection = sqlite3.connect(db_name)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    query='''
        SELECT geoaddr, year, month, description
        FROM project WHERE geoaddr IN (
            SELECT DISTINCT geoaddr
            FROM project
            WHERE description LIKE '%gatekeeper%'
        ) ORDER BY geoaddr, year ASC, month ASC
    '''
    writer = csv.writer(sys.stdout)
    writer.writerow(['address', 'date', 'status'])
    last_desc=''
    cursor.execute(query)
    for row in cursor.fetchall():
        if row['description'] != last_desc:
            last_desc = row['description']
            writer.writerow([
                row['geoaddr'],
                f'{row["year"]}-{row["month"]}',
                '\n'.join(wrap(row['description']))
            ])


if __name__ == '__main__':
    fetch_gatekeeper_projects()
