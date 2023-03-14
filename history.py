#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
from textwrap import wrap


def get_titles(cursor):
    cursor.execute(
        'SELECT normtitle FROM project '
        'GROUP BY normtitle ORDER BY count(*) DESC'
    )
    for title, in cursor.fetchall():
        yield title


def get_history(db_name: str = 'data.db'):
   connection = sqlite3.connect(db_name)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    for normtitle in get_titles(cursor):
        cursor.execute(
            'SELECT year, month, description, title, title_alt '
            'FROM project WHERE normtitle = ? '
            'ORDER BY year, month',
            (normtitle,)
        )
        first = cursor.fetchone()
        data = {
            'title': first['title'],
            'subtitle': first['title_alt'],
            'normtitle': normtitle,
            'history': [
                {
                    'year': first['year'],
                    'month': first['month'],
                    'description': first['description']
                }
            ]
        }
        for row in cursor.fetchall():
            data['history'].append(
                {
                    'year': row['year'],
                    'month': row['month'],
                    'description': row['description']
                }
            )
        yield data


if __name__ == '__main__':
    for thing in get_history():
        print(thing['title'], thing['normtitle'])
        last_desc = ''
        for n, h in enumerate(thing['history']):
            print(h['year'], h['month'])
            if h['description'] == last_desc:
                print(' " ')
                continue
            for line in wrap(h['description']):
                print(line)
            last_desc = h['description']
        print('---')
