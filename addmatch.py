#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Dict, Iterable, List, Generator, Tuple
import sqlite3
import json
import csv
import sys
import re
from pathlib import Path
from geocodio import GeocodioClient


KEY='5f06205d63ddc05b02004b0c3fbcd4fc4300b26'
CACHE_PATH = Path('./data/geocode_cache.json')


def get_client():
    return GeocodioClient(KEY)


def load_cache(cursor) -> Dict[str, str]:
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lookup_cache(
          orig TEXT NOT NULL UNIQUE,
          norm TEXT,
          data TEXT
        )
    ''')
    cursor.execute('SELECT * FROM lookup_cache')
    return {r['orig']: r['norm'] for r in cursor.fetchall()}


def load_json_cache(cursor, path):
    existing = set(load_cache(cursor))
    print('existing', len(existing))
    with open(path) as fob:
        data = json.load(fob)
    to_add = []
    for k, v in data.items():
        (_, normd), = normalize_addr([k])
        if normd and normd not in existing:
            if 'error' in v:
                coded = None
            else:
                coded = v['results'][0]['formatted_address']
            to_add.append((normd, coded, json.dumps(v)))
    if to_add:
        update_cache(cursor, to_add)


def renorm_cache(cursor):
    cursor.execute('SELECT * FROM lookup_cache')
    cache = {r['orig']: r for r in cursor.fetchall()}
    normd = dict(normalize_addr(cache.keys()))
    to_resub = [(normd[k], v['norm'], v['data'])
                for k, v in cache.items()
                if normd[k] and normd[k] != k and normd[k] not in normd]
    print('resub', len(cache), len(to_resub))
    update_cache(cursor, to_resub)


def update_cache(cursor, values):
    print('adding to cache', len(values))
    cursor.executemany(
        'INSERT or REPLACE INTO lookup_cache VALUES(?, ?, ?)',
        values
    )


def normalize_addr(addresses: Iterable[str]) -> Generator[str, None, None]:
    for address_r in addresses:
        address = address_r.strip().casefold()
        if not re.match(r'\d+', address):
            in_paren = re.search(r'\((\d+[^)]+)', address)
            if in_paren:
                address = in_paren.group(1)
            else:
                print ('skipping', address)
                yield address_r, None
                continue

        # multiple addresses (134-139 example st)
        address = re.sub(r'(\d+)\s*(,|-|&|and)\s*\d+ ', r'\1 ', address)
        # whitespace cleanup
        address = re.sub(f'\s+', ' ', address)
        # add city
        address = re.split('[,(]', address)[0] + ', mountain view, ca'

        yield address_r, address


def lookup(connection, addresses: Iterable[str]) -> Dict[str, str]:
    """
    Please don't run this function in parallel, because the cache isn't thread-safe.
    Since it takes an Iterable, it's easy to use this with a Pandas series:
        df['geocode_results'] = geocode_cache.lookup(df['address'])
    """
    cache = load_cache(connection.cursor())
    # lookup normalized address to input address
    normd = dict(normalize_addr(addresses))
    addresses_to_lookup = [a for a in set(normd.values()) - set(cache.keys()) if a]
    print('addr', len(normd), 'cache', len(cache))

    if len(addresses_to_lookup):
        print('looking up', len(addresses_to_lookup))
        api_results = get_client().geocode(addresses_to_lookup)
        values = []
        for address, response in zip(addresses_to_lookup, api_results):
            if 'error' in response:
                coded = None
            else:
                coded = response['results'][0]['formatted_address']
            cache[address] = coded
            values.append((address, coded, json.dumps(response)))

        update_cache(connection.cursor(), values)
        connection.commit()

    return {k: cache.get(v) for k, v in normd.items() if k and v}


def get_addrs(cursor):
    cursor.execute(
        'SELECT DISTINCT title FROM project'
    )
    for title, in cursor.fetchall():
        yield title


def update_addr(connection, mapping):
    cursor = connection.cursor()
    cursor.executemany(
        'UPDATE project SET geoaddr=? WHERE title=?',
        [(v, k) for k, v in mapping.items()]
    )
    connection.commit()


def import_permit_data(connection):
    with open('data/all_permits.csv') as fob:
        reader = csv.DictReader(fob)
        data = {r['address']: dict(**r) for r in reader}

    addr_data = lookup(connection, data.keys())
    return {v: data[k] for k, v in addr_data.items() if v}


def import_table_a(connection):
    with open('data/hcd_table_a.csv') as fob:
        reader = csv.DictReader(fob)
        data = {r['Site Address/Intersection']: dict(**r) for r in reader}

    addr_data = lookup(connection, data.keys())
    return {v: data[k] for k, v in addr_data.items() if v}


def pdf_data(cursor):
    cursor.execute(
        'SELECT geoaddr, title, year, month, description FROM project '
        'GROUP BY geoaddr '
        'HAVING year*100 + month = min(year*100 + month) '
    )
    oldest = {r['geoaddr']: dict(r) for r in cursor.fetchall() if r['geoaddr']}
    cursor.execute(
        'SELECT geoaddr, title_alt, year, month, description FROM project '
        'GROUP BY geoaddr '
        'HAVING year*100 + month = max(year*100 + month) '
    )
    most_recent = {r['geoaddr']: dict(r) for r in cursor.fetchall() if r['geoaddr']}
    return oldest, most_recent


def match_addrs(db_name='data.db'):
    connection = sqlite3.connect(db_name)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    oldest, most_recent = pdf_data(cursor)
    permit_data = import_permit_data(connection)
    table_a_data = import_table_a(connection)
    keys = set(oldest)
    writer = csv.writer(sys.stdout)
    writer.writerow((
        'address',
        'title',
        'alt title',
        'first appearance month',
        'first description',
        'latest apperance month',
        'latest description',
        'permit category',
        'permitted units',
        'max density',
        'parcel size',
        'site status',
        'identified in last 2 cycles'
    ))

    for key in sorted(keys):
        old = oldest[key]
        recent = most_recent[key]
        permit = permit_data.get(key, {})
        tablea = table_a_data.get(key, {})
        writer.writerow((
            key,
            old['title'],
            recent['title_alt'],
            f'{old["year"]}-{old["month"]}',
            old['description'],
            f'{recent["year"]}-{recent["month"]}',
            recent['description'],
            permit.get('hcategory', 'no permit'),
            permit.get('totalunit', '0'),
            tablea.get('Max Density Allowed (units/acre)'),
            tablea.get('Parcel Size (Acres)'),
            tablea.get('Site Status'),
            tablea.get('Identified in Last/Last Two Planning Cycle(s)')
        ))


if __name__ == '__main__':
    match_addrs()
    '''
    import sys
    connection = sqlite3.connect('data.db')
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    #load_json_cache(cursor, sys.argv[1])
    #connection.commit()
    renorm_cache(cursor)
    connection.commit()

    #'''
