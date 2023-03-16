#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
import json
import sqlite3
import calendar
from pathlib import Path
from pypdf import PdfReader


meta_re = re.compile(r"[•] +([^:]+):(.+)$")
entry_re = re.compile(r' ?(\d{1,3})\. (.{5,})$')
header_re = re.compile(r'[A-Z -]{20,}($|\()')
paren_re = re.compile(r'(.+)\(([^)]+)\) *$')
page_head_re = re.compile(
    '\s*planning division update\s*(.{5,25})page \d+ of \d+', 
    re.MULTILINE | re.IGNORECASE | re.DOTALL
)
lookup = {m: n for n, m in enumerate(calendar.month_name)}


def make_table(connection):
    cursor = connection.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS project(
           pk text NOT NULL UNIQUE,
           normtitle text NOT NULL,
           title text NOT NULL,
           title_alt text COLLATE NOCASE,
           header text COLLATE NOCASE,
           description text,
           year integer NOT NULL,
           month integer NOT NULL,
           ordinal integer,
           page integer,
           plainning_area text COLLATE NOCASE,
           project_planner text COLLATE NOCASE,
           applicant text COLLATE NOCASE,
           status text COLLATE NOCASE,
           geoaddr text COLLATE NOCASE)
    ''')


def get_year_month(date: str) -> (int, int):
    '''Sometimes date had day, sometimes not'''
    parts = date.strip().split(' ')
    month = parts[0]
    year = parts[-1]
    # random spaces everywhere
    if len(year) < 4:
        year = parts[-2] + year
    return int(year), lookup[month]


def parse_into_db(filename: str, db_name: str = 'data.db'):
    connection = sqlite3.connect(db_name)
    make_table(connection)
    cursor = connection.cursor()
    date, projects = parse(filename)
    year, month = get_year_month(date)

    try:
        for project in projects:
            pks = re.sub('[^\w]+', '', project['title']).lower()
            cursor.execute(
                'INSERT INTO project VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                [
                    f'{year}-{month}-{pks}',
                    pks,
                    project['title'],
                    project.get('title_alt'),
                    project.get('header'),
                    project['description'],
                    year,
                    month, 
                    project.get('ordinal'),
                    project.get('page'),
                    project.get('Planning Area'),
                    project.get('Project Planner'),
                    project.get('Applicant'),
                    project.get('Status')
                ]
            )
    except Exception as e:
        print(f'DBFAIL {e} {filename} {project}')
    finally:
        connection.commit()
        connection.close()


def parse(filename: str):
    try:
        reader = PdfReader(filename)
        projects, state = [], {}
        for n, page in enumerate(reader.pages[:-1]):
            state['page'] = n + 1
            parse_page(page, projects, state)

        add_project(projects, state)
        return state['date'], projects
    except Exception:
        print(f'PARSEFAIL for {filename}')
        raise


def parse_date(text, state):
    # sometimes the date is in multiple lines
    # gotta use a multiline regex
    parts = re.split(page_head_re, text)
    if len(parts) > 1 and not state.get('date'):
        # missing newlines. bleah. will try anyway
        state['date'] = parts[1].strip()
    return parts[-1].strip()


def split(text):
    lines = text.split('\n')
    if len(lines) == 1 and len(text) > 100:
        lines = re.split('  +', text)
    return lines


def parse_page(page, projects, state):
    text = page.extract_text()
    text = parse_date(text, state)
    lines = split(text)
    if len(lines) < 3:
        print('SHORT PAGE', lines)
        return

    for n in range(3):
        if re.match(header_re, lines[n]):
            state['header'] = lines[n].strip()
            lines = lines[n:]
            break
    else:
        if re.match(header_re, lines[-1]):
            state['header'] = lines[-1].strip()
            lines = lines[:-1]

    for line in lines:
        if not line.strip():
            # skip empty lines
            continue

        # check for new project. line starting with a number
        match = re.match(entry_re, line)
        if match:
            add_project(projects, state)
            ordinal = int(match.group(1))
            state['project'] = {
                'ordinal': ordinal,
                'page': state['page'],
                'title': match.group(2).strip(),
                'header': state.get('header'),
                'description': []
            }
            match = re.match(paren_re, state['project']['title'])
            if match:
                # preference address for main title
                if re.match('\d+ ', match.group(2)):
                    state['project']['title_alt'] = match.group(1)
                    state['project']['title'] = match.group(2)
                else:
                    state['project']['title'] = match.group(1)
                    state['project']['title_alt'] = match.group(2)
            continue

        if re.match(header_re, line):
            # starting a new section
            state['header'] = line.strip()
            add_project(projects, state)
            continue

        # Don't check for description or metadata if we have no project
        if not state.get('project'):
            continue

        # metadata such as project planner, planning area
        match = re.match(meta_re, line)
        if match:
            # '/' is in "Status/Next Steps". Fix inconsistancies by removeing 2nd part
            key = match.group(1).split('/')[0].strip()
            value = match.group(2).strip()
            state['project'][key] = value
            continue

        # If nothing else matches assume it is just a continuation of the description
        state['project']['description'].append(line.strip())


def add_project(projects, state):
    if state.get('project'):
        proj = state.pop('project')
        if not proj['description']:
            print('!Projecct Without DESCR', proj)
        split1 = re.split(paren_re, proj['description'][-1])
        if len(split1) > 1:
            _, line, planner, _ = split1
            proj['Project Planner'] = planner
            split2 = re.split(paren_re, line)
            if len(split2) > 1:
                _, line, area, _ = split2
                proj['Planning Area'] = area
            proj['description'][-1] = line

        proj['description'] = fixup_description(proj['description'])
        projects.append(proj)


def fixup_description(desc_list):
    # get rid of some of the random spaces
    desc = ' '.join(desc_list)
    desc = re.sub(r'\s+', ' ', desc)
    desc = re.sub(r' ([.,:;)/-])', r'\1', desc)
    return desc


if __name__ == '__main__':
    path = Path(sys.argv[1])
    if path.is_dir():
        for file in path.glob('*.pdf'):
            parse_into_db(file)
    else:
        _, projects = parse(path)
        for project in projects:
            print(' PROJ ', project['title'], project['ordinal'])
