#!/bin/env python


import errno
import json
import logging
import os
import queue
import re
import shutil
import sys
import threading as tr
import time
from argparse import ArgumentParser
from collections import defaultdict

from prettytable import PrettyTable
from progress.bar import Bar
from progress.spinner import Spinner
from rarfile import RarFile


MEDIA_EXTS = [
    '.avi',
    '.mkv',
]

EPISODE_FMTS = [
    f'[sS](\d+)[eE](\d+)',  # s01e01
    f'(\d+)[xX](\d+)'       # 1x01
]


MODES = (
    'copy',
    'link',
    'move',
)
DEFAULT_MODE = 'move'


LOG = logging.getLogger('organize')


def get_episode_info(p):
    """
    :param str p: episode path

    :return: season + episode
    :rtype: tuple (int, int)
    """
    season, episode = None, None

    _, name = os.path.split(p)

    for fmt in EPISODE_FMTS:
        match = re.search(fmt, name)

        if match:
            season = int(match.group(1))
            episode = int(match.group(2))
            break

    if not episode:
        raise ValueError(f'could not parse episode: {p}')

    return season, episode


def get_media(p, m=None):
    """
    Recursively collect media item paths in a directory.

    :param str p: directory

    :return: collected media item paths as a flattened list
    :rtype: list
    """
    if m is None:
        m = []

    if not os.path.isdir(p):
        return []

    for i in os.listdir(p):
        item = os.path.join(p, i)
        if os.path.isdir(item):
            get_media(item, m)
            continue

        name, ext = os.path.splitext(item)
        if ext == '.rar':
            if not is_media_rar(item):
                continue
        elif ext not in MEDIA_EXTS:
            continue

        m.append(item)

    return m


def is_season(t):
    """
    Check if a directory has a season-like basename.

    :param str t: path to be checked
    
    :return: whether the directory is season-like
    :rtype: bool
    """
    return bool(re.match(f'season \d\d', os.path.basename(t)))


def get_season_target(t, s):
    return os.path.join(t, f'season {s:02}')


def get_target_path(source, target):
    season, episode = get_episode_info(source)

    if not is_season(target):
        target = get_season_target(target, season)

    _, ext = os.path.splitext(source)
    filename = f's{season:02}e{episode:02}{ext}'
    return os.path.join(target, filename)


def is_rar_first_volume(r):
    d, _ = os.path.split(r)

    ext_counts = defaultdict(int)
    parts = []
    for i in os.listdir(d):
        n, ext = os.path.splitext(i)
        ext_counts[ext] += 1

        match = re.search(r'(\d+)$', n)
        if match:
            p = int(match.group(1))
            parts.append((p, os.path.join(d, i)))

    rar_count = ext_counts.get('.rar')
    if rar_count > 1:
        parts.sort(key=lambda p: p[0])
        return parts[0][1] == r
    elif rar_count == 1:
        return True
    else:
        return False


def is_rar_media_file(r):
    has_media = False

    rf = RarFile(r)
    for f in rf.infolist():
        _, ext = os.path.splitext(f.filename)
        has_media |= ext in MEDIA_EXTS

    return has_media


def is_media_rar(r):
    if not is_rar_first_volume(r):
        return False

    return is_rar_media_file(r)


def extract_rar(j):
    """
    Extract a rar archive and update the job so that the extracted
    file is organized instead of the archive.
    
    :param dict j: organization job

    :return: extraction success
    :rtype: bool
    """
    rf = RarFile(j['s'])

    media = None
    for f in rf.infolist():
        _, ext = os.path.splitext(f.filename)
        if ext in MEDIA_EXTS:
            media = f
            break

    if not media:
        return False

    d, _ = os.path.split(j['s'])
    extract_path = os.path.join(d, media.filename)

    LOG.info(f"extract {j['s']} -> {extract_path}")
    rf.extract(rf.infolist()[0], path=extract_path)

    j['s'] = extract_path
    target_dir, _ = os.path.split(j['t'])
    j['t'] = get_target_path(extract_path, target_dir)

    return True


def organize(j):
    _, ext = os.path.splitext(j['t'])
    if ext == '.rar':
        if not extract_rar(j):
            return

    d, _ = os.path.split(j['t'])
    makedirs(d)

    if j['a'] == 'copy':
        copy(j['s'], j['t'])
    elif j['a'] == 'link':
        link(j['s'], j['t'])
    elif j['a'] == 'move':
        move(j['s'], j['t'])

    LOG.info(f"{j['a']} {j['s']} -> {j['t']}")
    return True


def organize_worker(q, f):
    while True:
        try:
            job = q.get_nowait()
        except queue.Empty:
            break

        try:
            r = organize(job)
        except Exception as e:
            result = (job, e)
        else:
            result = (job, r)

        f.put(result)
        q.task_done()


def makedirs(p):
    try:
        os.makedirs(p)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def copy(s, t):
    shutil.copy(s, t)


def link(s, t):
    os.link(s, t)


def move(s, t):
    shutil.move(s, t)


def _collect_spinner(s, q):
    while True:
        try:
            q.get_nowait()
        except queue.Empty:
            s.next()
            time.sleep(0.05)
        else:
            break


def main(**kwargs):
    sources = kwargs.get('sources', [])
    source_media = []

    spinner = Spinner('Collecting media... ')
    spinner_q = queue.Queue()
    tr.Thread(target=_collect_spinner, args=(spinner, spinner_q,)).start()

    for s in sources:
        src = os.path.abspath(s)
        for m in get_media(src):
            source_media.append(m)

    spinner_q.put(None)
    spinner.finish()

    if not source_media:
        print(f'no media found in {sources}')
        sys.exit(1)

    target = kwargs['target']

    jobs = []
    for source_item in source_media:
        try:
            target_item = get_target_path(source_item, target)
        except ValueError as e:
            print(e.args[0])
            continue

        jobs.append({
            'a': kwargs['mode'],
            's': source_item,
            't': target_item
        })

    if kwargs.get('dry_run'):
        print(json.dumps(jobs, indent=2, sort_keys=True))
        sys.exit(0)

    # create the progress bar
    print()
    bar = Bar('Organizing', max=len(jobs))

    if kwargs.get('serial') is True:
        for j in jobs:
            organize(j)
            bar.next()
        bar.finish()
        return

    job_q = queue.Queue()
    for j in jobs:
        job_q.put(j)
    finished = queue.Queue()

    worker_count = min(64, len(jobs))
    for _ in range(worker_count):
        tr.Thread(target=organize_worker, args=(job_q, finished)).start()

    done = 0
    dir_counts = defaultdict(int)
    failed = []
    while done < len(jobs):
        job, result = finished.get()
        if result is True:
            d, _ = os.path.split(job['t'])
            dir_counts[d] += 1
        elif isinstance(result, Exception):
            failed.append({
                'job': job,
                'error': result.args[0]
            })

        done += 1
        bar.next()

    job_q.join()
    bar.finish()

    if dir_counts:
        table = PrettyTable(field_names=['Directory', 'Items Added'])
        for d, cnt in dir_counts.items():
            table.add_row((d, cnt))

        table.sort_by = 'Directory'

        print()
        total = sum(dir_counts.values())
        print(table.get_string(title=f'Added {total} media items'))
    else:
        print('no media found')

    if failed:
        print(f'\n{len(failed)} jobs failed; see failed.json for details')
        with open('failed.json', 'w+') as f:
            json.dump(failed, f, indent=2, sort_keys=True)


DESC = '''
Scan a directory to find, organize, and add media to the `/tank/media` directory.
'''


if __name__ == '__main__':
    p = ArgumentParser(description=DESC)
    p.add_argument('--source', required=True, action='append', dest='sources',
                   help='move files from the given path')
    p.add_argument('--target', required=True, help='organize + move files to the given path')
    p.add_argument('-m', '--mode', choices=MODES, default=DEFAULT_MODE)
    p.add_argument('--serial', action='store_true', help='run jobs serially')
    p.add_argument('--dry-run', action='store_true')

    logging.basicConfig(level=logging.DEBUG,
                        filename='organize.log',
                        format='[%(asctime)s] %(msg)s')

    main(**vars(p.parse_args()))
