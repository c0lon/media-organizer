#!/usr/bin/env python


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
    r'[sS](\d+)[eE](\d+)',  # s01e01
    r'(\d+)[xX](\d+)'       # 1x01
]

SEASON_FMTS = [
    r'[sS](?:eason ?)?(\d+)',
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
    Extract season + episode numbers from a media item path.

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


def get_season_number(p):
    """
    Extract a season number from a path, if possible.

    :param str p: path to extract season number from
    
    :return: whether the directory is season-like
    :rtype: int
    :raises: ValueError
    """
    d = os.path.basename(p)

    for f in SEASON_FMTS:
        match = re.search(f, d)
        if match:
            return int(match.group(1))

    raise ValueError('not a season: {p}')


def is_season_dir(p):
    """
    Check if a path is a directory containing a season
    of media items.

    :param str p: path

    :return: whether the path is a season directory
    :rtype: bool
    """
    if not os.path.isdir(p):
        return False

    try:
        get_season_number(p)
    except ValueError:
        return False

    return True


def get_media(p, m=None):
    """
    Recursively collect media items in a directory.

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

        if is_season_dir(item):
            get_media(item, m)

        elif os.path.isfile(item):
            name, ext = os.path.splitext(item)

            if ext == '.rar':
                if not is_media_rar(item):
                    continue
            elif ext not in MEDIA_EXTS:
                continue

            m.append(item)

    return m


def get_season_target(t, s):
    return os.path.join(t, f'season {s:02}')


def get_target_path(source, target):
    season, episode = get_episode_info(source)

    try:
        target_season = get_season_number(target)
    except ValueError:
        target = get_season_target(target, season)
    else:
        assert target_season == season

    _, ext = os.path.splitext(source)
    filename = f's{season:02}e{episode:02}{ext}'
    return os.path.join(target, filename)


def is_rar_first_volume(r):
    """
    Check if a rar file is the first file in the volume, so that
    it can be extracted.

    :param str r: rar file path

    :return: whether the rar file is the first of the volume
    :rtype: bool
    """
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
    """
    Check if a rar archive contains media files.

    :param str r: rar file path
    
    :return: whether the rar archive contains media files
    :rtype: bool
    """
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


def write_json(j, p, fancy=True):
    with open(p, 'w+') as f:
        json.dump(j, f, indent=2, sort_keys=True)


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

    # use a spinner while we collect media items
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

    job_path = kwargs.get('print_jobs')
    if job_path:
        job_path = os.path.abspath(job_path)
        if os.path.isfile(job_path):
            r = input(f'{job_path} already exists, overwrite (y/n): ')
            if r.lower().startswith('y'):
                write_json(jobs, job_path)

    if kwargs.get('dry_run'):
        print(json.dumps(jobs, indent=2, sort_keys=True))
        return

    # use a progress bar while we organize
    print()
    bar = Bar('Organizing', max=len(jobs))

    # if kwargs.get('serial') is True:
    if kwargs.get('serial') is True or kwargs.get('mode') == 'COPY':
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

    # print results via prettytable
    if dir_counts:
        table = PrettyTable(field_names=['Directory', 'Items Added'])
        for d, cnt in dir_counts.items():
            table.add_row((d, cnt))

        table.sort_by = 'Directory'

        total = sum(dir_counts.values())
        print(f'Added {total} media items to {len(dir_counts)} directories.')
        print(table.get_string())
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
    p.add_argument('--print-jobs', help='write organization jobs to a JSON file')
    p.add_argument('-m', '--mode', choices=MODES, default=DEFAULT_MODE)
    p.add_argument('--serial', action='store_true', help='run jobs serially')
    p.add_argument('--dry-run', action='store_true')

    logging.basicConfig(level=logging.DEBUG,
                        filename='organize.log',
                        format='[%(asctime)s] %(msg)s')

    args = vars(p.parse_args())
    sys.exit(main(**args))
