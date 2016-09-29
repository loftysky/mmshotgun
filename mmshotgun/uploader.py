import argparse
import fcntl
import logging
import os
import sys
import re
import functools
import threading
import time
import socket

from shotgun_api3_registry import connect


log = logging.getLogger(__name__)


# Deactivate our use of the cache.
connect = functools.partial(connect, use_cache=False)


def hard_timeout(timeout):
    time.sleep(timeout)
    print >> sys.stderr, 'HARD TIMEOUT; EXITING!'
    os._exit(1)

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--pid-file')
    parser.add_argument('-t', '--soft-timeout', type=float, default=30)
    parser.add_argument('-T', '--hard-timeout', type=float, default=30 * 60)
    parser.add_argument('--since', nargs='?', default='1M',
        help="# HOURS|DAYS|WEEKS|MONTHS|YEARS")
    args = parser.parse_args()

    # Set up soft and hard timeouts.
    socket.setdefaulttimeout(args.soft_timeout)
    thread = threading.Thread(target=hard_timeout, args=[args.hard_timeout])
    thread.daemon = True
    thread.start()

    # Try to get a lock on the pid file, to prevent other CRON jobs from
    # stomping all over this.
    if args.pid_file:
        pid_file = open(args.pid_file, 'a+')
        try:
            fcntl.lockf(pid_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except:
            print >> sys.stderr, 'uploader is already running as %s' % pid_file.read().strip()
            exit(-1)
        else:
            pid_file.seek(0)
            pid_file.truncate()
            pid_file.write('%d\n' % os.getpid())
            pid_file.flush()

    m = re.match(r'''^(\d+)\s*(
        H(OURS?)? |
        D(AYS?)? |
        W(WEEKS?)? |
        M(MONTHS?)? |
        Y(EARS?)?
    )$''', args.since.strip().upper(), re.VERBOSE)
    if not m:
        parser.print_usage()
        return 1

    since_qty, since_unit = m.group(1, 2)
    since_qty = int(since_qty)
    since_unit = {
        'H': 'HOUR',
        'D': 'DAY',
        'W': 'WEEK',
        'M': 'MONTH',
        'Y': 'YEAR',
    }[since_unit[0]]

    try:
        upload_changed_movies(since=(since_qty, since_unit))
    except:
        log.exception('Error while uploading changed movies in last {:d} {}'.format(since_qty, since_unit.lower()))


def upload_changed_movies(since=(1, 'MONTH')):

    sg = connect()

    log.info('Retrieving versions in last {:d} {}...'.format(since[0], since[1].lower()))
    entities = sg.find("Version",
        filters=[
            ('created_at', 'in_last', since[0], since[1]),
            ('sg_path_to_movie', 'is_not', None),
        ], fields=[
            'created_at',
            'sg_path_to_movie',
            'sg_task',
            'sg_uploaded_movie',
            'sg_uploaded_movie_transcoding_status',
        ], order=[
            {'field_name': 'created_at', 'direction': 'desc'},
        ],
    )

    num_uploaded = 0

    for entity in entities:

        if entity['sg_uploaded_movie'] is not None:
            continue

        if entity['sg_uploaded_movie_transcoding_status']:
            # It has been uploaded, but is awaiting transcode.
            # I'm not totally sure how this field works, but this is the safe
            # assumption to make.
            log.info('{type} {id} is transcoding'.format(**entity))
            continue

        try:
            num_uploaded += int(bool(upload_movie(entity)))
        except:
            log.exception('Error while uploading {type} {id}'.format(**entity))

    log.info('Checked {:d} entities; uploaded {:d} movies'.format(len(entities), num_uploaded))


def upload_movie(entity, sg=None):

    uploaded_movie = entity['sg_uploaded_movie']
    if uploaded_movie:
        log.warning('{type} {id} already has an uploaded movie; skipping'.format(**entity))
        return

    path_to_movie = entity['sg_path_to_movie']
    if not path_to_movie:
        log.warning('{type} {id} does not have a path to a movie; skipping'.format(**entity))
        return

    if not os.path.exists(path_to_movie):
        log.warning('{type} {id} has a non existant movie at {sg_path_to_movie}; skipping'.format(**entity))
        return

    log.info('Uploading {sg_path_to_movie} to {type} {id}...'.format(**entity))

    name = os.path.splitext(os.path.basename(path_to_movie))[0]

    sg = sg or connect()
    sg.upload(entity['type'], entity['id'], path_to_movie, 'sg_uploaded_movie', name)

    return True



if __name__ == '__main__':
    if not logging.getLogger().handlers:
        logging.basicConfig()
    main()
