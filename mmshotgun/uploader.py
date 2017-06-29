import argparse
import fcntl
import functools
import logging
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
import contextlib
import shutil

from shotgun_api3_registry import connect


log = logging.getLogger(__name__)


# Deactivate our use of the cache.
connect = functools.partial(connect, use_cache=False)


MOVIE_EXTS = set('''
    .mov
    .mp4
    .mkv
    .avi
    .mxf
'''.strip().split())

AUDIO_EXTS = set('''
    .wav
'''.strip().split())

IMAGE_EXTS = set('''
    .jpg
    .jpeg
    .tga
    .png
'''.strip().split())



def hard_timeout(timeout):
    time.sleep(timeout)
    print >> sys.stderr, 'HARD TIMEOUT; EXITING!'
    os._exit(1)


@contextlib.contextmanager
def tempdir():
    dir_ = tempfile.mkdtemp('mmshotgun.uploader')
    try:
        yield dir_
    except:
        raise
    else:
        shutil.rmtree(dir_)


@contextlib.contextmanager
def temp_transcode(src, ext):

    dir_ = tempfile.mkdtemp('mmshotgun.uploader')

    name = os.path.splitext(os.path.basename(src))[0]
    dst = os.path.join(dir_, name + ext)

    if ext == '.mp4':
        subprocess.check_call([
            'ffmpeg',
            '-i', src,
            '-strict', 'experimental',
            '-acodec', 'aac',
                '-ab', '160k', '-ac', '2',
            '-vcodec', 'libx264',
                '-pix_fmt', 'yuv420p', '-vf', 'scale=trunc((a*oh)/2)*2:720',
                '-g', '30', '-b:v', '2000k', '-vprofile', 'high', '-bf', '0',
            '-f', 'mp4',
            dst
        ])

    elif ext == '.webm':
        yield
        return
        subprocess.check_call([
            'ffmpeg',
            '-i', src,
            '-acodec', 'libvorbis',
                '-aq', '60', '-ac', '2',
            '-vcodec', 'libvpx',
                '-pix_fmt', 'yuv420p', '-vf', 'scale=trunc((a*oh)/2)*2:720',
                '-g', '30', '-b:v 2000k', '-vpre 720p',
                #'-quality', 'realtime',
                #'-cpu-used', '0', 
                '-qmin', '10', '-qmax', '42',
            '-f', 'webm',
            dst
        ])

    else:
        raise ValueError('Unknown ext', ext)

    try:
        yield dst
    except:
        raise
    else:
        shutil.rmtree(dir_)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--pid-file')
    parser.add_argument('-t', '--soft-timeout', type=float, default=30)
    parser.add_argument('-T', '--hard-timeout', type=float, default=60 * 60)
    parser.add_argument('--no-transcode', action='store_true',
        help="Upload the original without transcoding.")
    parser.add_argument('--since', nargs='?', default='1M',
        help="# HOURS|DAYS|WEEKS|MONTHS|YEARS")

    parser.add_argument('-c', '--count', type=int,
        help="Only process this many versions.")

    parser.add_argument('-i', '--ids', action='store_true',
        help="Only check the given IDs.")
    parser.add_argument('id', nargs='*', type=int)



    args = parser.parse_args()

    if args.ids and not args.id or (not args.ids and args.id):
        parser.print_usage()
        exit(1)

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
        upload_changed_movies(since=(since_qty, since_unit), ids=args.id, transcode=not args.no_transcode)
    except:
        log.exception('Error while uploading changed movies in last {:d} {}'.format(since_qty, since_unit.lower()))


def upload_changed_movies(since=(1, 'MONTH'), ids=None, transcode=True):

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
            'image', # Thumbnail.
            'filmstrip_image',
            'sg_uploaded_movie',
            'sg_uploaded_movie_mp4',
            'sg_uploaded_movie_webm',
            'sg_uploaded_movie_image',
            'sg_uploaded_movie_frame_rate',
            'sg_uploaded_movie_transcoding_status',
        ], order=[
            {'field_name': 'created_at', 'direction': 'desc'},
        ],
    )

    num_uploaded = 0
    num_checked = 0

    for entity in entities:

        if ids and entity['id'] not in ids:
            continue

        num_checked += 1

        try:
            num_uploaded += int(bool(upload_movie(entity, transcode=transcode)))
        except:
            log.exception('Error while uploading {type} {id}'.format(**entity))

        if args.count and num_uploaded >= args.count:
            break

    log.info('Checked {:d} entities; uploaded {:d} movies'.format(num_checked, num_uploaded))


def upload_movie(entity, transcode=True, sg=None):

    log.info('Checking {type} {id} at: {sg_path_to_movie}'.format(**entity))

    # Something else uploaded the full movie.
    if entity['sg_uploaded_movie']:
        log.warning("Already has an uploaded movie; skipping")
        return

    # ... and it is still transcoding.
    if entity['sg_uploaded_movie_transcoding_status']:
        # It has been uploaded, but is awaiting transcode.
        # I'm not totally sure how this field works, but this is the safe
        # assumption to make.
        log.info("Already transcoding; skipping.")
        return

    path_to_movie = entity['sg_path_to_movie']
    if not path_to_movie:
        log.warning("Does not have a `sg_path_to_movie`; skipping.")
        return

    if not os.path.exists(path_to_movie):
        log.warning("`sg_path_to_movie` does not exist; skipping.")
        return

    name, ext = os.path.splitext(os.path.basename(path_to_movie))

    sg = sg or connect()

    # We used to not try so hard.
    if not transcode:
        log.info("Uploading full file (as requested).")
        sg.upload(entity['type'], entity['id'], path_to_movie, 'sg_uploaded_movie', name)
        return True

    # .. and we still don't for images or audio.
    if ext in IMAGE_EXTS or ext in AUDIO_EXTS
        log.info("Uploading full file (because it is not a movie).")
        sg.upload(entity['type'], entity['id'], path_to_movie, 'sg_uploaded_movie', name)
        return True

    if ext not in MOVIE_EXTS:
        log.warning("Not a movie file; skipping.")
        return

    # Start with thumbnails.
    if not entity['image']:
        log.info("Extracting thumbnail.")
        img = 
        with tempdir() as dir_:


    if entity['sg_uploaded_movie_mp4']:
        log.warning('{type} {id} already has an uploaded mp4; skipping'.format(**entity))
    else:
        log.info('Transcoding {sg_path_to_movie} to {type} {id} to mp4...'.format(**entity))
        with temp_transcode(path_to_movie, '.mp4') as trancoded:
            if trancoded:
                log.info('Uploading {0} to {type} {id} as mp4...'.format(trancoded, **entity))
                sg.upload(entity['type'], entity['id'], trancoded, 'sg_uploaded_movie_mp4', name)


    return True




if __name__ == '__main__':
    if not logging.getLogger().handlers:
        logging.basicConfig()
    main()
