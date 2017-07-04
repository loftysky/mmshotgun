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
import time

from shotgun_api3_registry import connect

from . import filmstrip as process


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


def transcode(src, dir_, ext):

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

    return dst


def transcode_mp4(src, dir_):
    return transcode(src, dir_, '.mp4')


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--pid-file')
    parser.add_argument('-p', '--include-project', type=int, action='append')
    parser.add_argument('-P', '--exclude-project', type=int, action='append')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-t', '--soft-timeout', type=float, default=30)
    parser.add_argument('-T', '--hard-timeout', type=float, default=60 * 60)
    parser.add_argument('--no-mp4', action='store_true')
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

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

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
        upload_changed_movies(since=(since_qty, since_unit), ids=args.id, include_project=args.include_project, exclude_project=args.exclude_project, mp4=not args.no_mp4, transcode=not args.no_transcode, count=args.count, max_time=args.hard_timeout / 2)
    except:
        log.exception('Error while uploading changed movies in last {:d} {}'.format(since_qty, since_unit.lower()))


def upload_changed_movies(since=(1, 'MONTH'), ids=None, include_project=None, exclude_project=None, mp4=True, transcode=True, count=0, max_time=0):
    
    start_time = time.time()

    sg = connect()

    filters = [
        ('created_at', 'in_last', since[0], since[1]),
        ('sg_path_to_movie', 'is_not', None),
    ]

    if include_project:
        filters.append(('project', 'in', [{'type': 'Project', 'id': i} for i in include_project]))
    if exclude_project:
        filters.append(('project', 'not_in', [{'type': 'Project', 'id': i} for i in exclude_project]))

    log.info('Retrieving versions in last {:d} {}...'.format(since[0], since[1].lower()))
    entities = sg.find("Version",
        filters=filters,
        fields=[
            'created_at',
            'sg_path_to_movie',
            'sg_task',
            'image', # Thumbnail.
            'filmstrip_image',
            'sg_barcode_file',
            'sg_barcode_entity',
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
            num_uploaded += int(bool(upload_movie(entity, mp4=mp4, transcode=transcode)))
        except:
            log.exception('Error while uploading {type} {id}'.format(**entity))

        if count and num_uploaded >= count:
            break
        if max_time and (time.time() - start_time) > max_time:
            log.info("Passed half of hard-timeout; stopping.")
            break

    log.info('Checked {:d} entities; uploaded {:d} movies'.format(num_checked, num_uploaded))


def upload_movie(entity, mp4=True, transcode=True, sg=None):

    log.info('Checking {type} {id} at: {sg_path_to_movie}'.format(**entity))
    did_something = False

    # Something else uploaded the full movie.
    if entity['sg_uploaded_movie']:
        log.debug("Already has an uploaded movie; skipping")
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
        log.debug("Does not have a `sg_path_to_movie`; skipping.")
        return

    if not os.path.exists(path_to_movie):
        log.info("`sg_path_to_movie` does not exist; skipping.")
        return

    name, ext = os.path.splitext(os.path.basename(path_to_movie))
    ext_lower = ext.lower()
    
    sg = sg or connect()

    # We used to not try so hard.
    if not transcode:
        log.info("Uploading full file (as requested).")
        sg.upload(entity['type'], entity['id'], path_to_movie, 'sg_uploaded_movie', name)
        return True

    # .. and we still don't for images or audio.
    if ext_lower in IMAGE_EXTS or ext_lower in AUDIO_EXTS:
        log.info("Uploading full file (because it is not a movie).")
        sg.upload(entity['type'], entity['id'], path_to_movie, 'sg_uploaded_movie', name)
        return True

    if ext_lower not in MOVIE_EXTS:
        log.warning("Not a movie file; skipping.")
        return

    # Start with thumbnails.
    if not entity['image']:
        log.info("Extracting thumbnail.")
        img = process.make_thumbnail(path_to_movie)
        with tempdir() as dir_:
            path = os.path.join(dir_, name + '.jpeg')
            img.save(path)
            log.info("Uploading thumbnail.")
            sg.upload(entity['type'], entity['id'], path, 'thumb_image')
        did_something = True

    # Filmstrips.
    if not entity['filmstrip_image']:
        log.info("Extracting filmstrip.")
        img = process.make_filmstrip(path_to_movie)
        with tempdir() as dir_:
            path = os.path.join(dir_, name + '.jpeg')
            img.save(path)
            log.info("Uploading filmstrip.")
            sg.upload(entity['type'], entity['id'], path, 'filmstrip_thumb_image')
        did_something = True

    # Barcodes.
    if not entity['sg_barcode_file']:
        log.info("Extracting barcode.")
        img = process.make_barcode(path_to_movie)
        with tempdir() as dir_:
            path = os.path.join(dir_, name + '.jpeg')
            img.save(path)
            log.info("Uploading barcode.")
            id_ = sg.upload(entity['type'], entity['id'], path, 'sg_barcode_file')
            entity['sg_barcode_file'] = {'type': 'Attachment', 'id': id_}
            entity['sg_barcode_entity'] = None # Force it.
        did_something = True

    if not entity['sg_barcode_entity']:
        log.info("Linking barcode.")
        sg.update(entity['type'], entity['id'], {'sg_barcode_entity': entity['sg_barcode_file']})
        did_something = True

    if mp4 and not entity['sg_uploaded_movie_mp4']:
        log.info("Transcoding mp4.")
        with tempdir() as dir_:
            transcoded = transcode_mp4(path_to_movie, dir_)
            log.info("Uploading mp4.")
            sg.upload(entity['type'], entity['id'], transcoded, 'sg_uploaded_movie_mp4')
        did_something = True

    return did_something




if __name__ == '__main__':
    main()
