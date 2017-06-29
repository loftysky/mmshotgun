from __future__ import division

import logging
import os
import shutil
import subprocess

from PIL import Image

import av


log = logging.getLogger(__name__)

_debug = False


def make_thumbnail(path_to_movie, max_width=720, max_height=480, time=0.25):
    """Make a thumbnail from the given media file.
    
    :param str path_to_movie: The movie file to thumbnail.
    :param int max_width: The max width of the thumbnail.
    :param int max_height: The max height of the thumbnail.
    :param float time: Where to pick the frame from the movie in time, from 0 to 1.
    :returns PIL.Image: The thumbnail.

    """
    
    container = av.open(path_to_movie)
    filename = os.path.splitext(os.path.basename(path_to_movie))[0]
    video_stream = container.streams.get(video=0) #gets video stream at index 0
    target_duration = int(container.duration * time) #casting to int b/c PYAV assumes that floats are seconds of time.

    container.seek(target_duration)
    frame = container.decode(video=0).next()

    if not frame:
        raise ValueError("Could not seek to {}.".format(time))

    image = frame.to_image()
    image.thumbnail((max_width, max_height))
    return image



def make_filmstrip(path_to_movie, max_frames=100):
    """Make a Shotgun filmstrip from the given media file.
    
    A filmstrip is a horizontal strip of frames that are played when the user
    hovers over them. Each image is 240px wide, so the final strip must be a
    multiple of 240px wide.
    
    See: http://developer.shotgunsoftware.com/python-api/reference.html#shotgun_api3.shotgun.Shotgun.upload_filmstrip_thumbnail
    
    :param str path_to_movie: The movie file to filmstrip.
    :param int max_frames: The maximum number of frames to take from the movie.
    :returns PIL.Image: The filmstrip.
    
    
    """
    filename = os.path.splitext(os.path.basename(path_to_movie))[0]
    container = av.open (path_to_movie)
    stream = container.streams.video[0]
    max_frames = min(stream.frames, 100) # getting < 100 frames for the filmstrip 
    max_diff = container.duration / max_frames / 4

    filmstrip = []
    basewidth = 240

    last_pts = None
    tiny_file = False
    seek_skip_count = 0

    for i in range(max_frames):

        next_cts = int(i * container.duration / (max_frames - 1))
        
        if not tiny_file:
            container.seek(next_cts)

        next_pts = int(float(next_cts - max_diff) / av.time_base / stream.time_base)

        for j, frame in enumerate(container.decode(video=0)):

            if not j and not tiny_file and last_pts and last_pts >= frame.pts:
                tiny_file = True
                log.info("File is small enough that we won't bother seeking.")

            if frame.pts >= next_pts:
                break
            
        else:
            log.info("Scanned rest of file after seek.")

        seek_skip_count += j
        last_pts = frame.pts

        if not i:
            frame = container.decode(video=0).next()
            wpercent = basewidth / float(frame.to_image().size[0])
            hsize = int(float(frame.to_image().size[1]) * float(wpercent))

        resize_frame = frame.to_image().resize((basewidth, hsize))  
        filmstrip.append(resize_frame)
    
    log.info("Skipped average of %.1f frames after seek.", float(seek_skip_count) / len(filmstrip))

    x = 0
    composite = Image.new('RGB', (len(filmstrip) * basewidth,hsize), Image.ANTIALIAS)
    for i, frame in enumerate(filmstrip):
        composite.paste(frame, (x, 0))
        x += frame.size[0]

    if _debug:
        composite.save('filmstrip-%s.jpg' %filename, quality=90)

    return composite


def make_barcode(path_to_movie, width=1280, height=480):
    """Make a video barcode from the given media file.
    
    A video barcode is effectively every frame of the video laid end to end,
    and then squashed horizontally. It gives an overview of the change of values
    in the video over time.
    
    See: http://moviebarcode.tumblr.com/
    
    Warning: This is rather naive, and will take ~5MB of RAM per minute of footage.
    
    :param str path_to_movie: The movie file to barcode.
    :param int width: The width of the barcode.
    :param int height: The height of the barcode.
    :returns PIL.Image: The barcode.
    
    """

    container = av.open(path_to_movie) 
    filename = os.path.splitext(os.path.basename(path_to_movie))[0]
    
    columns = []
    for i, frame in enumerate(container.decode(video=0)):
        frame_height = frame.to_image().size[1]
        column = frame.to_image().resize((1, frame_height), Image.ANTIALIAS)
        columns.append(column)
        if i and not i % 100:
            log.debug("Resizing frame %s for barcode.", i)

    x = 0
    barcode = Image.new('RGB', (len(columns), frame_height))
    for i, column in enumerate(columns):
        barcode.paste(column, (x, 0))
        if i and not i % 100:
            log.debug("Pasting frame %s for barcode.", i)
        x += column.size[0]
    barcode = barcode.resize((width, height), Image.ANTIALIAS)
    if _debug:
        barcode.save('barcode-%s.jpg' %filename, quality=90)
    return barcode


def make_proxy(path_to_movie, path_to_mp4, force=False):
    """Prepare proxies that are suitable for Shotgun.
    
    See: https://support.shotgunsoftware.com/hc/en-us/articles/219030418-Do-it-yourself-DIY-transcoding
    
    :param str path_to_movie: The input movie.
    :param str path_to_mp4: The output H.264.
    :param bool force: Create the outputs even if they already exist.
    
    """
    
    # Encode it to the side of the final requested path so that if
    # it fails we won't think it finished later.
    #tmp_mp4 = '%s.tmp-%s.mp4' % (path_to_mp4, os.urandom(2).encode('hex'))
    # use ffmpeg to create tmp_mp4
    #shtuil.move(tmp_mp4, path_to_mp4)


    if not (force or not os.path.exists(path_to_mp4)):
        return

    src_name, ext = os.path.splitext(os.path.basename(path_to_movie))
    tmp_mp4 = '%s.tmp-%s.mp4' % (src, os.urandom(2).encode('hex'))
    subprocess.check_call([
        'ffmpeg',
        '-i', src+ext,
        '-strict', 'experimental',
        '-acodec', 'aac',
            '-ab', '160k', '-ac', '2',
        '-vcodec', 'libx264',
            '-pix_fmt', 'yuv420p', '-vf', 'scale=trunc((a*oh)/2)*2:720',
            '-g', '30', '-b:v', '2000k', '-vprofile', 'high', '-bf', '0',
        '-f', 'mp4',
        tmp_mp4
    ])
    shutil.move(tmp_mp4, path_to_mp4)


