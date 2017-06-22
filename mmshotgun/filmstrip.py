from sgfs import SGFS
from av import time_base as AV_TIME_BASE
from PIL import Image, ImageDraw, ImageFont
import av
import os

sgfs = SGFS()

def make_thumbnail(path_to_movie, max_width=720, max_height=480, time=0.25):
    """Make a thumbnail from the given media file.
    
    :param str path_to_movie: The movie file to thumbnail.
    :param int max_width: The max width of the thumbnail.
    :param int max_height: The max height of the thumbnail.
    :param float time: Where to pick the frame from the movie in time, from 0 to 1.
    :returns PIL.Image: The thumbnail.

    """
    
    # See: http://pillow.readthedocs.io/en/4.0.x/reference/Image.html#PIL.Image.Image.thumbnail
    container = av.open(path_to_movie)
    filename, _ = os.path.basename(path_to_movie).split('.')
    video_stream = next(s for s in container.streams if s.type == 'video')
    target_duration = int(container.duration * time)
    #target_timestamp = int((target_frame * AV_TIME_BASE) / video_stream.rate)

    container.seek(target_duration)
    one_frame = container.decode(video=0).next()
    new_thumbnail = one_frame.reformat(width=max_width, height=max_height)
    new_thumbnail.to_image().save('new_thumbnail-%s.jpg' %filename)
    return new_thumbnail

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
    filename, _ = os.path.basename(path_to_movie).split('.')
    container = av.open (path_to_movie)
    video_stream = next(s for s in container.streams if s.type == 'video')
    max_frames = min(video_stream.frames, 100)

    delta = container.duration/max_frames
    next_time = 0
    container.seek(0)
    filmstrip = []
    basewidth = 240
    frame = container.decode(video=0).next()
    wpercent = (basewidth/float(frame.to_image().size[0]))
    hsize = int((float(frame.to_image().size[1])*float(wpercent)))


    for i in range(max_frames):
        frame = container.decode(video=0).next()
        resize_frame = frame.to_image().resize((basewidth, hsize))
        filmstrip.append(resize_frame)
        next_time += delta
        print next_time, container.duration, i
        container.seek(next_time)

    x=0
    composite = Image.new('RGB', (len(filmstrip) * basewidth,hsize), Image.ANTIALIAS)
    for i, frame in enumerate(filmstrip):
        composite.paste(frame, (x, 0))
        x += frame.size[0]
        # See: https://github.com/mikeboers/PyAV/blob/master/examples/merge-filmstrip.py#L31-L38
    composite.save('filmstrip-%s.jpg' %filename, quality=90)
    return composite

def make_barcode(path_to_movie, width=720, height=480):
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
    filename, _ = os.path.basename(path_to_movie).split('.')
    columns = []
    for frame in container.decode(video=0):
        height = frame.to_image().size[1]
        column = frame.to_image().resize((1, height), Image.ANTIALIAS)
        columns.append(column)
        print "resizing frame", frame.index

    x = 0
    composite = Image.new('RGB', (len(columns), height))
    for i, column in enumerate(columns):
        composite.paste(column, (x, 0))
        print "pasting frame", i
        x += column.size[0]
    composite = composite.resize((width, height), Image.ANTIALIAS)
    composite.save('barcode-%s.jpg' %filename, quality=90)
    return composite


def make_proxy(path_to_movie, path_to_mp4, path_to_webm, force=False):
    """Prepare proxies that are suitable for Shotgun.
    
    See: https://support.shotgunsoftware.com/hc/en-us/articles/219030418-Do-it-yourself-DIY-transcoding
    
    :param str path_to_movie: The input movie.
    :param str path_to_mp4: The output H.264.
    :param str path_to_webm: The output WebM.
    :param bool force: Create the outputs even if they already exist.
    
    """
    
    # Encode it to the side of the final requested path so that if
    # it fails we won't think it finished later.
    #tmp_mp4 = '%s.tmp-%s.mp4' % (path_to_mp4, os.urandom(2).encode('hex'))
    # use ffmpeg to create tmp_mp4
    #shtuil.move(tmp_mp4, path_to_mp4)
    pass
    
  
    
