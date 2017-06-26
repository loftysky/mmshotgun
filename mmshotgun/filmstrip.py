from __future__ import division
from PIL import Image, ImageDraw, ImageFont
import av
import os
import shutil
import subprocess

debug = False

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
    one_frame = container.decode(video=0).next()
    #use PIL to thumbails and it will retain aspect ratio 
    one_thumbnail = one_frame.to_image()
    if max_width and max_height:
        if max_width > 720: 
            max_width = 720
        if max_height > 480: 
            max_height = 480
        wanted_ratio = float(max_height/max_width)
        original_ratio = float(one_thumbnail.size[1]/one_thumbnail.size[0])
        if wanted_ratio != original_ratio:
            print "width and height selected not the same ratio as video. Adjusting accordingly."
            hsize = (max_height/float(one_thumbnail.size[1]))
            max_width = int((float(one_thumbnail.size[0])*float(hsize)))

    new_thumbnail = one_thumbnail.resize((max_width, max_height), Image.ANTIALIAS)
    if debug:
        new_thumbnail.save('%s-thumbnail.jpg' %filename)
    return new_thumbnail

make_thumbnail('youtube-test.mp4', max_width=620)

def make_filmstrip(path_to_movie, max_frames=100, verbose=True):
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
    video_stream = container.streams.get(video=0)
    max_frames = min(video_stream.frames, 100) # getting < 100 frames for the filmstrip 

    delta = container.duration/max_frames
    next_time = 0
    container.seek(0)
    filmstrip = []
    basewidth = 240


    for i in range(max_frames):
        frame = container.decode(video=0).next()
        if i == 0: 
                frame = container.decode(video=0).next()
                wpercent = (basewidth/float(frame.to_image().size[0]))
                hsize = int((float(frame.to_image().size[1])*float(wpercent)))
        resize_frame = frame.to_image().resize((basewidth, hsize))  
        filmstrip.append(resize_frame)
        container.seek(next_time)
        next_time = int(i * container.duration / max_frames)
        if verbose: 
            print 'seeking to', next_time, 'for', i, 'frames'
        

    x=0
    composite = Image.new('RGB', (len(filmstrip) * basewidth,hsize), Image.ANTIALIAS)
    for i, frame in enumerate(filmstrip):
        composite.paste(frame, (x, 0))
        x += frame.size[0]
    if debug:
        composite.save('filmstrip-%s.jpg' %filename, quality=90)
    return composite

def make_barcode(path_to_movie, width=854, height=480, verbose=True):
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
    for frame in container.decode(video=0):
        frame_height = frame.to_image().size[1]
        column = frame.to_image().resize((1, frame_height), Image.ANTIALIAS)
        columns.append(column)
        if verbose:
            print "resizing frame", frame.index

    x = 0
    barcode = Image.new('RGB', (len(columns), frame_height))
    for i, column in enumerate(columns):
        barcode.paste(column, (x, 0))
        if verbose: 
            print "pasting frame", i
        x += column.size[0]
    barcode = barcode.resize((width, height), Image.ANTIALIAS)
    if debug:
        barcode.save('barcode-%s.jpg' %filename, quality=90)
    return barcode

#make_barcode('DO WHAT YOU CANT.mkv')
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

    src, ext = os.path.splitext(os.path.basename(path_to_movie))
    tmp_mp4 = '%s.tmp-%s.mp4' % (src, os.urandom(2).encode('hex'))

    if force = True: 
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

    
