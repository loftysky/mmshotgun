import os


def locate_schema(name):
    if name in ('mm-v1', 'markmedia', 'loftysky'):
        return os.path.abspath(os.path.join(__file__, '..', 'schema'))
