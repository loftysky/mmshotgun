from subprocess import check_call

from setuptools import setup, find_packages
from distutils.command.build import build as default_build




setup(

    name='mmshotgun',
    version='0.1.0b1',
    description='A collection of Keystone-specific Shotgun tools.',
    url='http://github.com/westernx/mmshotgun',

    packages=find_packages(exclude=['build*', 'tests*']),
    include_package_data=True,

    entry_points={
        'console_scripts': '''
            mmshotgun-uploader = mmshotgun.uploader:main
        ''',
        'sgfs_schema_locators': '''
            mmshotgun = mmshotgun.sgfs:locate_schema
        ''',
    },

    author='Mike Boers',
    author_email='mmshotgun@mikeboers.com',
    license='BSD-3',

)
