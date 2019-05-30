from setuptools import setup

setup(
    name = 'ceph-backup',
    version = '1.0',
    packages = ['cephbackup',],
    description = 'ceph rbd backup!',
    entry_points={'console_scripts': ['cephbackup = cephbackup.__main__:main']},
    author = 'tony',
    author_email = '10779164@qq.com',
    url = 'jouline.com',
)
