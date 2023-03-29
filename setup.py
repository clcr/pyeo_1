from distutils.core import setup
import os
import stat

setup(
    name='pyeo_1',
    version='0.1.0',
    author='University of Leicester',
    author_email='hb71@le.ac.uk',
    packages=['pyeo_1', 'pyeo_1.tests'],
    url='http://pypi.python.org/pypi/Pyeo/',
    license='LICENSE',
    description='Modular processing chain from download to ard',
    install_requires=[
        "boto3",
        "botocore",
        "gdal",
        "joblib",
        "matplotlib",
        "pip",
        "pytest",
        "requests",
        "setuptools",
        "numpy",
        "scikit-learn",
        "scipy",
        "geojson",
        "sentinelhub",
        "sentinelsat",
        "tenacity",
        "tqdm", 'pysolar'
    ],
)
