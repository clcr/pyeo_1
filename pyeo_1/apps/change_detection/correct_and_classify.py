"""
Downloads, preprocesses, applies terrain correction and classifies a set of images from Landsat or Sentinel-2
"""

from pyeo_1 import terrain_correction
from pyeo_1 import queries_and_downloads
from pyeo_1 import raster_manipulation
from pyeo_1 import filesystem_utilities
from pyeo_1 import classification

import argparse
import configparser

if __name__ == "__main__":
    pass