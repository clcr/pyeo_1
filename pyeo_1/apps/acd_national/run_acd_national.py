import configparser
import json
import os
from pyeo_1 import filesystem_utilities
from pyeo_1 import vectorisation
from pyeo_1 import acd_national
import sys

if __name__ == "__main__":

    # for arg in sys.argv[1:]:
    #     print(arg)

    # sys.argv[0] is the name of the python script, e.g. acd_national.py
    # we pass sys.argv[1:] to acd_national() because we need to pass config to acd_national (poorly explained)
    acd_national.automatic_change_detection_national(sys.argv[1:])