import sys
from pyeo_1 import acd_national

if __name__ == "__main__":

    # sys.argv[0] is the name of the python script, e.g. acd_national.py
    # we pass sys.argv[1:] to acd_national() because we need to pass
    # config to acd_national (poorly explained)
    acd_national.automatic_change_detection_national(sys.argv[1:])
