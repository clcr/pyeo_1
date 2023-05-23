import sys
from pyeo_1 import acd_national

if __name__ == "__main__":
    # if run from terminal, __name__ becomes "__main__"
    # sys.argv[0] is the name of the python script, e.g. acd_national.py
    
    # I.R. Chnage as a list was getting passed but a single config_path string expected by receiving functions
    acd_national.automatic_change_detection_national(sys.argv[1])