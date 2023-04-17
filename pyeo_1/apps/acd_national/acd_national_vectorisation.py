from pyeo_1 import filesystem_utilities as file_util
from pyeo_1 import vectorisation
from pyeo_1 import vectorisation.acd_national_vectorisation
import geopandas as gpd
import os

level_1_boundaries_path = "/data/clcr/shared/IMPRESS/matt/pyeo_1/geometry/gadm41_KEN_1.json"
epsg = 21097
root_dir = "/data/clcr/shared/IMPRESS/Ivan/kenya_national_prod"
conda_env_name = "pyeo_prod_0_8_0_env"
delete_existing = True

log_folder = "national_log_160423_newmulti_model"

log_folder_path = os.path.join(root_dir, log_folder)
if not os.path.exists(log_folder_path):
    os.mkdir(log_folder_path)

log = file_util.init_log(os.path.join(log_folder_path, "log.txt"))

acd_national_vectorisation_test(root_dir, log, epsg, level_1_boundaries_path, conda_env_name, delete_existing)

if __name__ : "main":


