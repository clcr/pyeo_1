import configparser
import json
import os
import shutil
import glob
from pathlib import Path
from pyeo_1 import filesystem_utilities
from pyeo_1 import vectorisation
from pyeo_1 import queries_and_downloads
from pyeo_1 import raster_manipulation
from pyeo_1 import classification
import geopandas as gpd
import pandas as pd
import numpy as np

# acd_national is the top-level function which controls the raster and vector processes for pyeo_1
def automatic_change_detection_national(path_to_config):

    """
    This function:
        - acts as the singular call to run automatic change detection per tile, then aggregate to national, then distribute the change alerts.

    Parameters
    ----------
    path_to_config : str
        The path to the config file, which is an .ini

    Returns
    ----------
    None

    """

    # starting acd_initialisation()
    config_dict, acd_log = acd_initialisation(path_to_config)

    acd_log.info("---------------------------------------------------------------")
    acd_log.info("Starting acd_to_config_to_log()")
    acd_log.info("---------------------------------------------------------------")

    # echo configuration to log
    acd_config_to_log(config_dict, acd_log)

    acd_log.info("---------------------------------------------------------------")
    acd_log.info("Starting acd_roi_tile_intersection()")
    acd_log.info("---------------------------------------------------------------")
    tilelist_filepath = acd_roi_tile_intersection(config_dict, acd_log)

    # report errors if any ROI files are missing, i.e. "I cannot find X ROI"

    acd_log.info("---------------------------------------------------------------")
    acd_log.info("Starting acd_integrated_raster():")
    acd_log.info("---------------------------------------------------------------")
    acd_integrated_raster(config_dict, acd_log, tilelist_filepath)

    acd_log.info("---------------------------------------------------------------")
    acd_log.info("Starting acd_by_tile_vectorisation(), vectorising each change report raster, by tile")
    acd_log.info("---------------------------------------------------------------")

    # potential todo: integrated_vectorisation to accomodate the tilelist_df
    # and skip already existing vectors

    acd_integrated_vectorisation(root_dir=config_dict["tile_dir"],
                              log=acd_log,
                              epsg=config_dict["epsg"],
                              level_1_boundaries_path=config_dict["level_1_boundaries_path"],
                              conda_env_name=config_dict["conda_env_name"],
                              delete_existing=config_dict["do_delete_existing_vector"],
                              tilelist_filepath=tilelist_filepath)
    
    acd_log.info("---------------------------------------------------------------")
    acd_log.info("Starting acd_national_integration")
    acd_log.info("---------------------------------------------------------------")
    
    acd_national_integration(root_dir=config_dict["tile_dir"],
                             log=acd_log,
                             epsg=config_dict["epsg"],
                             conda_env_name=config_dict["conda_env_name"],
                             config_dict=config_dict)
    # Heiko - KFS want aggregated statistics at county scale, instead of over one big shapefile
    # Heiko - integration at the ROI scale

    acd_log.info("---------------------------------------------------------------")
    acd_log.info("Starting acd_national_filtering")
    acd_log.info("---------------------------------------------------------------")
    # acd_national_filtering()
    # Ivan - ROI clipping?
    # Ivan - multiple filters e.g. time,
    
    acd_log.info("---------------------------------------------------------------")
    acd_log.info("Starting acd_national_dataframe_to_shapefile()")
    acd_log.info("---------------------------------------------------------------")
    # acd_national_dataframe_to_shapefile()
    
    acd_log.info("---------------------------------------------------------------")
    acd_log.info("Starting acd_national_manual_validation()")
    acd_log.info("---------------------------------------------------------------")
    # acd_national_manual_validation()
    # add in fields:
    # user
    # judgement
    # 
    
    acd_log.info("---------------------------------------------------------------")
    acd_log.info("Starting acd_national_distribution()")
    acd_log.info("---------------------------------------------------------------")
    # acd_national_distribution()
    # messaging services to Park Rangers (e.g. WhatsApp, Maps2Me)
    
    acd_log.info("---------------------------------------------------------------")
    acd_log.info("---                  INTEGRATED PROCESSING END              ---")
    acd_log.info("---------------------------------------------------------------")

    # this is the end of the function

def acd_composite_update():
    """
    This function updates the composite to a new specified start and end date.
    
    We could potentially streamline the composite update process by:

        - Move out of date change images to the composite folder

            - "Out of Date" = time period parameter e.g. every 3 months

        - rebuild composite based on whichever .tiffs are within the composite folder.

    """
    

############################
# the functions below are those required by acd_national()
############################

def acd_initialisation(path_to_config):

    """
    
    This function initialises the log.txt, making the log object available

    Parameters
    ----------
    path_to_config : str
        The path to the config file, which is an .ini

    Returns
    ----------
    dict : config
        A config dictionary
    log
        A log object
    
    """

    # read in config file
    config = configparser.ConfigParser(allow_no_value=True)
    config.read(path_to_config)

    config_dict = {}

    # changes directory to pyeo_dir, enabling the use of relative paths from the config file
    os.chdir(config["environment"]["pyeo_dir"])

    # initialise log file
    log = filesystem_utilities.init_log_acd(log_path=os.path.join(config["environment"]["log_dir"], config["environment"]["log_filename"]),
                                            logger_name="pyeo_1_acd_log")


    log.info("---------------------------------------------------------------")
    log.info("---                  INTEGRATED PROCESSING START            ---")
    log.info("---------------------------------------------------------------")

    log.info("Reading in parameters defined in the Config")
    log.info("---------------------------------------------------------------")

    config_dict["do_dev"] = config.getboolean("raster_processing_parameters","do_dev")
    config_dict["do_all"] = config.getboolean("raster_processing_parameters","do_all")
    config_dict["do_classify"] = config.getboolean("raster_processing_parameters","do_classify")
    config_dict["do_change"] = config.getboolean("raster_processing_parameters","do_change")
    config_dict["do_download"] = config.getboolean("raster_processing_parameters","do_download")
    config_dict["do_update"] = config.getboolean("raster_processing_parameters","do_update")
    config_dict["do_quicklooks"] = config.getboolean("raster_processing_parameters","do_quicklooks")
    config_dict["do_delete"]= config.getboolean("raster_processing_parameters","do_delete")
    config_dict["do_zip"] = config.getboolean("raster_processing_parameters","do_zip")
    config_dict["build_composite"] = config.getboolean("raster_processing_parameters","do_build_composite")
    config_dict["build_prob_image"] = config.getboolean("raster_processing_parameters","do_build_prob_image")
    config_dict["do_skip_existing"] = config.getboolean("raster_processing_parameters","do_skip_existing")

    config_dict["start_date"] = config["forest_sentinel"]["start_date"]
    config_dict["end_date"] = config["forest_sentinel"]["end_date"]
    config_dict["composite_start"] = config["forest_sentinel"]["composite_start"]
    config_dict["composite_end"] = config["forest_sentinel"]["composite_end"]
    config_dict["epsg"] = int(config["forest_sentinel"]["epsg"])
    config_dict["cloud_cover"] = int(config["forest_sentinel"]["cloud_cover"])
    config_dict["cloud_certainty_threshold"] = int(config["forest_sentinel"]["cloud_certainty_threshold"])
    config_dict["model_path"] = config["forest_sentinel"]["model"]
    config_dict["download_source"] = config["raster_processing_parameters"]["download_source"]

    config_dict["bands"] = json.loads(config["raster_processing_parameters"]["band_names"])

    config_dict["resolution_string"] = config["raster_processing_parameters"]["resolution_string"]
    config_dict["output_resolution"] = int(config["raster_processing_parameters"]["output_resolution"])
    config_dict["buffer_size_cloud_masking"] = int(config["raster_processing_parameters"]["buffer_size_cloud_masking"])
    config_dict["buffer_size_cloud_masking_composite"] = int(config["raster_processing_parameters"]["buffer_size_cloud_masking_composite"])
    config_dict["download_limit"] = int(config["raster_processing_parameters"]["download_limit"])
    config_dict["faulty_granule_threshold"] = int(config["raster_processing_parameters"]["faulty_granule_threshold"])
    config_dict["sieve"] = int(config["raster_processing_parameters"]["sieve"])
    config_dict["chunks"] = int(config["raster_processing_parameters"]["chunks"])
    config_dict["class_labels"] = json.loads(config["raster_processing_parameters"]["class_labels"])
    config_dict["from_classes"] = json.loads(config["raster_processing_parameters"]["change_from_classes"])
    config_dict["to_classes"] = json.loads(config["raster_processing_parameters"]["change_to_classes"])

    config_dict["conda_env_name"] = config["environment"]["conda_env_name"]
    config_dict["pyeo_dir"] = config["environment"]["pyeo_dir"]
    config_dict["tile_dir"] = config["environment"]["tile_dir"]
    config_dict["integrated_dir"] = config["environment"]["integrated_dir"]
    config_dict["roi_dir"] = config["environment"]["roi_dir"]
    config_dict["roi_filename"] = config["environment"]["roi_filename"]
    config_dict["geometry_dir"] = config["environment"]["geometry_dir"]
    config_dict["sen2cor_path"] = config["environment"]["sen2cor_path"]


    config_dict["level_1_filename"] = config["vector_processing_parameters"]["level_1_filename"]
    config_dict["level_1_boundaries_path"] = os.path.join(config_dict["geometry_dir"], config_dict["level_1_filename"])
    config_dict["do_delete_existing_vector"] = config.getboolean("vector_processing_parameters","do_delete_existing_vector")
    config_dict["credentials_path"] = config["environment"]["credentials_path"]

    return config_dict, log

def acd_config_to_log(config_dict: dict, log):

    """
    This function echoes the contents of config_dict to the log file.
    It does not return anything.

    Parameters
    ----------

    config_dict : dict
        config_dict variable
    log :
        log variable

    Returns
    ----------
    None
    """

    log.info("Options:")
    if config_dict["do_dev"]:
        log.info("  --dev Running in development mode, choosing development versions of functions where available")
    else:
        log.info("  Running in production mode, avoiding any development versions of functions.")
    if config_dict["do_all"]:
        log.info("  --do_all")
    if config_dict["build_composite"]:
        log.info("  --build_composite for baseline composite")
        log.info("  --download_source = {}".format(config_dict["download_source"]))
        log.info(f"         composite start date  : {config_dict['start_date']}")
        log.info(f"         composite end date  : {config_dict['end_date']}")
    if config_dict["do_download"]:
        log.info("  --download for change detection images")
        if not config_dict["build_composite"]:
            log.info("  --download_source = {}".format(config_dict["download_source"]))
    if config_dict["do_classify"]:
        log.info("  --classify to apply the random forest model and create classification layers")
    if config_dict["build_prob_image"]:
        log.info("  --build_prob_image to save classification probability layers")
    if config_dict["do_change"]:
        log.info("  --change to produce change detection layers and report images")
        log.info(f"         change start date  : {config_dict['composite_start']}")
        log.info(f"         change end date  : {config_dict['composite_end']}")
    if config_dict["do_update"]:
        log.info("  --update to update the baseline composite with new observations")
    if config_dict["do_quicklooks"]:
        log.info("  --quicklooks to create image quicklooks")
    if config_dict["do_delete"]:
        log.info("  --remove downloaded L1C images and intermediate image products")
        log.info("           (cloud-masked band-stacked rasters, class images, change layers) after use.")
        log.info("           Deletes remaining temporary directories starting with \'tmp\' from interrupted processing runs.")
        log.info("           Keeps only L2A images, composites and report files.")
        log.info("           Overrides --zip for the above files. WARNING! FILE LOSS!")
    if config_dict["do_zip"]:
        log.info("  --zip archives L2A images, and if --remove is not selected also L1C,")
        log.info("           cloud-masked band-stacked rasters, class images and change layers after use.")
    if config_dict["do_delete_existing_vector"]:
        log.info("  --do_delete_existing_vector , when vectorising the change report rasters, ")
        log.info("            existing vectors files will be deleted and new vector files created.")

    # reporting more parameters
    log.info(f"EPSG used is: {config_dict['epsg']}")
    log.info(f"List of image bands: {config_dict['bands']}")
    log.info(f"Model used: {config_dict['model_path']}")
    log.info("List of class labels:")
    for c, this_class in enumerate(config_dict["class_labels"]):
        log.info("  {} : {}".format(c+1, this_class))
    log.info(f"Detecting changes from any of the classes: {config_dict['from_classes']}")
    log.info(f"                    to any of the classes: {config_dict['to_classes']}")
    log.info("--"*30)
    log.info("Reporting Directories and Filepaths")
    log.info(f"Tile Directory is   : {config_dict['tile_dir']}")
    log.info(f"Working Directory is   : {config_dict['pyeo_dir']}")
    log.info("The following directories are relative to the working directory, i.e. stored underneath:")
    log.info(f"Integrated Directory is   : {config_dict['integrated_dir']}")
    log.info(f"ROI Directory is   : {config_dict['roi_dir']}")
    log.info(f"Geometry Directory is   : {config_dict['geometry_dir']}")
    log.info(f"Path to the Administrative Boundaries used in the Change Report Vectorisation   : {config_dict['level_1_boundaries_path']}")
    log.info(f"Path to Sen2Cor is   : {config_dict['sen2cor_path']}")
    log.info(f"The Conda Environment which was provided in .ini file is :  {config_dict['conda_env_name']}")

    # end of function


def acd_roi_tile_intersection(config_dict, log):
    """
    
    This function:
        - accepts a Region of Interest (RoI) (specified by config_dict) and writes a tilelist.txt of the Sentinel-2 tiles that the RoI covers.
    
        - the tilelist.txt is then used to perform the tile-based processes, raster and vector.

    Parameters
    ----------
    config_dict : (dict)
        Dictionary of the Configuration Parameters specified in pyeo_1.ini
    log
        Log variable

    Returns
    ----------
    tilelist_filepath (str)
        Filepath of a .txt containing the list of tiles on which to perform raster processes
        
    """

    # specify geopandas gdal and proj installation
    conda_env_name = config_dict["conda_env_name"]
    home = str(Path.home())
    os.environ["GDAL_DATA"] = f"{home}/miniconda3/envs/{conda_env_name}/lib/python3.10/site-packages/pyproj/proj_dir/share/gdal"
    os.environ["PROJ_LIB"] = f"{home}/miniconda3/envs/{conda_env_name}/lib/python3.10/site-packages/pyproj/proj_dir/share/proj"

    ####### read in roi
    # roi_filepath is relative to pyeo_dir supplied in pyeo_1.ini
    roi_filepath = os.path.join(config_dict["roi_dir"], config_dict["roi_filename"])
    roi = gpd.read_file(roi_filepath)

    # check if s2_tiles exists (it should, as is provided with git clone pyeo)
    s2_tiles_filepath = os.path.join(config_dict["geometry_dir"], "kenya_s2_tiles.shp")
    s2_tile_geometry = gpd.read_file(s2_tiles_filepath)

    # change projection
    roi = roi.to_crs(s2_tile_geometry.crs)

    # intersect roi with s2 tiles to return 
    intersection = s2_tile_geometry.sjoin(roi)
    tiles_list = list(intersection["Name"].unique())
    tiles_list.sort()
    log.info(f"The provided ROI intersects with {len(tiles_list)} Sentinel-2 tiles")
    log.info("These tiles are  :")
    for n, this_tile in enumerate(tiles_list):
        log.info("  {} : {}".format(n+1, this_tile))

    tilelist_filepath = os.path.join(config_dict["roi_dir"],"tilelist.csv")
    log.info(f"Writing Sentinel-2 tiles that intersect with the provided ROI to  : {tilelist_filepath}")
    
    try:
        tiles_list_df = pd.DataFrame({"tile" : tiles_list})
        tiles_list_df.to_csv(tilelist_filepath, header=True,index=False)
    except:
        log.error(f"Could not write to {tilelist_filepath}")
    log.info("Finished ROI tile intersection")

    # "reset" gdal and proj installation back to default (which is GDAL's GDAL and PROJ_LIB installation)
    home = str(Path.home())
    os.environ["GDAL_DATA"] = f"{home}/miniconda3/envs/{conda_env_name}/share/gdal"
    os.environ["PROJ_LIB"] = f"{home}/miniconda3/envs/{conda_env_name}/share/proj"

    return tilelist_filepath


def acd_integrated_raster(config_dict: dict, log, tilelist_filepath: str):

    """
    
    This function:

        - checks whether tilelist.csv exists before running acd_by_tile_raster for each tile

        - sequentially calls acd_by_tile_raster for all active tiles and waits for completion/failure (parallelism?)

    Parameters
    ----------
    config_dict : dict
        Dictionary of the Configuration Parameters specified in pyeo_1.ini
    log:
        Logger object
    tilelist_filepath : str
        Filepath of a .csv containing the list of tiles on which to perform raster processes
    
    Returns
    ----------
    None
    """

    ####### reads in tilelist.txt, then runs acd_per_tile_raster, per tile

    # check if tilelist_filepath exists
    if os.path.exists(tilelist_filepath):
        try:
            tilelist_df = pd.read_csv(tilelist_filepath)
        except:
            log.error(f"Could not open {tilelist_filepath}")
    else:
        log.error(f"{tilelist_filepath} does not exist, check that you ran the acd_roi_tile_intersection beforehand")
    
    # check and read in credentials for downloading Sentinel-2 data
    credentials_path = config_dict["credentials_path"]
    if os.path.exists(credentials_path):
        try:
            conf = configparser.ConfigParser(allow_no_value=True)
            conf.read(credentials_path)
            credentials_dict = {}
            credentials_dict["sent_2"] = {}
            credentials_dict["sent_2"]["user"] = conf['sent_2']['user']
            credentials_dict["sent_2"]["pass"] = conf['sent_2']['pass']
        except:
            log.error(f"Could not open {credentials_path}")
    else:
        log.error(f"{credentials_path} does not exist, did you write the correct filepath in pyeo_1.ini?")

    # check for and create tile directory, which will hold the tiles
    tile_directory = config_dict["tile_dir"]
    if not os.path.exists(tile_directory):
        log.info(f"The following directory for tile_dir did not exist, creating it at: {tile_directory}")
        os.makedirs(tile_directory)
    else:
        pass

    ######## run acd_by_tile_raster
    for index, tile in tilelist_df.iterrows():
        try:
            log.info(f"Starting ACD Raster Processes for Tile :  {tile[0]}")
            acd_by_tile_raster(config_dict=config_dict,
                            log=log,
                            tile_directory_path=tile_directory,
                            tile_to_process=tile[0],
                            credentials_dict=credentials_dict)
            log.info(f"Finished ACD Raster Processes for Tile :  {tile[0]}")
        except:
            log.error(f"Could not complete ACD Raster Processes for Tile: {tile[0]}")


def acd_by_tile_raster(config_dict: dict, log, tile_directory_path: str, tile_to_process: str, credentials_dict: dict):
    """
  
    This function:

        - Downloads Images for the Composite

        - Downloads Change Images

        - 
    
    Parameters
    ----------
    config_dict : dict
    log:
    tile_dictionary_path : str
    tile_to_process : str
    credentials_dict : dict

    Returns
    ----------
    None
    """

    # wrap the whole thing in the classic try block
    try:
        # check for and create the folder structure pyeo expects
        individual_tile_directory_path = os.path.join(tile_directory_path, tile_to_process)
        if not os.path.exists(individual_tile_directory_path):
            log.info(f"individual tile directory path  : {individual_tile_directory_path}")
            filesystem_utilities.create_folder_structure_for_tiles(individual_tile_directory_path)
        else:
            log.info(f"This individual tile directory already exists  : {individual_tile_directory_path}")

        # create per tile log file
        tile_log = filesystem_utilities.init_log_acd(log_path=os.path.join(individual_tile_directory_path, "log", tile_to_process+"_log.txt"),
                                                    logger_name=f"pyeo_1_tile_{tile_to_process}_log")

        # print config parameters to the tile log
        acd_config_to_log(config_dict=config_dict, log=tile_log)

        # create per tile directory variables
        tile_log.info("Creating the directory paths")
        tile_root_dir = individual_tile_directory_path
        
        change_image_dir = os.path.join(tile_root_dir, r"images")
        l1_image_dir = os.path.join(tile_root_dir, r"images/L1C")
        l2_image_dir = os.path.join(tile_root_dir, r"images/L2A")
        l2_masked_image_dir = os.path.join(tile_root_dir, r"images/cloud_masked")
        categorised_image_dir = os.path.join(tile_root_dir, r"output/classified")
        probability_image_dir = os.path.join(tile_root_dir, r"output/probabilities")
        sieved_image_dir = os.path.join(tile_root_dir, r"output/sieved")
        composite_dir = os.path.join(tile_root_dir, r"composite")
        composite_l1_image_dir = os.path.join(tile_root_dir, r"composite/L1C")
        composite_l2_image_dir = os.path.join(tile_root_dir, r"composite/L2A")
        composite_l2_masked_image_dir = os.path.join(tile_root_dir, r"composite/cloud_masked")
        quicklook_dir = os.path.join(tile_root_dir, r"output/quicklooks")

        start_date = config_dict["start_date"]
        end_date = config_dict["end_date"]
        composite_start_date = config_dict["composite_start"]
        composite_end_date = config_dict["composite_end"]
        cloud_cover = config_dict["cloud_cover"]
        cloud_certainty_threshold = config_dict["cloud_certainty_threshold"]
        model_path = config_dict['model_path']
        sen2cor_path = config_dict['sen2cor_path']
        epsg = config_dict["epsg"]
        bands = config_dict["bands"]
        resolution = config_dict['resolution_string']
        out_resolution = config_dict['output_resolution']
        buffer_size = config_dict['buffer_size_cloud_masking']
        buffer_size_composite = config_dict['buffer_size_cloud_masking_composite']
        max_image_number = config_dict['download_limit']
        faulty_granule_threshold = config_dict["faulty_granule_threshold"]
        download_limit = config_dict["download_limit"]

        skip_existing = config_dict["do_skip_existing"]
        # download_source = config_dict["download_source"]
        # monkey patch b/c config_dict version gets rejected yet is a string that is "scihub"
        download_source = "scihub"
        sen_user = credentials_dict["sent_2"]["user"]
        sen_pass = credentials_dict["sent_2"]["pass"]

        # ------------------------------------------------------------------------
        # Step 1: Create an initial cloud-free median composite from Sentinel-2 as a baseline map
        # ------------------------------------------------------------------------

        if config_dict["build_composite"] or config_dict["do_all"]:
            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Creating an initial cloud-free median composite from Sentinel-2 as a baseline map")
            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Searching for images for initial composite.")
            try:
                composite_products_all = queries_and_downloads.check_for_s2_data_by_date(tile_root_dir,
                                                                                        composite_start_date,
                                                                                        composite_end_date,
                                                                                        credentials_dict,
                                                                                        cloud_cover=cloud_cover,
                                                                                        tile_id=tile_to_process,
                                                                                        producttype=None #"S2MSI2A" or "S2MSI1C"
                                                                                        )
            except Exception as error:
                tile_log.error(f"check_for_s2_data_by_date failed, got this error :  {error}")
                
            tile_log.info("--> Found {} L1C and L2A products for the composite:".format(len(composite_products_all)))
            df_all = pd.DataFrame.from_dict(composite_products_all, orient='index')

            # check granule sizes on the server
            df_all['size'] = df_all['size'].str.split(' ').apply(lambda x: float(x[0]) * {'GB': 1e3, 'MB': 1, 'KB': 1e-3}[x[1]])
            df = df_all.query('size >= '+str(faulty_granule_threshold))
            tile_log.info("Removed {} faulty scenes <{}MB in size from the list:".format(len(df_all)-len(df), faulty_granule_threshold))
            df_faulty = df_all.query('size < '+str(faulty_granule_threshold))
            for r in range(len(df_faulty)):
                tile_log.info("   {} MB: {}".format(df_faulty.iloc[r,:]['size'], df_faulty.iloc[r,:]['title']))

            l1c_products = df[df.processinglevel == 'Level-1C']
            l2a_products = df[df.processinglevel == 'Level-2A']
            tile_log.info("    {} L1C products".format(l1c_products.shape[0]))
            tile_log.info("    {} L2A products".format(l2a_products.shape[0]))

            rel_orbits = np.unique(l1c_products['relativeorbitnumber'])
            if len(rel_orbits) > 0:
                if l1c_products.shape[0] > max_image_number / len(rel_orbits):
                    tile_log.info("Capping the number of L1C products to {}".format(max_image_number))
                    tile_log.info("Relative orbits found covering tile: {}".format(rel_orbits))
                    uuids = []
                    for orb in rel_orbits:
                        uuids = uuids + list(l1c_products.loc[l1c_products['relativeorbitnumber'] == orb].sort_values(by=['cloudcoverpercentage'], ascending=True)['uuid'][:int(max_image_number/len(rel_orbits))])
                    l1c_products = l1c_products[l1c_products['uuid'].isin(uuids)]
                    tile_log.info("    {} L1C products remain:".format(l1c_products.shape[0]))
                    for product in l1c_products['title']:
                        tile_log.info("       {}".format(product))

            rel_orbits = np.unique(l2a_products['relativeorbitnumber'])
            if len(rel_orbits) > 0:
                if l2a_products.shape[0] > max_image_number/len(rel_orbits):
                    tile_log.info("Capping the number of L2A products to {}".format(max_image_number))
                    tile_log.info("Relative orbits found covering tile: {}".format(rel_orbits))
                    uuids = []
                    for orb in rel_orbits:
                        uuids = uuids + list(l2a_products.loc[l2a_products['relativeorbitnumber'] == orb].sort_values(by=['cloudcoverpercentage'], ascending=True)['uuid'][:int(max_image_number/len(rel_orbits))])
                    l2a_products = l2a_products[l2a_products['uuid'].isin(uuids)]
                    tile_log.info("    {} L2A products remain:".format(l2a_products.shape[0]))
                    for product in l2a_products['title']:
                        tile_log.info("       {}".format(product))

            if l1c_products.shape[0]>0 and l2a_products.shape[0]>0:
                tile_log.info("Filtering out L1C products that have the same 'beginposition' time stamp as an existing L2A product.")
                l1c_products, l2a_products = queries_and_downloads.filter_unique_l1c_and_l2a_data(df)
                tile_log.info("--> {} L1C and L2A products with unique 'beginposition' time stamp for the composite:".format(l1c_products.shape[0]+l2a_products.shape[0]))
                tile_log.info("    {} L1C products".format(l1c_products.shape[0]))
                tile_log.info("    {} L2A products".format(l2a_products.shape[0]))
            df = None

            # Search the composite/L2A and L1C directories whether the scenes have already been downloaded and/or processed and check their dir sizes
            if l1c_products.shape[0] > 0:
                tile_log.info("Checking for already downloaded and zipped L1C or L2A products and")
                tile_log.info("  availability of matching L2A products for download.")
                n = len(l1c_products)
                drop=[]
                add=[]
                for r in range(n):
                    id = l1c_products.iloc[r,:]['title']
                    search_term = id.split("_")[2]+"_"+id.split("_")[3]+"_"+id.split("_")[4]+"_"+id.split("_")[5]
                    tile_log.info("Searching locally for file names containing: {}.".format(search_term))
                    file_list = [os.path.join(composite_l1_image_dir, f) for f in os.listdir(composite_l1_image_dir)] + \
                        [os.path.join(composite_l2_image_dir, f) for f in os.listdir(composite_l2_image_dir)] + \
                        [os.path.join(composite_l2_masked_image_dir, f) for f in os.listdir(composite_l2_masked_image_dir)]
                    for f in file_list:
                        if search_term in f:
                            tile_log.info("  Product already downloaded: {}".format(f))
                            drop.append(l1c_products.index[r])
                    search_term = "*"+id.split("_")[2]+"_"+id.split("_")[3]+"_"+id.split("_")[4]+"_"+id.split("_")[5]+"*"
                    tile_log.info("Searching on the data hub for files containing: {}.".format(search_term))
                    matching_l2a_products = queries_and_downloads._file_api_query(user=sen_user,
                                                                                  passwd=sen_pass,
                                                                                  start_date=composite_start_date,end_date=composite_end_date,filename=search_term,cloud=cloud_cover,producttype="S2MSI2A")

                    matching_l2a_products_df = pd.DataFrame.from_dict(matching_l2a_products, orient='index')
                    # 07/03/2023: Matt - Applied Ali's fix for converting product size to MB to compare against faulty_grandule_threshold
                    if len(matching_l2a_products_df) == 1 and [float(x[0]) * {'GB': 1e3, 'MB': 1, 'KB': 1e-3}[x[1]] for x in [matching_l2a_products_df['size'][0].split(' ')]][0] > faulty_granule_threshold:
                        tile_log.info("Replacing L1C {} with L2A product:".format(id))
                        tile_log.info("              {}".format(matching_l2a_products_df.iloc[0,:]['title']))
                        drop.append(l1c_products.index[r])
                        add.append(matching_l2a_products_df.iloc[0,:])
                    if len(matching_l2a_products_df) == 0:
                        pass
                    if len(matching_l2a_products_df) > 1:
                        # check granule sizes on the server
                        matching_l2a_products_df['size'] = matching_l2a_products_df['size'].str.split(' ').apply(lambda x: float(x[0]) * {'GB': 1e3, 'MB': 1, 'KB': 1e-3}[x[1]])
                        matching_l2a_products_df = matching_l2a_products_df.query('size >= '+str(faulty_granule_threshold))
                        if matching_l2a_products_df.iloc[0,:]['size'].str.split(' ').apply(lambda x: float(x[0]) * {'GB': 1e3, 'MB': 1, 'KB': 1e-3}[x[1]]) > faulty_granule_threshold:
                            tile_log.info("Replacing L1C {} with L2A product:".format(id))
                            tile_log.info("              {}".format(matching_l2a_products_df.iloc[0,:]['title']))
                            drop.append(l1c_products.index[r])
                            add.append(matching_l2a_products_df.iloc[0,:])
                if len(drop) > 0:
                    l1c_products = l1c_products.drop(index=drop)
                if len(add) > 0:
                    l2a_products = l2a_products.append(add)
                l2a_products = l2a_products.drop_duplicates(subset='title')
                tile_log.info("    {} L1C products remaining for download".format(l1c_products.shape[0]))
                tile_log.info("    {} L2A products remaining for download".format(l2a_products.shape[0]))    


            # if L1C products remain after matching for L2As, then download the unmatched L1Cs
            if l1c_products.shape[0] > 0:
                tile_log.info("Downloading Sentinel-2 L1C products.")
                        
                queries_and_downloads.download_s2_data_from_df(l1c_products,
                                                                    composite_l1_image_dir,
                                                                    composite_l2_image_dir,
                                                                    source="scihub",
                                                                    user=sen_user,
                                                                    passwd=sen_pass,
                                                                    try_scihub_on_fail=True)
                tile_log.info("Atmospheric correction with sen2cor.")
                raster_manipulation.atmospheric_correction(composite_l1_image_dir,
                                                           composite_l2_image_dir,
                                                           sen2cor_path,
                                                           delete_unprocessed_image=False,
                                                           log=tile_log)
            tile_log.info(f"download source is   {download_source}")
            if l2a_products.shape[0] > 0:
                tile_log.info("Downloading Sentinel-2 L2A products.")
                queries_and_downloads.download_s2_data(l2a_products.to_dict('index'),
                                                            composite_l1_image_dir,
                                                            composite_l2_image_dir,
                                                            source="scihub",
                                                            #download_source,
                                                            user=sen_user,
                                                            passwd=sen_pass,
                                                            try_scihub_on_fail=True)

            # check for incomplete L2A downloads
            incomplete_downloads, sizes = raster_manipulation.find_small_safe_dirs(composite_l2_image_dir, threshold=faulty_granule_threshold*1024*1024)
            if len(incomplete_downloads) > 0:
                for index, safe_dir in enumerate(incomplete_downloads):
                    if sizes[index]/1024/1024 < faulty_granule_threshold and os.path.exists(safe_dir):
                        tile_log.warning("Found likely incomplete download of size {} MB: {}".format(str(round(sizes[index]/1024/1024)), safe_dir))
                        #shutil.rmtree(safe_dir)
            
            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Image download and atmospheric correction for composite is complete.")
            tile_log.info("---------------------------------------------------------------")

            if config_dict["do_delete"]:
                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Deleting downloaded L1C images for composite, keeping only derived L2A products")
                tile_log.info("---------------------------------------------------------------")
                directory = composite_l1_image_dir
                tile_log.info('Deleting {}'.format(directory))
                shutil.rmtree(directory)
                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Deletion of L1C images complete. Keeping only L2A images.")
                tile_log.info("---------------------------------------------------------------")
            else:
                if config_dict["do_zip"]:
                    tile_log.info("---------------------------------------------------------------")
                    tile_log.info("Zipping downloaded L1C images for composite after atmospheric correction")
                    tile_log.info("---------------------------------------------------------------")
                    filesystem_utilities.zip_contents(composite_l1_image_dir)
                    tile_log.info("---------------------------------------------------------------")
                    tile_log.info("Zipping complete")
                    tile_log.info("---------------------------------------------------------------")

            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Applying simple cloud, cloud shadow and haze mask based on SCL files and stacking the masked band raster files.")
            tile_log.info("---------------------------------------------------------------")

            directory = composite_l2_masked_image_dir
            masked_file_paths = [f for f in os.listdir(directory) if f.endswith(".tif") \
                                 and os.path.isfile(os.path.join(directory, f))]

            directory = composite_l2_image_dir
            l2a_zip_file_paths = [f for f in os.listdir(directory) if f.endswith(".zip")]

            if len(l2a_zip_file_paths) > 0:
                for f in l2a_zip_file_paths:
                    # check whether the zipped file has already been cloud masked
                    zip_timestamp  = filesystem_utilities.get_image_acquisition_time(os.path.basename(f)).strftime("%Y%m%dT%H%M%S")
                    if any(zip_timestamp in f for f in masked_file_paths):
                        continue
                    else:
                        # extract it if not
                        filesystem_utilities.unzip_contents(os.path.join(composite_l2_image_dir, f),
                                       ifstartswith="S2", ending=".SAFE")

            directory = composite_l2_image_dir
            l2a_safe_file_paths = [f for f in os.listdir(directory) if f.endswith(".SAFE") \
                                   and os.path.isdir(os.path.join(directory, f))]

            files_for_cloud_masking = []
            if len(l2a_safe_file_paths) > 0:
                for f in l2a_safe_file_paths:
                    # check whether the L2A SAFE file has already been cloud masked
                    safe_timestamp  = filesystem_utilities.get_image_acquisition_time(os.path.basename(f)).strftime("%Y%m%dT%H%M%S")
                    if any(safe_timestamp in f for f in masked_file_paths):
                        continue
                    else:
                        # add it to the list of files to do if it has not been cloud masked yet
                        files_for_cloud_masking = files_for_cloud_masking + [f]

            if len(files_for_cloud_masking) == 0:
                tile_log.info("No L2A images found for cloud masking. They may already have been done.")
            else:
                raster_manipulation.apply_scl_cloud_mask(composite_l2_image_dir,
                                                              composite_l2_masked_image_dir,
                                                              scl_classes=[0,1,2,3,8,9,10,11],
                                                              buffer_size=buffer_size_composite,
                                                              bands=bands,
                                                              out_resolution=out_resolution,
                                                              haze=None,
                                                              epsg=epsg,
                                                              skip_existing=skip_existing)
            # I.R. 20220607 START
            # Apply offset to any images of processing baseline 0400 in the composite cloud_masked folder
            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Offsetting cloud masked L2A images for composite.")
            tile_log.info("---------------------------------------------------------------")

            raster_manipulation.apply_processing_baseline_offset_correction_to_tiff_file_directory(composite_l2_masked_image_dir, composite_l2_masked_image_dir)

            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Offsetting of cloud masked L2A images for composite complete.")
            tile_log.info("---------------------------------------------------------------")
            # I.R. 20220607 END

            if config_dict["do_quicklooks"] or config_dict["do_all"]:
                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Producing quicklooks.")
                tile_log.info("---------------------------------------------------------------")
                dirs_for_quicklooks = [composite_l2_masked_image_dir]
                for main_dir in dirs_for_quicklooks:
                    files = [ f.path for f in os.scandir(main_dir) if f.is_file() and os.path.basename(f).endswith(".tif") ]
                    #files = [ f.path for f in os.scandir(main_dir) if f.is_file() and os.path.basename(f).endswith(".tif") and "class" in os.path.basename(f) ] # do classification images only
                    if len(files) == 0:
                        tile_log.warning("No images found in {}.".format(main_dir))
                    else:
                        for f in files:
                            quicklook_path = os.path.join(quicklook_dir, os.path.basename(f).split(".")[0]+".png")
                            tile_log.info("Creating quicklook: {}".format(quicklook_path))
                            raster_manipulation.create_quicklook(f,
                                                                      quicklook_path,
                                                                      width=512,
                                                                      height=512,
                                                                      format="PNG",
                                                                      bands=[3,2,1],
                                                                      scale_factors=[[0,2000,0,255]]
                                                                      )
            tile_log.info("Quicklooks complete.")

            if config_dict["do_zip"]:
                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Zipping downloaded L2A images for composite after cloud masking and band stacking")
                tile_log.info("---------------------------------------------------------------")
                filesystem_utilities.zip_contents(composite_l2_image_dir)
                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Zipping complete")
                tile_log.info("---------------------------------------------------------------")

            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Building initial cloud-free median composite from directory {}".format(composite_l2_masked_image_dir))
            tile_log.info("---------------------------------------------------------------")
            directory = composite_l2_masked_image_dir
            masked_file_paths = [f for f in os.listdir(directory) if f.endswith(".tif") \
                                 and os.path.isfile(os.path.join(directory, f))]

            if len(masked_file_paths) > 0:
                raster_manipulation.clever_composite_directory(composite_l2_masked_image_dir,
                                                                    composite_dir,
                                                                    chunks=config_dict["chunks"],
                                                                    generate_date_images=True,
                                                                    missing_data_value=0)
                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Baseline composite complete.")
                tile_log.info("---------------------------------------------------------------")

                if config_dict["do_quicklooks"] or config_dict["do_all"]:
                    tile_log.info("---------------------------------------------------------------")
                    tile_log.info("Producing quicklooks.")
                    tile_log.info("---------------------------------------------------------------")
                    dirs_for_quicklooks = [composite_dir]
                    for main_dir in dirs_for_quicklooks:
                        files = [ f.path for f in os.scandir(main_dir) if f.is_file() and os.path.basename(f).endswith(".tif") ]
                        #files = [ f.path for f in os.scandir(main_dir) if f.is_file() and os.path.basename(f).endswith(".tif") and "class" in os.path.basename(f) ] # do classification images only
                        if len(files) == 0:
                            tile_log.warning("No images found in {}.".format(main_dir))
                        else:
                            for f in files:
                                quicklook_path = os.path.join(quicklook_dir, os.path.basename(f).split(".")[0]+".png")
                                tile_log.info("Creating quicklook: {}".format(quicklook_path))
                                raster_manipulation.create_quicklook(f,
                                                                          quicklook_path,
                                                                          width=512,
                                                                          height=512,
                                                                          format="PNG",
                                                                          bands=[3,2,1],
                                                                          scale_factors=[[0,2000,0,255]]
                                                                          )
                    tile_log.info("Quicklooks complete.")

                if config_dict["do_delete"]:
                    tile_log.info("---------------------------------------------------------------")
                    tile_log.info("Deleting intermediate cloud-masked L2A images used for the baseline composite")
                    tile_log.info("---------------------------------------------------------------")
                    f = composite_l2_masked_image_dir
                    tile_log.info('Deleting {}'.format(f))
                    shutil.rmtree(f)
                    tile_log.info("---------------------------------------------------------------")
                    tile_log.info("Intermediate file products have been deleted.")
                    tile_log.info("They can be reprocessed from the downloaded L2A images.")
                    tile_log.info("---------------------------------------------------------------")
                else:
                    if config_dict["do_zip"]:
                        tile_log.info("---------------------------------------------------------------")
                        tile_log.info("Zipping cloud-masked L2A images used for the baseline composite")
                        tile_log.info("---------------------------------------------------------------")
                        filesystem_utilities.zip_contents(composite_l2_masked_image_dir)
                        tile_log.info("---------------------------------------------------------------")
                        tile_log.info("Zipping complete")
                        tile_log.info("---------------------------------------------------------------")

                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Compressing tiff files in directory {} and all subdirectories".format(composite_dir))
                tile_log.info("---------------------------------------------------------------")
                for root, dirs, files in os.walk(composite_dir):
                    all_tiffs = [image_name for image_name in files if image_name.endswith(".tif")]
                    for this_tiff in all_tiffs:
                        raster_manipulation.compress_tiff(os.path.join(root, this_tiff), os.path.join(root, this_tiff))

                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Baseline image composite, file compression, zipping and deletion of")
                tile_log.info("intermediate file products (if selected) are complete.")
                tile_log.info("---------------------------------------------------------------")

            else:
                tile_log.error("No cloud-masked L2A image products found in {}.".format(composite_l2_image_dir))
                tile_log.error("Cannot produce a median composite. Download and cloud-mask some images first.")

        # ------------------------------------------------------------------------
        # Step 2: Download change detection images for the specific time window (L2A where available plus additional L1C)
        # ------------------------------------------------------------------------
        if config_dict["do_all"] or config_dict["do_download"]:
            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Downloading change detection images between {} and {} with cloud cover <= {}".format(
                     start_date, end_date, cloud_cover))
            tile_log.info("---------------------------------------------------------------")

            products_all = queries_and_downloads.check_for_s2_data_by_date(tile_root_dir,
                                                                               start_date,
                                                                               end_date,
                                                                               credentials_dict,
                                                                               cloud_cover=cloud_cover,
                                                                               tile_id=tile_to_process,
                                                                               producttype=None #"S2MSI2A" or "S2MSI1C"
                                                                               )
            tile_log.info("--> Found {} L1C and L2A products for change detection:".format(len(products_all)))
            df_all = pd.DataFrame.from_dict(products_all, orient='index')

            # check granule sizes on the server
            df_all['size'] = df_all['size'].str.split(' ').apply(lambda x: float(x[0]) * {'GB': 1e3, 'MB': 1, 'KB': 1e-3}[x[1]])
            df = df_all.query('size >= '+str(faulty_granule_threshold))
            tile_log.info("Removed {} faulty scenes <{}MB in size from the list:".format(len(df_all)-len(df), faulty_granule_threshold))
            df_faulty = df_all.query('size < '+str(faulty_granule_threshold))
            for r in range(len(df_faulty)):
                tile_log.info("   {} MB: {}".format(df_faulty.iloc[r,:]['size'], df_faulty.iloc[r,:]['title']))

            l1c_products = df[df.processinglevel == 'Level-1C']
            l2a_products = df[df.processinglevel == 'Level-2A']
            tile_log.info("    {} L1C products".format(l1c_products.shape[0]))
            tile_log.info("    {} L2A products".format(l2a_products.shape[0]))

            if l1c_products.shape[0]>0 and l2a_products.shape[0]>0:
                tile_log.info("Filtering out L1C products that have the same 'beginposition' time stamp as an existing L2A product.")
                l1c_products, l2a_products = queries_and_downloads.filter_unique_l1c_and_l2a_data(df)
                tile_log.info("--> {} L1C and L2A products with unique 'beginposition' time stamp for the composite:".format(l1c_products.shape[0]+l2a_products.shape[0]))
                tile_log.info("    {} L1C products".format(l1c_products.shape[0]))
                tile_log.info("    {} L2A products".format(l2a_products.shape[0]))
            df = None

            #TODO: Before the next step, search the composite/L2A and L1C directories whether the scenes have already been downloaded and/or processed and check their dir sizes
            # Remove those already obtained from the list

            if l1c_products.shape[0] > 0:
                tile_log.info("Checking for availability of L2A products to minimise download and atmospheric correction of L1C products.")
                n = len(l1c_products)
                drop=[]
                add=[]
                for r in range(n):
                    id = l1c_products.iloc[r,:]['title']
                    search_term = "*"+id.split("_")[2]+"_"+id.split("_")[3]+"_"+id.split("_")[4]+"_"+id.split("_")[5]+"*"
                    tile_log.info("Search term: {}.".format(search_term))
                    matching_l2a_products = queries_and_downloads._file_api_query(user=sen_user,
                                                                                       passwd=sen_pass,
                                                                                       start_date=start_date,
                                                                                       end_date=end_date,
                                                                                       filename=search_term,
                                                                                       cloud=cloud_cover,
                                                                                       producttype="S2MSI2A"
                                                                                       )

                    matching_l2a_products_df = pd.DataFrame.from_dict(matching_l2a_products, orient='index')
                    if len(matching_l2a_products_df) == 1:
                        tile_log.info(matching_l2a_products_df.iloc[0,:]['size'])
                        matching_l2a_products_df['size'] = matching_l2a_products_df['size'].str.split(' ').apply(lambda x: float(x[0]) * {'GB': 1e3, 'MB': 1, 'KB': 1e-3}[x[1]])
                        if matching_l2a_products_df.iloc[0,:]['size'] > faulty_granule_threshold:
                            tile_log.info("Replacing L1C {} with L2A product:".format(id))
                            tile_log.info("              {}".format(matching_l2a_products_df.iloc[0,:]['title']))
                            drop.append(l1c_products.index[r])
                            add.append(matching_l2a_products_df.iloc[0,:])
                    if len(matching_l2a_products_df) == 0:
                        tile_log.info("Found no match for L1C: {}.".format(id))
                    if len(matching_l2a_products_df) > 1:
                        # check granule sizes on the server
                        matching_l2a_products_df['size'] = matching_l2a_products_df['size'].str.split(' ').apply(lambda x: float(x[0]) * {'GB': 1e3, 'MB': 1, 'KB': 1e-3}[x[1]])
                        if matching_l2a_products_df.iloc[0,:]['size'] > faulty_granule_threshold:
                            tile_log.info("Replacing L1C {} with L2A product:".format(id))
                            tile_log.info("              {}".format(matching_l2a_products_df.iloc[0,:]['title']))
                            drop.append(l1c_products.index[r])
                            add.append(matching_l2a_products_df.iloc[0,:])

                if len(drop) > 0:
                    l1c_products = l1c_products.drop(index=drop)
                if len(add) > 0:
                    if config_dict["do_dev"]:
                        add = pd.DataFrame(add)
                        tile_log.info("Types for concatenation: {}, {}".format(type(l2a_products), type(add)))
                        l2a_products = pd.concat([l2a_products, add])
                        #TODO: test the above fix for:
                        # pyeo_1/pyeo_1/apps/change_detection/tile_based_change_detection_from_cover_maps.py:456: FutureWarning: The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.
                    else:
                        l2a_products = l2a_products.append(add)

                tile_log.info("    {} L1C products remaining for download".format(l1c_products.shape[0]))
                l2a_products = l2a_products.drop_duplicates(subset='title')
                #I.R.
                tile_log.info("    {} L2A products remaining for download".format(l2a_products.shape[0]))
                if (l1c_products.shape[0] >0):
                    tile_log.info("Downloading Sentinel-2 L1C products.")
                    queries_and_downloads.download_s2_data_from_df(l1c_products,
                                                                l1_image_dir,
                                                                l2_image_dir,
                                                                download_source,
                                                                user=sen_user,
                                                                passwd=sen_pass,
                                                                try_scihub_on_fail=True)
                    tile_log.info("Atmospheric correction with sen2cor.")
                    raster_manipulation.atmospheric_correction(l1_image_dir,
                                                                    l2_image_dir,
                                                                    sen2cor_path,
                                                                    delete_unprocessed_image=False,
                                                                    log=tile_log)
            if l2a_products.shape[0] > 0:
                tile_log.info("Downloading Sentinel-2 L2A products.")
                queries_and_downloads.download_s2_data(l2a_products.to_dict('index'),
                                                            l1_image_dir,
                                                            l2_image_dir,
                                                            download_source,
                                                            user=sen_user,
                                                            passwd=sen_pass,
                                                            try_scihub_on_fail=True)

            # check for incomplete L2A downloads and remove them
            incomplete_downloads, sizes = raster_manipulation.find_small_safe_dirs(l2_image_dir, threshold=faulty_granule_threshold*1024*1024)
            if len(incomplete_downloads) > 0:
                for index, safe_dir in enumerate(incomplete_downloads):
                    if sizes[index]/1024/1024 < faulty_granule_threshold and os.path.exists(safe_dir):
                        tile_log.warning("Found likely incomplete download of size {} MB: {}".format(str(round(sizes[index]/1024/1024)), safe_dir))
                        #shutil.rmtree(safe_dir)

            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Image download and atmospheric correction for change detection images is complete.")
            tile_log.info("---------------------------------------------------------------")
            #TODO: delete L1C images if do_delete is True
            if config_dict["do_delete"]:
                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Deleting L1C images downloaded for change detection.")
                tile_log.info("Keeping only the derived L2A images after atmospheric correction.")
                tile_log.info("---------------------------------------------------------------")
                directory = l1_image_dir
                tile_log.info('Deleting {}'.format(directory))
                shutil.rmtree(directory)
                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Deletion complete")
                tile_log.info("---------------------------------------------------------------")
            else:
                if config_dict["do_zip"]:
                    tile_log.info("---------------------------------------------------------------")
                    tile_log.info("Zipping L1C images downloaded for change detection")
                    tile_log.info("---------------------------------------------------------------")
                    filesystem_utilities.zip_contents(l1_image_dir)
                    tile_log.info("---------------------------------------------------------------")
                    tile_log.info("Zipping complete")
                    tile_log.info("---------------------------------------------------------------")

            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Applying simple cloud, cloud shadow and haze mask based on SCL files and stacking the masked band raster files.")
            tile_log.info("---------------------------------------------------------------")
            #l2a_paths = [ f.path for f in os.scandir(l2_image_dir) if f.is_dir() ]
            #tile_log.info("  l2_image_dir: {}".format(l2_image_dir))
            #tile_log.info("  l2_masked_image_dir: {}".format(l2_masked_image_dir))
            #tile_log.info("  bands: {}".format(bands))
            raster_manipulation.apply_scl_cloud_mask(l2_image_dir,
                                                          l2_masked_image_dir,
                                                          scl_classes=[0,1,2,3,8,9,10,11],
                                                          buffer_size=buffer_size,
                                                          bands=bands,
                                                          out_resolution=out_resolution,
                                                          haze=None,
                                                          epsg=epsg,
                                                          skip_existing=skip_existing)

            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Cloud masking and band stacking of new L2A images are complete.")
            tile_log.info("---------------------------------------------------------------")

            # I.R. 20220607 START
            # Apply offset to any images of processing baseline 0400 in the composite cloud_masked folder
            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Offsetting cloud masked L2A images.")
            tile_log.info("---------------------------------------------------------------")

            raster_manipulation.apply_processing_baseline_offset_correction_to_tiff_file_directory(l2_masked_image_dir, l2_masked_image_dir)

            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Offsetting of cloud masked L2A images complete.")
            tile_log.info("---------------------------------------------------------------")
            # I.R. 20220607 END

            if config_dict["do_quicklooks"] or config_dict["do_all"]:
                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Producing quicklooks.")
                tile_log.info("---------------------------------------------------------------")
                dirs_for_quicklooks = [l2_masked_image_dir]
                for main_dir in dirs_for_quicklooks:
                    files = [ f.path for f in os.scandir(main_dir) if f.is_file() and os.path.basename(f).endswith(".tif") ]
                    #files = [ f.path for f in os.scandir(main_dir) if f.is_file() and os.path.basename(f).endswith(".tif") and "class" in os.path.basename(f) ] # do classification images only
                    if len(files) == 0:
                        tile_log.warning("No images found in {}.".format(main_dir))
                    else:
                        for f in files:
                            quicklook_path = os.path.join(quicklook_dir, os.path.basename(f).split(".")[0]+".png")
                            tile_log.info("Creating quicklook: {}".format(quicklook_path))
                            raster_manipulation.create_quicklook(f,
                                                                      quicklook_path,
                                                                      width=512,
                                                                      height=512,
                                                                      format="PNG",
                                                                      bands=[3,2,1],
                                                                      scale_factors=[[0,2000,0,255]]
                                                                      )
            tile_log.info("Quicklooks complete.")

            if config_dict["do_zip"]:
                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Zipping L2A images downloaded for change detection")
                tile_log.info("---------------------------------------------------------------")
                filesystem_utilities.zip_contents(l2_image_dir)
                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Zipping complete")
                tile_log.info("---------------------------------------------------------------")

            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Compressing tiff files in directory {} and all subdirectories".format(l2_masked_image_dir))
            tile_log.info("---------------------------------------------------------------")
            for root, dirs, files in os.walk(l2_masked_image_dir):
                all_tiffs = [image_name for image_name in files if image_name.endswith(".tif")]
                for this_tiff in all_tiffs:
                    raster_manipulation.compress_tiff(os.path.join(root, this_tiff), os.path.join(root, this_tiff))

            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Pre-processing of change detection images, file compression, zipping")
            tile_log.info("and deletion of intermediate file products (if selected) are complete.")
            tile_log.info("---------------------------------------------------------------")

        # ------------------------------------------------------------------------
        # Step 3: Classify each L2A image and the baseline composite
        # ------------------------------------------------------------------------
        if config_dict["do_all"] or config_dict["do_classify"]:
            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Classify a land cover map for each L2A image and composite image using a saved model")
            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Model used: {}".format(model_path))
            if skip_existing:
                tile_log.info("Skipping existing classification images if found.")
            classification.classify_directory(composite_dir,
                                                   model_path,
                                                   categorised_image_dir,
                                                   prob_out_dir = None,
                                                   apply_mask=False,
                                                   out_type="GTiff",
                                                   chunks=config_dict["chunks"],
                                                   skip_existing=skip_existing)
            classification.classify_directory(l2_masked_image_dir,
                                                   model_path,
                                                   categorised_image_dir,
                                                   prob_out_dir = None,
                                                   apply_mask=False,
                                                   out_type="GTiff",
                                                   chunks=config_dict["chunks"],
                                                   skip_existing=skip_existing)

            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Compressing tiff files in directory {} and all subdirectories".format(categorised_image_dir))
            tile_log.info("---------------------------------------------------------------")
            for root, dirs, files in os.walk(categorised_image_dir):
                all_tiffs = [image_name for image_name in files if image_name.endswith(".tif")]
                for this_tiff in all_tiffs:
                    raster_manipulation.compress_tiff(os.path.join(root, this_tiff), os.path.join(root, this_tiff))

            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Classification of all images is complete.")
            tile_log.info("---------------------------------------------------------------")

            if config_dict["do_quicklooks"] or config_dict["do_all"]:
                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Producing quicklooks.")
                tile_log.info("---------------------------------------------------------------")
                dirs_for_quicklooks = [categorised_image_dir]
                for main_dir in dirs_for_quicklooks:
                    #files = [ f.path for f in os.scandir(main_dir) if f.is_file() and os.path.basename(f).endswith(".tif") ]
                    files = [ f.path for f in os.scandir(main_dir) if f.is_file() and os.path.basename(f).endswith(".tif") and "class" in os.path.basename(f) ] # do classification images only
                    if len(files) == 0:
                        tile_log.warning("No images found in {}.".format(main_dir))
                    else:
                        for f in files:
                            quicklook_path = os.path.join(quicklook_dir, os.path.basename(f).split(".")[0]+".png")
                            tile_log.info("Creating quicklook: {}".format(quicklook_path))
                            raster_manipulation.create_quicklook(f,
                                                                      quicklook_path,
                                                                      width=512,
                                                                      height=512,
                                                                      format="PNG"
                                                                      )
            tile_log.info("Quicklooks complete.")


        # ------------------------------------------------------------------------
        # Step 4: Pair up the class images with the composite baseline map
        # and identify all pixels with the change between groups of classes of interest.
        # Optionally applies a sieve filter to the class images if specified in the ini file.
        # Confirms detected changes by NDVI differencing.
        # ------------------------------------------------------------------------

        if config_dict["do_all"] or config_dict["do_change"]:
            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Creating change layers from stacked class images.")
            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Changes of interest:")
            tile_log.info("  from any of the classes {}".format(config_dict["from_classes"]))
            tile_log.info("  to   any of the classes {}".format(config_dict["to_classes"]))

            # optionally sieve the class images
            if sieve > 0:
                tile_log.info("Applying sieve to classification outputs.")
                sieved_paths = raster_manipulation.sieve_directory(in_dir = categorised_image_dir,
                                                                        out_dir = sieved_image_dir,
                                                                        neighbours = 8,
                                                                        sieve = sieve,
                                                                        out_type="GTiff",
                                                                        skip_existing=skip_existing)
                # if sieve was chosen, work with the sieved class images
                class_image_dir = sieved_image_dir
            else:
                # if sieve was not chosen, work with the original class images
                class_image_dir = categorised_image_dir

            # get all image paths in the classification maps directory except the class composites
            class_image_paths = [ f.path for f in os.scandir(class_image_dir) if f.is_file() and f.name.endswith(".tif") \
                                  and not "composite_" in f.name ]
            if len(class_image_paths) == 0:
                raise FileNotFoundError("No class images found in {}.".format(class_image_dir))

            # sort class images by image acquisition date
            class_image_paths = list(filter(pyeo_1.filesystem_utilities.get_image_acquisition_time, class_image_paths))
            class_image_paths.sort(key=lambda x: pyeo_1.filesystem_utilities.get_image_acquisition_time(x))
            for index, image in enumerate(class_image_paths):
                tile_log.info("{}: {}".format(index, image))

            # find the latest available composite
            try:
                latest_composite_name = \
                    filesystem_utilities.sort_by_timestamp(
                        [image_name for image_name in os.listdir(composite_dir) if image_name.endswith(".tif")],
                        recent_first=True
                    )[0]
                latest_composite_path = os.path.join(composite_dir, latest_composite_name)
                tile_log.info("Most recent composite at {}".format(latest_composite_path))
            except IndexError:
                tile_log.critical("Latest composite not found. The first time you run this script, you need to include the "
                             "do_build_composite = True line in the .ini file to create a base composite to work off. If you have already done this,"
                             "check that the earliest dated image in your images/merged folder is later than the earliest"
                             " dated image in your composite/ folder.")
                sys.exit(1)

            latest_class_composite_path = os.path.join(
                                                       class_image_dir, \
                                                       [ f.path for f in os.scandir(class_image_dir) if f.is_file() \
                                                         and os.path.basename(latest_composite_path)[:-4] in f.name \
                                                         and f.name.endswith(".tif")][0]
                                          )

            tile_log.info("Most recent class composite at {}".format(latest_class_composite_path))
            if not os.path.exists(latest_class_composite_path):
                tile_log.critical("Latest class composite not found. The first time you run this script, you need to include the "
                             "--build-composite flag to create a base composite to work off. If you have already done this,"
                             "check that the earliest dated image in your images/merged folder is later than the earliest"
                             " dated image in your composite/ folder. Then, you need to run the --classify option.")
                sys.exit(1)

            if config_dict["do_dev"]: # set the name of the report file in the development version run
                before_timestamp = filesystem_utilities.get_change_detection_dates(os.path.basename(latest_class_composite_path))[0]
                #I.R. 20220611 START
                ## Timestamp report with the date of most recent classified image that contributes to it
                after_timestamp  = filesystem_utilities.get_image_acquisition_time(os.path.basename(class_image_paths[-1]))
                ## ORIGINAL
                # gets timestamp of the earliest change image of those available in class_image_path
                # after_timestamp  = pyeo_1.filesystem_utilities.get_image_acquisition_time(os.path.basename(class_image_paths[0]))
                #I.R. 20220611 END
                output_product = os.path.join(probability_image_dir,
                                              "report_{}_{}_{}.tif".format(
                                              before_timestamp.strftime("%Y%m%dT%H%M%S"),
                                              tile_id,
                                              after_timestamp.strftime("%Y%m%dT%H%M%S"))
                                              )
                tile_log.info("I.R. Report file name will be {}".format(output_product))

                # if a report file exists, archive it  ( I.R. Changed from 'rename it to show it has been updated')
                n_report_files = len([ f for f in os.scandir(probability_image_dir) if f.is_file() \
                                       and f.name.startswith("report_") \
                                       and f.name.endswith(".tif")])

                if n_report_files > 0:
                    # I.R. ToDo: Should iterate over output_product_existing in case more than one report file is present (though unlikely)
                    output_product_existing = [ f.path for f in os.scandir(probability_image_dir) if f.is_file() \
                                                and f.name.startswith("report_") \
                                                and f.name.endswith(".tif")][0]
                    tile_log.info("Found existing report image product: {}".format(output_product_existing))
                    #I.R. 20220610 START
                    ## Mark existing reports as 'archive_'
                    ## - do not try and extend upon existing reports
                    ## - calls to __change_from_class_maps below will build a report incorporating all new AND pre-existing change maps
                    ## - this might be the cause of the error in report generation that caused over-range and periodicity in the histogram - as reported to Heiko by email
                    # report_timestamp = pyeo_1.filesystem_utilities.get_change_detection_dates(os.path.basename(output_product_existing))[1]
                    # if report_timestamp < after_timestamp:
                        # tile_log.info("Report timestamp {}".format(report_timestamp.strftime("%Y%m%dT%H%M%S")))
                        # tile_log.info(" is earlier than {}".format(after_timestamp.strftime("%Y%m%dT%H%M%S")))
                        # tile_log.info("Updating its file name to: {}".format(output_product))
                        # os.rename(output_product_existing, output_product)

                    # Renaming any pre-existing report file with prefix 'archive_'
                    ## it will therefore not be detected in __change_from_class_maps which will therefore create a new report file

                    output_product_existing_archived = os.path.join(os.path.dirname(output_product_existing), 'archived_' + os.path.basename(output_product_existing))
                    tile_log.info("Renaming existing report image product to: {}".format(output_product_existing_archived))
                    os.rename(output_product_existing, output_product_existing_archived)

                    #I.R. 20220610 END

            # find change patterns in the stack of classification images
            for index, image in enumerate(class_image_paths):
                before_timestamp = filesystem_utilities.get_change_detection_dates(os.path.basename(latest_class_composite_path))[0]
                after_timestamp  = filesystem_utilities.get_image_acquisition_time(os.path.basename(image))
                #I.R. 20220612 START
                tile_log.info("  class image index: {} of {}".format(index, len(class_image_paths)))
                #I.R. 20220612 END
                tile_log.info("  early time stamp: {}".format(before_timestamp))
                tile_log.info("  late  time stamp: {}".format(after_timestamp))
                change_raster = os.path.join(probability_image_dir,
                                             "change_{}_{}_{}.tif".format(
                                             before_timestamp.strftime("%Y%m%dT%H%M%S"),
                                             #tile_id,
                                             tile_to_process,
                                             after_timestamp.strftime("%Y%m%dT%H%M%S"))
                                             )
                tile_log.info("  Change raster file to be created: {}".format(change_raster))
                if config_dict["do_dev"]:
                    # This function looks for changes from class 'change_from' in the composite to any of the 'change_to_classes'
                    # in the change images. Pixel values are the acquisition date of the detected change of interest or zero.
                    #TODO: In change_from_class_maps(), add a flag (e.g. -1) whether a pixel was a cloud in the later image.
                    # Applying check whether dNDVI < -0.2, i.e. greenness has decreased over changed areas

                    tile_log.info("Update of the report image product based on change detection image.")
                    raster_manipulation.__change_from_class_maps(latest_class_composite_path,
                                                                image,
                                                                change_raster,
                                                                change_from = config_dict["from_classes"],
                                                                change_to = config_dict["to_classes"],
                                                                report_path = output_product,
                                                                skip_existing = skip_existing,
                                                                old_image_dir = composite_dir,
                                                                new_image_dir = l2_masked_image_dir,
                                                                viband1 = 4,
                                                                viband2 = 3,
                                                                threshold = -0.2
                                                                )
                else:
                    raster_manipulation.change_from_class_maps(latest_class_composite_path,
                                                                image,
                                                                change_raster,
                                                                change_from = config_dict["from_classes"],
                                                                change_to = config_dict["to_classes"],
                                                                skip_existing = skip_existing
                                                                )

            # I.R. ToDo: Insert new report_analysis function to generate additional computed layers derived
            # from the outputs of iteration through change image set above
            # E.g. :
                # binarisation
                # area analysis and filtering

            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Post-classification change detection complete.")
            tile_log.info("---------------------------------------------------------------")

            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Compressing tiff files in directory {} and all subdirectories".format(probability_image_dir))
            tile_log.info("---------------------------------------------------------------")
            for root, dirs, files in os.walk(probability_image_dir):
                all_tiffs = [image_name for image_name in files if image_name.endswith(".tif")]
                for this_tiff in all_tiffs:
                    raster_manipulation.compress_tiff(os.path.join(root, this_tiff), os.path.join(root, this_tiff))

            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Compressing tiff files in directory {} and all subdirectories".format(sieved_image_dir))
            tile_log.info("---------------------------------------------------------------")
            for root, dirs, files in os.walk(sieved_image_dir):
                all_tiffs = [image_name for image_name in files if image_name.endswith(".tif")]
                for this_tiff in all_tiffs:
                    raster_manipulation.compress_tiff(os.path.join(root, this_tiff), os.path.join(root, this_tiff))

            if not config_dict["do_dev"]:
                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Creating aggregated report file. Deprecated in the development version.")
                tile_log.info("---------------------------------------------------------------")
                # combine all change layers into one output raster with two layers:
                #   (1) pixels show the earliest change detection date (expressed as the number of days since 1/1/2000)
                #   (2) pixels show the number of change detection dates (summed up over all change images in the folder)
                date_image_paths = [ f.path for f in os.scandir(probability_image_dir) if f.is_file() and f.name.endswith(".tif") \
                                     and "change_" in f.name ]
                if len(date_image_paths) == 0:
                    raise FileNotFoundError("No class images found in {}.".format(categorised_image_dir))

                before_timestamp = pyeo_1.filesystem_utilities.get_change_detection_dates(os.path.basename(latest_class_composite_path))[0]
                after_timestamp  = pyeo_1.filesystem_utilities.get_image_acquisition_time(os.path.basename(class_image_paths[-1]))
                output_product = os.path.join(probability_image_dir,
                                              "report_{}_{}_{}.tif".format(
                                              before_timestamp.strftime("%Y%m%dT%H%M%S"),
                                              tile_to_process,
                                              # tile_id,
                                              after_timestamp.strftime("%Y%m%dT%H%M%S"))
                                              )
                tile_log.info("Combining date maps: {}".format(date_image_paths))
                raster_manipulation.combine_date_maps(date_image_paths, output_product)

            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Report image product completed / updated: {}".format(output_product))
            tile_log.info("Compressing the report image.")
            tile_log.info("---------------------------------------------------------------")
            raster_manipulation.compress_tiff(output_product, output_product)

            if config_dict["do_delete"]:
                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Deleting intermediate class images used in change detection.")
                tile_log.info("They can be recreated from the cloud-masked, band-stacked L2A images and the saved model.")
                tile_log.info("---------------------------------------------------------------")
                directories = [ categorised_image_dir, sieved_image_dir, probability_image_dir ]
                for directory in directories:
                    paths = [f for f in os.listdir(directory)]
                    for f in paths:
                        # keep the classified composite layers and the report image product for the next change detection
                        if not f.startswith("composite_") and not f.startswith("report_"):
                            tile_log.info('Deleting {}'.format(os.path.join(directory, f)))
                            if os.path.isdir(os.path.join(directory, f)):
                                shutil.rmtree(os.path.join(directory, f))
                            else:
                                os.remove(os.path.join(directory, f))
                tile_log.info("---------------------------------------------------------------")
                tile_log.info("Deletion of intermediate file products complete.")
                tile_log.info("---------------------------------------------------------------")
            else:
                if config_dict["do_zip"]:
                    tile_log.info("---------------------------------------------------------------")
                    tile_log.info("Zipping intermediate class images used in change detection")
                    tile_log.info("---------------------------------------------------------------")
                    directories = [ categorised_image_dir, sieved_image_dir ]
                    for directory in directories:
                        filesystem_utilities.zip_contents(directory, notstartswith = ["composite_", "report_"])
                    tile_log.info("---------------------------------------------------------------")
                    tile_log.info("Zipping complete")
                    tile_log.info("---------------------------------------------------------------")

            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Change detection and report image product updating, file compression, zipping")
            tile_log.info("and deletion of intermediate file products (if selected) are complete.")
            tile_log.info("---------------------------------------------------------------")

        if config_dict["do_delete"]:
            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Deleting temporary directories starting with \'tmp*\'")
            tile_log.info("These can be left over from interrupted processing runs.")
            tile_log.info("---------------------------------------------------------------")
            directory = tile_root_dir
            for root, dirs, files in os.walk(directory):
                temp_dirs = [d for d in dirs if d.startswith("tmp")]
                for temp_dir in temp_dirs:
                    tile_log.info('Deleting {}'.format(os.path.join(root, temp_dir)))
                    if os.path.isdir(os.path.join(directory, f)):
                        shutil.rmtree(os.path.join(directory, f))
                    else:
                        tile_log.warning("This should not have happened. {} is not a directory. Skipping deletion.".format(os.path.join(root, temp_dir)))
            tile_log.info("---------------------------------------------------------------")
            tile_log.info("Deletion of temporary directories complete.")
            tile_log.info("---------------------------------------------------------------")

        # ------------------------------------------------------------------------
        # Step 5: Update the baseline composite with the reflectance values of only the changed pixels.
        #         Update last_date of the baseline composite.
        # ------------------------------------------------------------------------

        if config_dict["do_update"] or config_dict["do_all"]:
            tile_log.warning("---------------------------------------------------------------")
            tile_log.warning("Updating of the baseline composite with new imagery is deprecated and will be ignored.")
            tile_log.warning("---------------------------------------------------------------")
            # Matt 11/05/23: the below code is kept for historical reasons, i.e. if a programmer wants to develop 
            # the update baseline composite method, they can follow the code below to see the thought process.
            '''
            log.info("---------------------------------------------------------------")
            log.info("Updating baseline composite with new imagery.")
            log.info("---------------------------------------------------------------")
            # get all composite file paths
            composite_paths = [ f.path for f in os.scandir(composite_dir) if f.is_file() ]
            if len(composite_paths) == 0:
                raise FileNotFoundError("No composite images found in {}.".format(composite_dir))
            log.info("Sorting composite image list by time stamp.")
            composite_images = \
                pyeo_1.filesystem_utilities.sort_by_timestamp(
                    [image_name for image_name in os.listdir(composite_dir) if image_name.endswith(".tif")],
                    recent_first=False
                )
            try:
                latest_composite_name = \
                    pyeo_1.filesystem_utilities.sort_by_timestamp(
                        [image_name for image_name in os.listdir(composite_dir) if image_name.endswith(".tif")],
                        recent_first=True
                    )[0]
                latest_composite_path = os.path.join(composite_dir, latest_composite_name)
                latest_composite_timestamp = pyeo_1.filesystem_utilities.get_sen_2_image_timestamp(os.path.basename(latest_composite_path))
                log.info("Most recent composite at {}".format(latest_composite_path))
            except IndexError:
                log.critical("Latest composite not found. The first time you run this script, you need to include the "
                             "--build-composite flag to create a base composite to work off. If you have already done this,"
                             "check that the earliest dated image in your images/merged folder is later than the earliest"
                             " dated image in your composite/ folder.")
                sys.exit(1)

            # Find all categorised images
            categorised_paths = [ f.path for f in os.scandir(categorised_image_dir) if f.is_file() ]
            if len(categorised_paths) == 0:
                raise FileNotFoundError("No categorised images found in {}.".format(categorised_image_dir))
            log.info("Sorting categorised image list by time stamp.")
            categorised_images = \
                pyeo_1.filesystem_utilities.sort_by_timestamp(
                    [image_name for image_name in os.listdir(categorised_image_dir) if image_name.endswith(".tif")],
                    recent_first=False
                )
            # Drop the categorised images that were made before the most recent composite date
            latest_composite_timestamp_datetime = pyeo_1.filesystem_utilities.get_image_acquisition_time(latest_composite_name)
            categorised_images = [image for image in categorised_images \
                                 if pyeo_1.filesystem_utilities.get_change_detection_dates(os.path.basename(image))[1] > latest_composite_timestamp_datetime ]

            # Find all L2A images
            l2a_paths = [ f.path for f in os.scandir(l2_masked_image_dir) if f.is_file() ]
            if len(l2a_paths) == 0:
                raise FileNotFoundError("No images found in {}.".format(l2_masked_image_dir))
            log.info("Sorting masked L2A image list by time stamp.")
            l2a_images = \
                pyeo_1.filesystem_utilities.sort_by_timestamp(
                    [image_name for image_name in os.listdir(l2_masked_image_dir) if image_name.endswith(".tif")],
                    recent_first=False
                )

            log.info("Updating most recent composite with new imagery over detected changed areas.")
            for categorised_image in categorised_images:
                # Find corresponding L2A file
                timestamp = pyeo_1.filesystem_utilities.get_change_detection_date_strings(os.path.basename(categorised_image))
                before_time = timestamp[0]
                after_time = timestamp[1]
                granule = pyeo_1.filesystem_utilities.get_sen_2_image_tile(os.path.basename(categorised_image))
                l2a_glob = "S2[A|B]_MSIL2A_{}_*_{}_*.tif".format(after_time, granule)
                log.info("Searching for image name pattern: {}".format(l2a_glob))
                l2a_image = glob.glob(os.path.join(l2_masked_image_dir, l2a_glob))
                if len(l2a_image) == 0:
                    log.warning("Matching L2A file not found for categorised image {}".format(categorised_image))
                else:
                    l2a_image = l2a_image[0]
                log.info("Categorised image: {}".format(categorised_image))
                log.info("Matching stacked masked L2A file: {}".format(l2a_image))

                # Extract all reflectance values from the pixels with the class of interest in the classified image
                with TemporaryDirectory(dir=os.getcwd()) as td:
                    log.info("Creating mask file from categorised image {} for class: {}".format(os.path.join(categorised_image_dir, categorised_image), class_of_interest))
                    mask_path = os.path.join(td, categorised_image.split(sep=".")[0]+".msk")
                    log.info("  at {}".format(mask_path))
                    pyeo_1.raster_manipulation.create_mask_from_class_map(os.path.join(categorised_image_dir, categorised_image),
                                                                        mask_path, [class_of_interest], buffer_size=0, out_resolution=None)
                    masked_image_path = os.path.join(td, categorised_image.split(sep=".")[0]+"_change.tif")
                    pyeo_1.raster_manipulation.apply_mask_to_image(mask_path, l2a_image, masked_image_path)
                    new_composite_path = os.path.join(composite_dir, "composite_{}.tif".format(
                                                      pyeo_1.filesystem_utilities.get_sen_2_image_timestamp(os.path.basename(l2a_image))))
                    # Update pixel values in the composite over the selected pixel locations where values are not missing
                    log.info("  {}".format(latest_composite_path))
                    log.info("  {}".format([l2a_image]))
                    log.info("  {}".format(new_composite_path))
                    # todo generate_date_image=True currently produces a type error
                    pyeo_1.raster_manipulation.update_composite_with_images(
                                                                         latest_composite_path,
                                                                         [masked_image_path],
                                                                         new_composite_path,
                                                                         generate_date_image=False,
                                                                         missing=0
                                                                         )
                latest_composite_path = new_composite_path
            '''

        tile_log.info("---------------------------------------------------------------")
        tile_log.info("---                  PROCESSING END                         ---")
        tile_log.info("---------------------------------------------------------------")


    except Exception as error:
        # this log needs to stay as the "main" log, which is `log`
        log.error(f"Could not complete ACD Raster Process for Tile  {tile_to_process}")
        log.error(f"error received   :  {error}")

def acd_integrated_vectorisation(root_dir,
                              log,
                              epsg,
                              level_1_boundaries_path,
                              conda_env_name,
                              delete_existing,
                              tilelist_filepath):
    """
    
    This function:
        - Vectorises the change report raster by calling acd_per_tile_vector for all active tiles

        - Adds two additional columns to allocate and record to support acd_national_manual_validation
        
            - "user" and "decision"

    Parameters
    ----------
    root_dir : str
        The path to the root directory of the tiles directory
    log
        The logger object
    epsg : int
        The epsg code for the spatial area
    level_1_boundaries_path : str
        The path to the geometries that are wished to categorise the spatial information by
    conda_env_name : str
        A string of the conda environment this script is running from
    delete_existing : bool
        Whether to delete existing files, or not
    tilelist_filepath : str
        A filepath of a `.csv` containing the tiles to vectorise, is used for sorting the tiles so they are vectorised in the order reported by `acd_roi_tile_intersection()`.

    Returns
    ----------
    None

    """

    import glob
    import os

    tiles_name_pattern = "[0-9][0-9][A-Z][A-Z][A-Z]"
    report_tif_pattern = "/output/probabilities/report*.tif"
    search_pattern = f"{tiles_name_pattern}{report_tif_pattern}"

    tiles_paths = glob.glob(os.path.join(root_dir, search_pattern))

    # extract tile names from file paths
    tile_names = [fp.split("/")[-3] for fp in tiles_paths]

    # check if tilelist_filepath exists
    if os.path.exists(tilelist_filepath):
        
        tilelist_df = pd.read_csv(tilelist_filepath)
        # create a categorical variable based on the order of tilelist_df
        cat_var = pd.Categorical(tile_names, categories=tilelist_df["tile"], ordered=True)

        # sort the file paths based on the order of the categorical variable
        sorted_filepaths = [fp for _, fp in sorted(zip(cat_var.codes, tiles_paths))]

        try:
            log.info(f"There are {len(sorted_filepaths)} Change Report Rasters to vectorise, these are:")

            # log the order of filepaths to vectorise
            for n, tile_path in enumerate(sorted_filepaths):
                log.info(f"{n+1}  :  {tile_path}")
            log.info("---------------------------------------------------------------")

            # vectorise per path logic
            for report in sorted_filepaths:                
                if delete_existing:
                    
                    # get list of existing report files in report path
                    log.info("delete_existing flag is set to True: deleting existing vectorised change report shapefiles, pkls and csvs")
                    directory = os.path.dirname(report)
                    report_shp_pattern = "/report*"
                    search_shp_pattern = f"{directory}{report_shp_pattern}"
                    existing_files = glob.glob(search_shp_pattern)

                    # exclude .tif files from the delete list
                    files_to_remove = [file for file in existing_files if not file.endswith(".tif")]

                    for file in files_to_remove:
                        try:
                            os.remove(file)
                        except:
                            log.error(f"Could not delete {file}, skipping")
                try:
                    vectorisation.vector_report_generation(raster_change_report_path=report,
                                                        write_csv=False,
                                                        write_pkl=True,
                                                        write_shapefile=True,
                                                        log=log,
                                                        epsg=epsg,
                                                        level_1_boundaries_path=level_1_boundaries_path,
                                                        conda_env_name=conda_env_name,
                                                        delete_intermediates=True) 
                except:
                    log.info(f"Failed to vectorise {report}, moving on to the next")

        except Exception as error:
            #log.error(f"Could not open {tilelist_filepath}")
            log.error(f"An error occurred  {error}")

    else:
        log.error(f"{tilelist_filepath} does not exist, check that you ran the acd_roi_tile_intersection beforehand")

    log.info("---------------------------------------------------------------")
    log.info("---------------------------------------------------------------")
    log.info("National Vectorisation of the Change Reports Complete")
    log.info("---------------------------------------------------------------")
    log.info("---------------------------------------------------------------")

    return

def acd_national_integration(root_dir: str,
                             log,
                             epsg: int,
                             conda_env_name: str,
                             config_dict : dict):
    
    """

    This function:
        - glob to find 1 report_xxx.pkl file per tile in output/probabilities,
        - from which to read in a Pandas DataFrame of the vectorised changes,
        - then concatenate DataFrame for each tile to form a national change event DataFrame
        - save to disc in /integrated/acd_national_integration.pkl
    
    Parameters:
    ----------
    root_dir : str
    log :
    epsg : int
    conda_env_name : str
    config_dict : dict

    Returns:
    ----------
    None

    """

    # get tile name pattern and report shapefile pattern for glob
    tiles_name_pattern = "[0-9][0-9][A-Z][A-Z][A-Z]"
    report_shp_pattern = "/output/probabilities/report*.shp"
    search_pattern = f"{tiles_name_pattern}{report_shp_pattern}"

    # glob through passed directory, return files matching the two patterns
    vectorised_paths = glob.glob(os.path.join(root_dir, search_pattern))
    
    # specify gdal and proj installation, this is geopandas'
    home = str(Path.home())
    os.environ[
        "GDAL_DATA"
    ] = f"{home}/miniconda3/envs/{conda_env_name}/lib/python3.10/site-packages/pyproj/proj_dir/share/gdal"
    os.environ[
        "PROJ_LIB"
    ] = f"{home}/miniconda3/envs/{conda_env_name}/lib/python3.10/site-packages/pyproj/proj_dir/share/proj"

    # initialise empty geodataframe
    merged_gdf = gpd.GeoDataFrame()

    try:
        # read in ROI
        roi_filepath = os.path.join(config_dict["roi_dir"], config_dict["roi_filename"])
        roi = gpd.read_file(roi_filepath)
        log.info(f"Ensuring ROI is of EPSG  :  {epsg}")
        roi = roi.to_crs(epsg)
        # for each shapefile in the list of shapefile paths, read, filter and merge
        for vector in vectorised_paths[:-1]:
            try:
                log.info(f"Reading in change report shapefile   :  {vector}")
                shape = gpd.read_file(vector)
                log.info(f"Ensuring change report shapefile is of EPSG  :  {epsg}")
                shape = shape.to_crs(epsg)
                # spatial filter intersection
                log.info(f"Intersecting {vector} with {roi_filepath}")
                intersected = shape.overlay(roi, how="intersection")

                # join the two gdfs
                merged_gdf = pd.concat([merged_gdf, intersected], ignore_index=True)
                log.info(f"Intersection: Success")
            except:
                log.error(f"failed to merge geodataframe: {vector}")
    except:
        log.error(f"Could not open ROI, is the filepath correct?  :  {roi_filepath}")

    out_path = f"{os.path.join(root_dir, 'national_geodataframe.shp')}"
    merged_gdf.to_file(filename=out_path)
    log.info(f"National GeoDataFrame written to {out_path}")

    log.info("---------------------------------------------------------------")
    log.info("---------------------------------------------------------------")
    log.info("National Integration of the Vectorised Change Reports Complete")
    log.info("---------------------------------------------------------------")
    log.info("---------------------------------------------------------------")

    return



# def acd_national_filtering():
#     """
    
#     This function:
#         - Applies filters to the national vectorised change report, as specified in the pyeo.ini
#             - The planned filters are:
#                 - Counties
#                 - Minimum Area
#                 - Date Period     

#     """
#     pass

# def acd_national_dataframe_to_shapefile():
#     """
    
#     This function:
#         - converts an event DataFrame stored in a .pkl format to a shapefile suitable for importing in QGIS
    
#     """
#     pass

# def acd_national_dataframe_to_csv():
#     """
    
#     This function:
#         - converts an event DataFrame stored in a .pkl format to a csv for Excel or a text editor
    
#     """
#     pass

# def acd_national_qgis_bookmark_generation():
#     """
    
#     This function:
#        - Generates a QGIS Project file using pyQGIS
#        - Generates QGIS Spatial Bookmarks (.xml) from a filtered dataframe for import into QGIS
#        - Import report.tif
#        - Import ROI
    
#     """
#     pass


# def acd_national_manual_validation():
#     """
    
#     This function:
#         - is a placeholder for manual validation to assess each event, flagging for on the ground observation
    
#     """
#     pass

# def acd_national_distribution():
#     """

#     This function:
#         - Once the user is happy and has verified the change alerts, Maps.Me and WhatsApp messages are sent from this function.
    
#     """
#     pass

# ############################

# def acd_per_tile_raster():
#     """
    
    
#     This function:
        
#         - queries available Sentinel-2 imagery that matches the tilelist and environmental parameters specified in the pyeo.ini file

#         - for the composite, this downloads Sentinel-2 imagery, applies atmospheric correction (if necessary), converts from .jp2 to .tif and applies cloud masking

#         - for the change images, this downloads Sentinel-2 imagery, applies atmospheric correction (if necessary), converts from .jp2 to .tif and applies cloud masking

#         - classifies the composite and change images

#         - performs change detection between the classified composite and the classified change images, searching for land cover changes specified in the from_classes and to_classes in the pyeo.ini

#         - outputs a change report raster with dates land cover change


#     """
    # create tiles folder structure
    # log.info("\nCreating the directory structure if not already present")

    pyeo_1.filesystem_utilities.create_folder_structure_for_tiles(tile_dir)



    # try:
#   # 17.04.23 uncomment the below variables when we have written the code that needs them
#         change_image_dir = os.path.join(tile_dir, r"images")
#         l1_image_dir = os.path.join(tile_dir, r"images/L1C")
#         l2_image_dir = os.path.join(tile_dir, r"images/L2A")
#         l2_masked_image_dir = os.path.join(tile_dir, r"images/cloud_masked")
#         categorised_image_dir = os.path.join(tile_dir, r"output/classified")
#         probability_image_dir = os.path.join(tile_dir, r"output/probabilities")
#         sieved_image_dir = os.path.join(tile_dir, r"output/sieved")
#         composite_dir = os.path.join(tile_dir, r"composite")
#         composite_l1_image_dir = os.path.join(tile_dir, r"composite/L1C")
#         composite_l2_image_dir = os.path.join(tile_dir, r"composite/L2A")
#         composite_l2_masked_image_dir = os.path.join(tile_dir, r"composite/cloud_masked")
#         quicklook_dir = os.path.join(tile_dir, r"output/quicklooks")

        # if arg_start_date == "LATEST":
        #     report_file_name = [f for f in os.listdir(probability_image_dir) if os.path.isfile(f) and f.startswith("report_") and f.endswith(".tif")][0]
        #     report_file_path = os.path.join(probability_image_dir, report_file_name)
        #     after_timestamp  = pyeo_1.filesystem_utilities.get_change_detection_dates(os.path.basename(report_file_path))[-1]
        #     after_timestamp.strftime("%Y%m%d") # Returns the yyyymmdd string of the acquisition date from which the latest classified image was derived
        # elif arg_start_date:
        #     start_date = arg_start_date

        # if arg_end_date == "TODAY":
        #     end_date = dt.date.today().strftime("%Y%m%d")
        # elif arg_end_date:
        #     end_date = arg_end_date

    # except:
    #     log.error("failed to initialise log")

#     pass
    
# def acd_per_tile_vector():
#     """
    
#     This function:
#         - Vectorises the change report raster

#         - Adds two additional columns to allocate and record to support acd_national_manual_validation
        
#             - "assesor" and "decision"

#     """
#     pass