import configparser
import json
import os
import shutil
from pathlib import Path
from pyeo_1 import filesystem_utilities
from pyeo_1 import vectorisation
from pyeo_1 import queries_and_downloads
from pyeo_1 import raster_manipulation
import geopandas as gpd
import pandas as pd
import numpy as np

# acd_national is the top-level function which controls the raster and vector processes for pyeo_1
def automatic_change_detection_national(path_to_config):

    """
    This function:
        - acts as the singular call to run automatic change detection per tile, then aggregate to national, then distribute the change alerts.

    ------------
    Parameters:

    path_to_config (str):
        The path to the config file, which is an .ini

    ------------
    Returns:

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

    # acd_integrated_vectorisation(root_dir=config_dict["tile_dir"],
    #                           log=acd_log,
    #                           epsg=config_dict["epsg"],
    #                           level_1_boundaries_path=config_dict["level_1_boundaries_path"],
    #                           conda_env_name=config_dict["conda_env_name"],
    #                           delete_existing=config_dict["do_delete_existing_vector"],
    #                           tilelist_filepath=tilelist_filepath)
    
    acd_log.info("---------------------------------------------------------------")
    acd_log.info("Starting acd_national_integration")
    acd_log.info("---------------------------------------------------------------")
    # acd_national_integration()
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

    ------------
    Parameters:

    path_to_config (str):
        The path to the config file, which is an .ini

    Returns:

    config
        A config dictionary
    log
        A log object
    ------------
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

    config_dict["do_dev"] = config["raster_processing_parameters"]["do_dev"]
    config_dict["do_all"] = config["raster_processing_parameters"]["do_all"]
    config_dict["do_download"] = config["raster_processing_parameters"]["do_download"]
    config_dict["do_classify"] = config["raster_processing_parameters"]["do_classify"]
    config_dict["do_change"] = config["raster_processing_parameters"]["do_change"]
    config_dict["do_update"] = config["raster_processing_parameters"]["do_update"]
    config_dict["do_quicklooks"] = config["raster_processing_parameters"]["do_quicklooks"]
    config_dict["do_delete"]= config["raster_processing_parameters"]["do_delete"]
    config_dict["do_zip"] = config["raster_processing_parameters"]["do_zip"]
    config_dict["build_composite"] = config["raster_processing_parameters"]["do_build_composite"]
    config_dict["build_prob_image"] = config["raster_processing_parameters"]["do_build_prob_image"]
    config_dict["do_skip_existing"] = config["raster_processing_parameters"]["do_skip_existing"]

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
    config_dict["do_delete_existing_vector"] = config["vector_processing_parameters"]["do_delete_existing_vector"]
    config_dict["credentials_path"] = config["environment"]["credentials_path"]

    return config_dict, log

def acd_config_to_log(config_dict: dict, log):

    """
    This function echoes the contents of config_dict to the log file.
    It does not return anything.

    ------------
    Parameters:

    config_dict:
        config_dict variable
    log:
        log variable
    
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
        log.info(f"  composite start date  : {config_dict['start_date']}")
        log.info(f"  composite end date  : {config_dict['end_date']}")
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
        log.info(f"  change start date  : {config_dict['composite_start']}")
        log.info(f"  change end date  : {config_dict['composite_end']}")
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

    ------------
    Parameters

    config_dict (dict)
        Dictionary of the Configuration Parameters specified in pyeo_1.ini
    log
        Log variable

    ------------
    Returns

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

    ------------
    Parameters

    config_dict (dict)
        Dictionary of the Configuration Parameters specified in pyeo_1.ini
    log
        Logger object
    tilelist_filepath (str)
        Filepath of a .csv containing the list of tiles on which to perform raster processes
    
    ------------
    Returns

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
        log.info("Creating the directory paths")
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
            log.info("---------------------------------------------------------------")
            log.info("Creating an initial cloud-free median composite from Sentinel-2 as a baseline map")
            log.info("---------------------------------------------------------------")
            log.info("Searching for images for initial composite.")
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
                log.error(f"check_for_s2_data_by_date failed, got this error :  {error}")
                
            log.info("--> Found {} L1C and L2A products for the composite:".format(len(composite_products_all)))
            df_all = pd.DataFrame.from_dict(composite_products_all, orient='index')

            # check granule sizes on the server
            df_all['size'] = df_all['size'].str.split(' ').apply(lambda x: float(x[0]) * {'GB': 1e3, 'MB': 1, 'KB': 1e-3}[x[1]])
            df = df_all.query('size >= '+str(faulty_granule_threshold))
            log.info("Removed {} faulty scenes <{}MB in size from the list:".format(len(df_all)-len(df), faulty_granule_threshold))
            df_faulty = df_all.query('size < '+str(faulty_granule_threshold))
            for r in range(len(df_faulty)):
                log.info("   {} MB: {}".format(df_faulty.iloc[r,:]['size'], df_faulty.iloc[r,:]['title']))

            l1c_products = df[df.processinglevel == 'Level-1C']
            l2a_products = df[df.processinglevel == 'Level-2A']
            log.info("    {} L1C products".format(l1c_products.shape[0]))
            log.info("    {} L2A products".format(l2a_products.shape[0]))

            rel_orbits = np.unique(l1c_products['relativeorbitnumber'])
            if len(rel_orbits) > 0:
                if l1c_products.shape[0] > max_image_number / len(rel_orbits):
                    log.info("Capping the number of L1C products to {}".format(max_image_number))
                    log.info("Relative orbits found covering tile: {}".format(rel_orbits))
                    uuids = []
                    for orb in rel_orbits:
                        uuids = uuids + list(l1c_products.loc[l1c_products['relativeorbitnumber'] == orb].sort_values(by=['cloudcoverpercentage'], ascending=True)['uuid'][:int(max_image_number/len(rel_orbits))])
                    l1c_products = l1c_products[l1c_products['uuid'].isin(uuids)]
                    log.info("    {} L1C products remain:".format(l1c_products.shape[0]))
                    for product in l1c_products['title']:
                        log.info("       {}".format(product))

            rel_orbits = np.unique(l2a_products['relativeorbitnumber'])
            if len(rel_orbits) > 0:
                if l2a_products.shape[0] > max_image_number/len(rel_orbits):
                    log.info("Capping the number of L2A products to {}".format(max_image_number))
                    log.info("Relative orbits found covering tile: {}".format(rel_orbits))
                    uuids = []
                    for orb in rel_orbits:
                        uuids = uuids + list(l2a_products.loc[l2a_products['relativeorbitnumber'] == orb].sort_values(by=['cloudcoverpercentage'], ascending=True)['uuid'][:int(max_image_number/len(rel_orbits))])
                    l2a_products = l2a_products[l2a_products['uuid'].isin(uuids)]
                    log.info("    {} L2A products remain:".format(l2a_products.shape[0]))
                    for product in l2a_products['title']:
                        log.info("       {}".format(product))

            if l1c_products.shape[0]>0 and l2a_products.shape[0]>0:
                log.info("Filtering out L1C products that have the same 'beginposition' time stamp as an existing L2A product.")
                l1c_products, l2a_products = queries_and_downloads.filter_unique_l1c_and_l2a_data(df)
                log.info("--> {} L1C and L2A products with unique 'beginposition' time stamp for the composite:".format(l1c_products.shape[0]+l2a_products.shape[0]))
                log.info("    {} L1C products".format(l1c_products.shape[0]))
                log.info("    {} L2A products".format(l2a_products.shape[0]))
            df = None

            # Search the composite/L2A and L1C directories whether the scenes have already been downloaded and/or processed and check their dir sizes
            if l1c_products.shape[0] > 0:
                log.info("Checking for already downloaded and zipped L1C or L2A products and")
                log.info("  availability of matching L2A products for download.")
                n = len(l1c_products)
                drop=[]
                add=[]
                for r in range(n):
                    id = l1c_products.iloc[r,:]['title']
                    search_term = id.split("_")[2]+"_"+id.split("_")[3]+"_"+id.split("_")[4]+"_"+id.split("_")[5]
                    log.info("Searching locally for file names containing: {}.".format(search_term))
                    file_list = [os.path.join(composite_l1_image_dir, f) for f in os.listdir(composite_l1_image_dir)] + \
                        [os.path.join(composite_l2_image_dir, f) for f in os.listdir(composite_l2_image_dir)] + \
                        [os.path.join(composite_l2_masked_image_dir, f) for f in os.listdir(composite_l2_masked_image_dir)]
                    for f in file_list:
                        if search_term in f:
                            log.info("  Product already downloaded: {}".format(f))
                            drop.append(l1c_products.index[r])
                    search_term = "*"+id.split("_")[2]+"_"+id.split("_")[3]+"_"+id.split("_")[4]+"_"+id.split("_")[5]+"*"
                    log.info("Searching on the data hub for files containing: {}.".format(search_term))
                    matching_l2a_products = queries_and_downloads._file_api_query(user=sen_user,
                                                                                  passwd=sen_pass,
                                                                                  start_date=composite_start_date,end_date=composite_end_date,filename=search_term,cloud=cloud_cover,producttype="S2MSI2A")

                    matching_l2a_products_df = pd.DataFrame.from_dict(matching_l2a_products, orient='index')
                    # 07/03/2023: Matt - Applied Ali's fix for converting product size to MB to compare against faulty_grandule_threshold
                    if len(matching_l2a_products_df) == 1 and [float(x[0]) * {'GB': 1e3, 'MB': 1, 'KB': 1e-3}[x[1]] for x in [matching_l2a_products_df['size'][0].split(' ')]][0] > faulty_granule_threshold:
                        log.info("Replacing L1C {} with L2A product:".format(id))
                        log.info("              {}".format(matching_l2a_products_df.iloc[0,:]['title']))
                        drop.append(l1c_products.index[r])
                        add.append(matching_l2a_products_df.iloc[0,:])
                    if len(matching_l2a_products_df) == 0:
                        pass
                    if len(matching_l2a_products_df) > 1:
                        # check granule sizes on the server
                        matching_l2a_products_df['size'] = matching_l2a_products_df['size'].str.split(' ').apply(lambda x: float(x[0]) * {'GB': 1e3, 'MB': 1, 'KB': 1e-3}[x[1]])
                        matching_l2a_products_df = matching_l2a_products_df.query('size >= '+str(faulty_granule_threshold))
                        if matching_l2a_products_df.iloc[0,:]['size'].str.split(' ').apply(lambda x: float(x[0]) * {'GB': 1e3, 'MB': 1, 'KB': 1e-3}[x[1]]) > faulty_granule_threshold:
                            log.info("Replacing L1C {} with L2A product:".format(id))
                            log.info("              {}".format(matching_l2a_products_df.iloc[0,:]['title']))
                            drop.append(l1c_products.index[r])
                            add.append(matching_l2a_products_df.iloc[0,:])
                if len(drop) > 0:
                    l1c_products = l1c_products.drop(index=drop)
                if len(add) > 0:
                    l2a_products = l2a_products.append(add)
                l2a_products = l2a_products.drop_duplicates(subset='title')
                log.info("    {} L1C products remaining for download".format(l1c_products.shape[0]))
                log.info("    {} L2A products remaining for download".format(l2a_products.shape[0]))    


            # if L1C products remain after matching for L2As, then download the unmatched L1Cs
            if l1c_products.shape[0] > 0:
                log.info("Downloading Sentinel-2 L1C products.")
                        
                queries_and_downloads.download_s2_data_from_df(l1c_products,
                                                                    composite_l1_image_dir,
                                                                    composite_l2_image_dir,
                                                                    source="scihub",
                                                                    user=sen_user,
                                                                    passwd=sen_pass,
                                                                    try_scihub_on_fail=True)
                log.info("Atmospheric correction with sen2cor.")
                raster_manipulation.atmospheric_correction(composite_l1_image_dir,
                                                           composite_l2_image_dir,
                                                           sen2cor_path,
                                                           delete_unprocessed_image=False)
            log.info(f"download source is   {download_source}")
            if l2a_products.shape[0] > 0:
                log.info("Downloading Sentinel-2 L2A products.")
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
                        log.warning("Found likely incomplete download of size {} MB: {}".format(str(round(sizes[index]/1024/1024)), safe_dir))
                        #shutil.rmtree(safe_dir)
            
            log.info("---------------------------------------------------------------")
            log.info("Image download and atmospheric correction for composite is complete.")
            log.info("---------------------------------------------------------------")

            if config_dict["do_delete"]:
                log.info("---------------------------------------------------------------")
                log.info("Deleting downloaded L1C images for composite, keeping only derived L2A products")
                log.info("---------------------------------------------------------------")
                directory = composite_l1_image_dir
                log.info('Deleting {}'.format(directory))
                shutil.rmtree(directory)
                log.info("---------------------------------------------------------------")
                log.info("Deletion of L1C images complete. Keeping only L2A images.")
                log.info("---------------------------------------------------------------")
            else:
                if config_dict["do_zip"]:
                    log.info("---------------------------------------------------------------")
                    log.info("Zipping downloaded L1C images for composite after atmospheric correction")
                    log.info("---------------------------------------------------------------")
                    filesystem_utilities.zip_contents(composite_l1_image_dir)
                    log.info("---------------------------------------------------------------")
                    log.info("Zipping complete")
                    log.info("---------------------------------------------------------------")

            log.info("---------------------------------------------------------------")
            log.info("Applying simple cloud, cloud shadow and haze mask based on SCL files and stacking the masked band raster files.")
            log.info("---------------------------------------------------------------")

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
                log.info("No L2A images found for cloud masking. They may already have been done.")
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


    except Exception as error:
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

    ------------
    Parameters

    root_dir (str):
        The path to the root directory of the tiles directory
    log
        The log object
    epsg (int):
        The epsg code for the spatial area
    level_1_boundaries_path (str):
        The path to the geometries that are wished to categorise the spatial information by
    conda_env_name (str):
        A string of the conda environment this script is running from
    delete_existing (bool):
        Whether to delete existing files, or not
    tilelist_filepath (str)
        A filepath of a `.csv` containing the tiles to vectorise, is used for sorting the tiles so they are vectorised in the order reported by `acd_roi_tile_intersection()`.

    ----------
    Returns
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
            log.info("--"*30)

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

    log.info("--"*30)
    log.info("--"*30)
    log.info("National Vectorisation of the Change Reports Complete")
    log.info("--"*30)
    log.info("--"*30)

    return

## I'm not sure how acd_national_vectorisation is different to acd_by_tile_vectorisation (Matt 28/04/23)
# def acd_national_vectorisation(root_dir,
#                                log,
#                                epsg,
#                                level_1_boundaries_path,
#                                conda_env_name,
#                                delete_existing):
#     """
#     This function:
#         - Globs through the tile outputs, running acd_by_tile_vectorisation()
  
    
#     """

#     import glob
#     import os

#     tiles_name_pattern = "[0-9][0-9][A-Z][A-Z][A-Z]"
#     report_tif_pattern = "/output/probabilities/report*.tif"
#     search_pattern = f"{tiles_name_pattern}{report_tif_pattern}"

#     tiles_paths = glob.glob(os.path.join(root_dir, search_pattern))
    
#     log.info("Tiles to vectorise the change report rasters of:  ")
#     log.info(tiles_paths)
#     log.info("--"*20)
    
#     for report in tiles_paths:
#         if delete_existing:
#             # get list of existing report files in report path
#             log.info("delete_existing flag is set to True: deleting existing vectorised change report shapefiles, pkls and csvs")
#             directory = os.path.dirname(report)
#             report_shp_pattern = "/report*"
#             search_shp_pattern = f"{directory}{report_shp_pattern}"
#             existing_files = glob.glob(search_shp_pattern)

#             # exclude .tif files from the delete list
#             files_to_remove = [file for file in existing_files if not file.endswith(".tif")]

#             for file in files_to_remove:
#                 try:
#                     os.remove(file)
#                 except:
#                     log.error(f"Could not delete {file}, skipping")
#         try:
#             vectorisation.vector_report_generation(raster_change_report_path=report,
#                                                 write_csv=False,
#                                                 write_pkl=True,
#                                                 write_shapefile=True,
#                                                 log=log,
#                                                 epsg=epsg,
#                                                 level_1_boundaries_path=level_1_boundaries_path,
#                                                 conda_env_name=conda_env_name,
#                                                 delete_intermediates=True) 
#         except:
#             log.info(f"Failed to vectorise {report}, moving on to the next")

#     log.info("--"*20)
#     log.info("--"*20)
#     log.info("National Vectorisation of the Change Reports Complete")
#     log.info("--"*20)
#     log.info("--"*20)

#     return tiles_paths

# def acd_national_integration():
#     """

#     This function:
#         - glob to find 1 report_xxx.pkl file per tile in output/probabilities,
#         - from which to read in a Pandas DataFrame of the vectorised changes,
#         - then concatenate DataFrame for each tile to form a national change event DataFrame
#         - save to disc in /integrated/acd_national_integration.pkl
       
#     """
#     pass

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