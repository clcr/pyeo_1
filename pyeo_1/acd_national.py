import configparser
import json
import os
from pyeo_1 import filesystem_utilities
from pyeo_1 import vectorisation

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

    config_dict, log = acd_initialisation(path_to_config)

    # echo configuration to log
    acd_config_to_log(config_dict, log)


    # acd_roi_tile_intersection()
    
    # acd_by_tile_raster(config_dict)
    
    log.info("---------------------------------------------------------------")
    log.info("Starting acd_by_tile_vectorisation(), vectorising each change report raster, by tile")
    log.info("---------------------------------------------------------------")
    acd_by_tile_vectorisation(root_dir=config_dict["tile_dir"],
                              log=log,
                              epsg=config_dict["epsg"],
                              level_1_boundaries_path=config_dict["level_1_boundaries_path"],
                              conda_env_name=config_dict["conda_env_name"],
                              delete_existing=config_dict["do_delete_existing_vector"])
    
    # acd_national_integration()
    
    # acd_national_filtering()
    
    # acd_national_dataframe_to_shapefile()
    
    # acd_national_manual_validation()
    
    # acd_national_distribution()
    
    log.info("---------------------------------------------------------------")
    log.info("---                  INTEGRATED PROCESSING END              ---")
    log.info("---------------------------------------------------------------")

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
    log = filesystem_utilities.init_log(os.path.join(config["environment"]["log_dir"], config["environment"]["log_filename"]))


    log.info("---------------------------------------------------------------")
    log.info("---                  INTEGRATED PROCESSING START            ---")
    log.info("---------------------------------------------------------------")

    log.info("Reading in parameters defined in the Config")
    log.info("---------------------------------------------------------------")

    config_dict["do_dev"] = bool(config["raster_processing_parameters"]["do_dev"])
    config_dict["do_all"] = bool(config["raster_processing_parameters"]["do_all"])
    config_dict["do_download"] = bool(config["raster_processing_parameters"]["do_download"])
    config_dict["do_classify"] = bool(config["raster_processing_parameters"]["do_classify"])
    config_dict["do_change"] = bool(config["raster_processing_parameters"]["do_change"])
    config_dict["do_update"] = bool(config["raster_processing_parameters"]["do_update"])
    config_dict["do_quicklooks"] = bool(config["raster_processing_parameters"]["do_quicklooks"])
    config_dict["do_delete"]= bool(config["raster_processing_parameters"]["do_delete"])
    config_dict["do_zip"] = bool(config["raster_processing_parameters"]["do_zip"])
    
    config_dict["download_source"] = bool(config["raster_processing_parameters"]["download_source"])
    config_dict["build_composite"] = bool(config["raster_processing_parameters"]["do_build_composite"])
    config_dict["build_prob_image"] = bool(config["raster_processing_parameters"]["do_build_prob_image"])

    config_dict["bands"] = json.loads(config["raster_processing_parameters"]["band_names"])
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

    config_dict["model_path"] = config["forest_sentinel"]["model"]
    config_dict["epsg"] = int(config["forest_sentinel"]["epsg"])

    config_dict["level_1_filename"] = config["vector_processing_parameters"]["level_1_filename"]
    config_dict["level_1_boundaries_path"] = os.path.join(config_dict["geometry_dir"], config_dict["level_1_filename"])
    config_dict["do_delete_existing_vector"] = bool(config["vector_processing_parameters"]["do_delete_existing_vector"])

    return config_dict, log

def acd_config_to_log(config_dict, log):

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

    # end of function


def acd_roi_tile_intersection():
    """
    
    This function:
        - accepts a Region of Interest (RoI) and writes a tilelist.txt of the Sentinel-2 tiles that the RoI covers.
    
        - the tilelist.txt is then used to perform the tile-based processes, raster and vector.

    """

    pass

# def acd_by_tile_raster():
#     """
#   
#     This function:
#
#         - checks whether tilelist.txt exists before running acd_per_tile_raster
#
#         - sequentially calls acd_per_tile_raster for all active tiles and waits for completion/failure 
#
#     """
#    pass

def acd_by_tile_vectorisation(root_dir,
                              log,
                              epsg,
                              level_1_boundaries_path,
                              conda_env_name,
                              delete_existing):
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

    ----------
    Returns
    """

    import glob
    import os

    tiles_name_pattern = "[0-9][0-9][A-Z][A-Z][A-Z]"
    report_tif_pattern = "/output/probabilities/report*.tif"
    search_pattern = f"{tiles_name_pattern}{report_tif_pattern}"

    tiles_paths = glob.glob(os.path.join(root_dir, search_pattern))
    
    log.info("Change Report Rasters to vectorise:  ")
    for n, tile_path in enumerate(tiles_paths):
        log.info(f"{n+1}  :  {tile_path}")
    log.info("--"*30)
    
    for report in tiles_paths:
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