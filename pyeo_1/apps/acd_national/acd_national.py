import configparser
import os
from pyeo_1 import filesystem_utilities
import sys

def acd_national(path_to_config):
    """
    This function:
        - acts as the singular call to run automatic change detection per tile, then aggregate to national, then distribute the change alerts.
    
    """
    print(path_to_config)

    config, log = acd_initialisation(path_to_config)

    log.info("---------------------------------------------------------------")
    log.info("---                  INTEGRATED PROCESSING START            ---")
    log.info("---------------------------------------------------------------")

    # acd_roi_tile_intersection()
    
    # acd_by_tile_raster()
    
    acd_by_tile_vectorisation(root_dir=config["environment"]["tile_dir"],
                               log=log,
                               epsg=config["forest_sentinel"]["epsg"],
                               level_1_boundaries_path=config["vector_processing_parameters"]["level_1_filename"],
                               conda_env_name=config["environment"]["conda_env_name"],
                               delete_existing=config["vector_processing_parameters"]["do_delete_existing_vector"])
    
    # acd_national_integration()
    
    # acd_national_filtering()
    
    # acd_national_dataframe_to_shapefile()
    
    # acd_national_manual_validation()
    
    # acd_national_distribution()
    
    log.info("---------------------------------------------------------------")
    log.info("---                  INTEGRATED PROCESSING END              ---")
    log.info("---------------------------------------------------------------")

    return
    
############################

def acd_initialisation(path_to_config):

    """
    
    This function initialises 
    """

    # read in config file
    config = configparser.ConfigParser(allow_no_value=True)
    config.read(path_to_config)
    
    # changes directory to pyeo_dir, enabling the use of relative paths from the config file
    os.chdir(config["environment"]["pyeo_dir"])

    # # create tiles folder structure
    # pyeo_1.filesystem_utilities.create_folder_structure_for_tiles(tile_root_dir)

    # initialise log file
    log = filesystem_utilities.init_log(os.path.join(config["environment"]["log_dir"], config["environment"]["log_filename"]))
    # log.info("---------------------------------------------------------------")
    # log.info("---   PROCESSING START: {}   ---".format(tile_root_dir))
    # log.info("---------------------------------------------------------------")
    # log.info("Options:")
    # if do_dev:
    #     log.info("  --dev Running in development mode, choosing development versions of functions where available")
    # else:
    #     log.info("  Running in production mode, avoiding any development versions of functions.")
    # if do_all:
    #     log.info("  --do_all")
    # if build_composite:
    #     log.info("  --build_composite for baseline composite")
    #     log.info("  --download_source = {}".format(download_source))
    # if do_download:
    #     log.info("  --download for change detection images")
    #     if not build_composite:
    #         log.info("  --download_source = {}".format(download_source))
    # if do_classify:
    #     log.info("  --classify to apply the random forest model and create classification layers")
    # if build_prob_image:
    #     log.info("  --build_prob_image to save classification probability layers")
    # if do_change:
    #     log.info("  --change to produce change detection layers and report images")
    # if do_update:
    #     log.info("  --update to update the baseline composite with new observations")
    # if do_quicklooks:
    #     log.info("  --quicklooks to create image quicklooks")
    # if do_delete:
    #     log.info("  --remove downloaded L1C images and intermediate image products")
    #     log.info("           (cloud-masked band-stacked rasters, class images, change layers) after use.")
    #     log.info("           Deletes remaining temporary directories starting with \'tmp\' from interrupted processing runs.")
    #     log.info("           Keeps only L2A images, composites and report files.")
    #     log.info("           Overrides --zip for the above files. WARNING! FILE LOSS!")
    # if do_zip:
    #     log.info("  --zip archives L2A images, and if --remove is not selected also L1C,")
    #     log.info("           cloud-masked band-stacked rasters, class images and change layers after use.")

    # log.info("List of image bands: {}".format(bands))
    # log.info("Model used: {}".format(model_path))
    # log.info("List of class labels:")
    # for c, this_class in enumerate(class_labels):
    #     log.info("  {} : {}".format(c+1, this_class))
    # log.info("Detecting changes from any of the classes: {}".format(from_classes))
    # log.info("                    to any of the classes: {}".format(to_classes))

    # log.info("\nCreating the directory structure if not already present")

    #try:
    # 17.04.23 uncomment the below variables when we have written the code that needs them
        # change_image_dir = os.path.join(tile_root_dir, r"images")
        # l1_image_dir = os.path.join(tile_root_dir, r"images/L1C")
        # l2_image_dir = os.path.join(tile_root_dir, r"images/L2A")
        # l2_masked_image_dir = os.path.join(tile_root_dir, r"images/cloud_masked")
        # categorised_image_dir = os.path.join(tile_root_dir, r"output/classified")
        # probability_image_dir = os.path.join(tile_root_dir, r"output/probabilities")
        # sieved_image_dir = os.path.join(tile_root_dir, r"output/sieved")
        # composite_dir = os.path.join(tile_root_dir, r"composite")
        # composite_l1_image_dir = os.path.join(tile_root_dir, r"composite/L1C")
        # composite_l2_image_dir = os.path.join(tile_root_dir, r"composite/L2A")
        # composite_l2_masked_image_dir = os.path.join(tile_root_dir, r"composite/cloud_masked")
        # quicklook_dir = os.path.join(tile_root_dir, r"output/quicklooks")

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

    #except:
        #log.error("failed to initialise log")

    return config, log

# def acd_roi_tile_intersection():
#     """
    
#     This function:
#         - accepts a Region of Interest (RoI) and writes a tilelist.txt of the Sentinel-2 tiles that the RoI covers.
    
#         - the tilelist.txt is then used to perform the tile-based processes, raster and vector.

#     """
#     pass

# def acd_by_tile_raster():
#     """
    
#     This function:
#         - sequentially calls acd_per_tile_raster for all active tiles and waits for completion/failure 
    
#     """
#     pass

# def acd_by_tile_vectorisation(root_dir,
#                                log,
#                                epsg,
#                                level_1_boundaries_path,
#                                conda_env_name,
#                                delete_existing):
#     """
    
#     This function:
#         - Vectorises the change report raster by calling acd_per_tile_vector for all active tiles

#         - Adds two additional columns to allocate and record to support acd_national_manual_validation
        
#             - "user" and "decision"

#     ------------
#     Parameters

#     root_dir (str):
#     log
#     epsg (int):
#     level_1_boundaries_path (str):
#     conda_env_name (str):
#     delete_existing (bool):

#     ----------
#     Returns
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

#     return

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
#     pass
    
# def acd_per_tile_vector():
#     """
    
#     This function:
#         - Vectorises the change report raster

#         - Adds two additional columns to allocate and record to support acd_national_manual_validation
        
#             - "assesor" and "decision"

#     """
#     pass

if __name__ == "__main__":

    # for arg in sys.argv[1:]:
    #     print(arg)

    acd_national(sys.argv[1:])