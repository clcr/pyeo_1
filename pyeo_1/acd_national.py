import configparser
import os
import subprocess
import sys
import glob
import logging
from pathlib import Path
from pyeo_1 import filesystem_utilities
from pyeo_1.apps.acd_national import acd_by_tile_raster
from pyeo_1.apps.acd_national import acd_by_tile_vectorisation
import geopandas as gpd
import pandas as pd
from tempfile import TemporaryDirectory

# acd_national is the top-level function which controls the raster and vector processes for pyeo_1
def automatic_change_detection_national(config_path):

    """
    This function:
        - acts as the singular call to run automatic change detection per tile, then aggregate to national, then distribute the change alerts.

    Parameters
    ----------
    config_path : str
        The path to the config file, which is an .ini

    Returns
    ----------
    None

    """

    # starting acd_initialisation()
    config_dict, acd_log = acd_initialisation(config_path)

    acd_log.info("---------------------------------------------------------------")
    acd_log.info("Starting acd_to_config_to_log()")
    acd_log.info("---------------------------------------------------------------")

    # echo configuration to log
    acd_config_to_log(config_dict, acd_log)

    acd_log.info("---------------------------------------------------------------")
    acd_log.info("Starting acd_roi_tile_intersection()")
    acd_log.info("---------------------------------------------------------------")
    tilelist_filepath = acd_roi_tile_intersection(config_dict, acd_log)

    if config_dict["do_raster"]:
        acd_log.info("---------------------------------------------------------------")
        acd_log.info("Starting acd_integrated_raster():")
        acd_log.info("---------------------------------------------------------------")

        acd_integrated_raster(
            config_dict,
            acd_log,
            tilelist_filepath,
            config_path
            )

    # and skip already existing vectors
    if config_dict["do_vectorise"]:

        acd_log.info("---------------------------------------------------------------")
        acd_log.info("Starting acd_integrated_vectorisation()")
        acd_log.info("  vectorising each change report raster, by tile")
        acd_log.info("---------------------------------------------------------------")

        acd_integrated_vectorisation(
            log=acd_log,
            tilelist_filepath=tilelist_filepath,
            config_path=config_path
        )

    if config_dict["do_integrate"]:
            
        acd_log.info("---------------------------------------------------------------")
        acd_log.info("Starting acd_national_integration")
        acd_log.info("---------------------------------------------------------------")

        acd_national_integration(
            root_dir=config_dict["tile_dir"],
            log=acd_log,
            epsg=config_dict["epsg"],
            conda_env_name=config_dict["conda_env_name"],
            config_dict=config_dict,
        )

    acd_log.info("---------------------------------------------------------------")
    acd_log.info("Starting acd_national_filtering")
    acd_log.info("---------------------------------------------------------------")
    if config_dict["counties_of_interest"]:
        acd_national_filtering(log=acd_log,
                               config_dict=config_dict)

    # acd_log.info("---------------------------------------------------------------")
    # acd_log.info("Starting acd_national_distribution()")
    # acd_log.info("---------------------------------------------------------------")
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


def acd_initialisation(config_path):

    """

    This function initialises the log.txt, making the log object available

    Parameters
    ----------
    config_path : str
        The path to the config file, which is an .ini

    Returns
    ----------
    dict : config
        A config dictionary
    log : logging.Logger
        A log object

    """

    # build dictionary of configuration parameters
    config_dict = filesystem_utilities.config_path_to_config_dict(config_path)

    # changes directory to pyeo_dir, enabling the use of relative paths from the config file
    os.chdir(config_dict["pyeo_dir"])

    # initialise log file
    log = filesystem_utilities.init_log_acd(
        log_path=os.path.join(
            config_dict["log_dir"], config_dict["log_filename"]
        ),
        logger_name="pyeo_1_acd_log",
    )

    log.info("---------------------------------------------------------------")
    log.info("---                  INTEGRATED PROCESSING START            ---")
    log.info("---------------------------------------------------------------")

    log.info("Reading in parameters defined in the Config")
    log.info("---------------------------------------------------------------")
  
    
    return config_dict, log


def acd_config_to_log(config_dict: dict, log: logging.Logger):

    """
    This function echoes the contents of config_dict to the log file. \n
    It does not return anything.

    Parameters
    ----------

    config_dict : dict
        config_dict variable
    log : logging.Logger
        log variable

    Returns
    ----------

    None

    """

    log.info("Options:")
    if config_dict["do_parallel"]:
        log.info("  --do_parallel")
        log.info("        running in parallel mode, parallel functions will be enabled where available")

    if config_dict["do_dev"]:
        log.info(
            "  --dev Running in development mode, choosing development versions of functions where available"
        )
    else:
        log.info(
            "  Running in production mode, avoiding any development versions of functions."
        )
    if config_dict["do_raster"]:
        log.info("  --do_raster")
        log.info("      raster pipeline enabled")
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
            log.info(
                "  --classify to apply the random forest model and create classification layers"
            )
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
            log.info(
                "           (cloud-masked band-stacked rasters, class images, change layers) after use."
            )
            log.info(
                "           Deletes remaining temporary directories starting with 'tmp' from interrupted processing runs."
            )
            log.info("           Keeps only L2A images, composites and report files.")
            log.info("           Overrides --zip for the above files. WARNING! FILE LOSS!")
        if config_dict["do_zip"]:
            log.info(
                "  --zip archives L2A images, and if --remove is not selected also L1C,"
            )
            log.info(
                "           cloud-masked band-stacked rasters, class images and change layers after use."
            )
    if config_dict["do_delete_existing_vector"]:
        log.info(
            "  --do_delete_existing_vector , when vectorising the change report rasters, "
        )
        log.info(
            "            existing vectors files will be deleted and new vector files created."
        )
    if config_dict["do_vectorise"]:
        log.info("  --do_vectorise")
        log.info("      raster change reports will be vectorised")

    if config_dict["do_integrate"]:
        log.info("  --do_integrate")
        log.info("      vectorised reports will be merged together")

    if config_dict["counties_of_interest"]:
        log.info("  --counties_of_interest")
        log.info("        Counties to filter the national geodataframe:")
        for n, county in enumerate(config_dict["counties_of_interest"]):
            log.info(f"        {n}  :  {county}")

        log.info("  --minimum_area_to_report_m2")
        log.info(f"       Only Change Detections > {config_dict['minimum_area_to_report_m2']} metres squared will be reported")
    # reporting more parameters
    log.info(f"EPSG used is: {config_dict['epsg']}")
    log.info(f"List of image bands: {config_dict['bands']}")
    log.info(f"Model used: {config_dict['model_path']}")
    log.info(f"")
    log.info("List of class labels:")
    for c, this_class in enumerate(config_dict["class_labels"]):
        log.info("  {} : {}".format(c + 1, this_class))
    log.info(
        f"Detecting changes from any of the classes: {config_dict['from_classes']}"
    )
    log.info(f"                    to any of the classes: {config_dict['to_classes']}")
    log.info("--" * 30)
    log.info("Reporting Directories and Filepaths")
    log.info(f"Tile Directory is   : {config_dict['tile_dir']}")
    log.info(f"Working Directory is   : {config_dict['pyeo_dir']}")
    log.info(
        "The following directories are relative to the working directory, i.e. stored underneath:"
    )
    log.info(f"Integrated Directory is   : {config_dict['integrated_dir']}")
    log.info(f"ROI Directory is   : {config_dict['roi_dir']}")
    log.info(f"Geometry Directory is   : {config_dict['geometry_dir']}")
    log.info(
        f"Path to the Administrative Boundaries used in the Change Report Vectorisation   : {config_dict['level_1_boundaries_path']}"
    )
    log.info(f"Path to Sen2Cor is   : {config_dict['sen2cor_path']}")
    log.info(
        f"The Conda Environment which was provided in .ini file is :  {config_dict['conda_env_name']}"
    )
    log.info("-------------------------------------------")
    # log.info("Streaming config parameters to log file for reference")
    # # todo: for everything in config_dict, log the parameter
    # for each_section in config.sections():
    #     log.info(f"{each_section}")
    #     for (each_key, each_val) in config.items(each_section):
    #         log.info(f"     {each_key} :  {each_val}")
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
    os.environ[
        "GDAL_DATA"
    ] = f"{home}/miniconda3/envs/{conda_env_name}/lib/python3.10/site-packages/pyproj/proj_dir/share/gdal"
    os.environ[
        "PROJ_LIB"
    ] = f"{home}/miniconda3/envs/{conda_env_name}/lib/python3.10/site-packages/pyproj/proj_dir/share/proj"

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
        log.info("  {} : {}".format(n + 1, this_tile))

    tilelist_filepath = os.path.join(config_dict["roi_dir"], "tilelist.csv")
    log.info(
        f"Writing Sentinel-2 tiles that intersect with the provided ROI to  : {tilelist_filepath}"
    )

    try:
        tiles_list_df = pd.DataFrame({"tile": tiles_list})
        tiles_list_df.to_csv(tilelist_filepath, header=True, index=False)
    except:
        log.error(f"Could not write to {tilelist_filepath}")
    log.info("Finished ROI tile intersection")

    # "reset" gdal and proj installation back to default (which is GDAL's GDAL and PROJ_LIB installation)
    home = str(Path.home())
    os.environ["GDAL_DATA"] = f"{home}/miniconda3/envs/{conda_env_name}/share/gdal"
    os.environ["PROJ_LIB"] = f"{home}/miniconda3/envs/{conda_env_name}/share/proj"

    return tilelist_filepath


def acd_integrated_raster(config_dict: dict, log: logging.Logger, tilelist_filepath: str, config_path: str):

    """

    This function:

        - checks whether tilelist.csv exists before running acd_by_tile_raster for each tile

        - calls acd_by_tile_raster for all active tiles

    Parameters
    ----------
    config_dict : dict
        Dictionary of the Configuration Parameters specified in pyeo_1.ini
    log : logging.Logger
        Logger object
    tilelist_filepath : str
        Filepath of a .csv containing the list of tiles on which to perform raster processes
    config_path : str
        filepath of the config (pyeo_1.ini) for `acd_by_tile_raster`, this is present to enable the parallel processing option.

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
        log.error(
            f"{tilelist_filepath} does not exist, check that you ran the acd_roi_tile_intersection beforehand"
        )

    # check and read in credentials for downloading Sentinel-2 data
    credentials_path = config_dict["credentials_path"]
    if os.path.exists(credentials_path):
        try:
            conf = configparser.ConfigParser(allow_no_value=True)
            conf.read(credentials_path)
            credentials_dict = {}
            credentials_dict["sent_2"] = {}
            credentials_dict["sent_2"]["user"] = conf["sent_2"]["user"]
            credentials_dict["sent_2"]["pass"] = conf["sent_2"]["pass"]
        except:
            log.error(f"Could not open {credentials_path}")
    else:
        log.error(
            f"{credentials_path} does not exist, did you write the correct filepath in pyeo_1.ini?"
        )

    # check for and create tile directory, which will hold the tiles
    tile_directory = config_dict["tile_dir"]
    if not os.path.exists(tile_directory):
        log.info(
            f"The following directory for tile_dir did not exist, creating it at: {tile_directory}"
        )
        os.makedirs(tile_directory)
    else:
        pass

    ######## run acd_by_tile_raster
    for _, tile in tilelist_df.iterrows():
        #try:
        log.info(f"Starting ACD Raster Processes for Tile :  {tile[0]}")

        if not config_dict["do_parallel"]:
            acd_by_tile_raster.acd_by_tile_raster(config_path, tile[0])

        if config_dict["do_parallel"]:
            # parallel branch
            # call subprocess to call parallel shell script
            # shell_script = config_dict["parallel_shell_script"]
            subprocess.run(shell_script)
        # acd_by_tile_raster(
        #     config_dict=config_dict,
        #     log=log,
        #     tile_directory_path=tile_directory,
        #     tile_to_process=tile[0],
        #     credentials_dict=credentials_dict,
        #     config=config
        # )
        log.info(f"Finished ACD Raster Processes for Tile :  {tile[0]}")
        # except:
        #     log.error(f"Could not complete ACD Raster Processes for Tile: {tile[0]}")


def acd_integrated_vectorisation(
    log: logging.Logger,
    tilelist_filepath: str,
    config_path: str
):
    """

    This function:
        - Vectorises the change report raster by calling acd_by_tile_vectorisation for all active tiles

    Parameters
    ----------
    log : logging.Logger
        The logger object
    tilelist_filepath : str
        A filepath of a `.csv` containing the tiles to vectorise, is used for sorting the tiles so they are vectorised in the order reported by `acd_roi_tile_intersection()`.
    config_path : str
        path to pyeo_1.ini


    Returns
    -------
    None

    """

    import glob
    import os

    config_dict = filesystem_utilities.config_path_to_config_dict(config_path=config_path)

    # check if tilelist_filepath exists, open if it does, exit if it doesn't
    if os.path.exists(tilelist_filepath):
        try:
            tilelist_df = pd.read_csv(tilelist_filepath)
        except:
            log.error(f"Could not open {tilelist_filepath}")
    else:
        log.error(
            f"{tilelist_filepath} does not exist, check that you ran the acd_roi_tile_intersection beforehand"
        )
        log.error("exiting pipeline")
        sys.exit(1)
   
    # get all report.tif that are within the root_dir with search pattern
    tiles_name_pattern = "[0-9][0-9][A-Z][A-Z][A-Z]"
    report_tif_pattern = "/output/probabilities/report*.tif"
    search_pattern = f"{tiles_name_pattern}{report_tif_pattern}"
    
    tiles_paths = glob.glob(os.path.join(config_dict["tile_dir"], search_pattern))

    # only keep filepaths which match tilelist
    matching_filepaths = []

    for filepath in tiles_paths:
        if tilelist_df["tile"].str.contains(filepath.split("/")[-1].split("_")[2]).any():
            matching_filepaths.append(filepath)

    # sort filepaths in ascending order
    sorted_filepaths = sorted(matching_filepaths)
    if len(sorted_filepaths) == 0:
        log.error("there are no change reports to vectorise, here are some pointers:")
        log.error("    Ensure the raster processing pipeline has successfully ran and completed ")
        log.error("    Ensure tile_dir has been specified correctly in pyeo_1.ini")
        log.error("Now exiting the vector pipeline")
        sys.exit(1)

    log.info(f"There are {len(sorted_filepaths)} Change Report Rasters to vectorise, these are:")
    
    # log the filepaths to vectorise
    for n, tile_path in enumerate(sorted_filepaths):
        log.info(f"{n+1}  :  {tile_path}")
        log.info("---------------------------------------------------------------")

    # vectorise per path logic
    for report in sorted_filepaths:

        if config_dict["do_delete_existing_vector"]:

            # get list of existing report files in report path
            log.info(
                "do_delete_existing_vector flag is set to True: deleting existing vectorised change report shapefiles, pkls and csvs"
            )
            directory = os.path.dirname(report)
            report_shp_pattern = "/report*"
            search_shp_pattern = f"{directory}{report_shp_pattern}"
            existing_files = glob.glob(search_shp_pattern)

            # exclude .tif files from the delete list
            files_to_remove = [
                file for file in existing_files if not file.endswith(".tif")
            ]

            for file in files_to_remove:
                try:
                    os.remove(file)
                except:
                    log.error(f"Could not delete : {file}, skipping")

        # find tile string for the report to be vectorised
        tile = sorted_filepaths[0].split("/")[-1].split("_")[-2]

        if not config_dict["do_parallel"]:
            #try:
            acd_by_tile_vectorisation.vector_report_generation(config_path, tile)
            #except:
             #   log.error(f"Sequential Mode: Failed to vectorise {report}, moving on to the next")
        if config_dict["do_parallel"]:
            try:
                subprocess.run()
            except:
                log.error(f"Parallel Mode: Failed to vectorise {report}, moving on to the next")

    log.info("---------------------------------------------------------------")
    log.info("---------------------------------------------------------------")
    log.info("National Vectorisation of the Change Reports Complete")
    log.info("---------------------------------------------------------------")
    log.info("---------------------------------------------------------------")

    return


def acd_national_integration(
    root_dir: str, log: logging.Logger, epsg: int, conda_env_name: str, config_dict: dict
):

    """

    This function:
        - glob to find 1 report_xxx.pkl file per tile in output/probabilities,
        - from which to read in a Pandas DataFrame of the vectorised changes,
        - then concatenate DataFrame for each tile to form a national change event DataFrame
        - save to disc in /integrated/acd_national_integration.pkl

    Parameters:
    ----------
    root_dir : str
    log : logging.Logger
        a Logger object
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
    log.info(f"Number of change Report Shapefiles to integrate  :  {len(vectorised_paths)}")
    log.info("Paths of shapefiles to integrate are:")
    for number, path in enumerate(sorted(vectorised_paths)):
        log.info(f"{number} : {path}")

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

    # specify roi path
    roi_filepath = os.path.join(config_dict["roi_dir"], config_dict["roi_filename"])
    
    # logic if roi path does not exist
    if not os.path.exists(roi_filepath):
        log.error("Could not open ROI, filepath does not exist")
        log.error(f"Exiting acd_national(), ensure  {roi_filepath}  exists")
        sys.exit(1)

    # read in ROI, reproject
    log.info("Reading in ROI")
    roi = gpd.read_file(roi_filepath)
    #log.info(f"Ensuring ROI is of EPSG  :  {epsg}")
    roi = roi.to_crs(epsg)

    # for each shapefile in the list of shapefile paths, read, filter and merge
    with TemporaryDirectory(dir=os.getcwd()) as td:
        for vector in sorted(vectorised_paths):
            try:
                # read in shapefile, reproject
                log.info(f"Reading in change report shapefile   :  {vector}")
                shape = gpd.read_file(vector)
                #log.info(f"Ensuring change report shapefile is of EPSG  :  {epsg}")
                shape = shape.to_crs(epsg)

                # spatial filter intersection of shapefile with ROI
                log.info(f"Intersecting {vector} with {roi_filepath}")
                intersected = shape.overlay(roi, how="intersection")

                # join the two gdfs
                merged_gdf = pd.concat([merged_gdf, intersected], ignore_index=True)
                log.info(f"Intersection: Success")

                # explode to convert any multipolygons created from intersecting to individual polygons
                merged_gdf = merged_gdf.explode(index_parts=False)

                # recompute area
                merged_gdf["area"] = merged_gdf.area

                log.info(f"Integrated geodataframe length is currently  :  {len(merged_gdf['area'])}")
            except:
                log.error(f"failed to merge geodataframe: {vector}")

    # write integrated geodataframe to shapefile
    with TemporaryDirectory(dir=os.getcwd()) as td:
        try:
            out_path = f"{os.path.join(root_dir, 'national_geodataframe.shp')}"
            log.info(f"Merging loop complete, now writing integrated shapefile to {out_path}")
            merged_gdf.to_file(filename=out_path)
        except:
            log.error(f"failed to write output at :  {out_path}")
    log.info(f"Integrated GeoDataFrame written to {out_path}")

    log.info("---------------------------------------------------------------")
    log.info("---------------------------------------------------------------")
    log.info("National Integration of the Vectorised Change Reports Complete")
    log.info("---------------------------------------------------------------")
    log.info("---------------------------------------------------------------")

    return

def acd_national_filtering(log: logging.Logger, config_dict: dict):
    """

    This function:
        - Applies filters to the national vectorised change report, as specified in the pyeo.ini
            - The planned filters are:
                - Counties
                - Minimum Area
                - Date Period?

                
    Parameters:
    ----------

    log : logging.Logger
        The logger object
    config_dict : dict
        config dictionary containing runtime parameters

    Returns:
    ----------
    None
    """

    # specify gdal and proj installation, this is geopandas'
    home = str(Path.home())
    conda_env_name = config_dict["conda_env_name"]
    os.environ[
        "GDAL_DATA"
    ] = f"{home}/miniconda3/envs/{conda_env_name}/lib/python3.10/site-packages/pyproj/proj_dir/share/gdal"
    os.environ[
        "PROJ_LIB"
    ] = f"{home}/miniconda3/envs/{conda_env_name}/lib/python3.10/site-packages/pyproj/proj_dir/share/proj"

    # find national_geodataframe
    search_pattern = "national_geodataframe.shp"
    national_change_report_path = glob.glob(os.path.join(config_dict["tile_dir"], search_pattern))[0]

    # read in national geodataframe created before by the integrated step
    if os.path.exists(national_change_report_path):
        national_gdf = gpd.read_file(national_change_report_path)
    else:
        log.error(f"national geodataframe does not exist, have you set 'do_integrate' to True in pyeo_1.ini?")
        sys.exit(1)

    # create a query based on the county list provided in pyeo_1.ini
    query_values = " or ".join(f"County == '{county_name}'" for county_name in config_dict["counties_of_interest"])

    # apply the county query filter
    filtered = national_gdf.query(query_values)

    # create a query based on minimum area provided in pyeo_1.ini
    query_values = f"area > {config_dict['minimum_area_to_report_m2']}"

    # apply the minimum area filter
    filtered = filtered.query(query_values)

    # write filtered geodataframe to shapefile
    with TemporaryDirectory(dir=os.getcwd()) as td:
        try:
            out_path = f"{os.path.join(config_dict['tile_dir'], 'national_geodataframe_filtered.shp')}"
            log.info(f"Filtering complete, now writing filtered national shapefile to :")
            log.info(f"        {out_path}")
            filtered.to_file(filename=out_path)
        except:
            log.error(f"failed to write output at :  {out_path}")

    return

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

    # filesystem_utilities.create_folder_structure_for_tiles(tile_dir)

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