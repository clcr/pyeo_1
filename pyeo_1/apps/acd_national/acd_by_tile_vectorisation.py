import os
import sys
import glob
import logging
from pyeo_1 import filesystem_utilities
from pyeo_1 import vectorisation


def vector_report_generation(config_path: str, tile: str):
    """

    This function:

        - Vectorises the Change Report Raster, with the aim of producing shapefiles \n
            that can be filtered and summarised spatially, and displayed in a GIS.

    Parameters
    ----------
    config_path : str
        path to pyeo_1.ini
    tile : str
        Sentinel-2 tile name to process

    Returns
    ----------
    None

    """

    # get the config
    config_dict = filesystem_utilities.config_path_to_config_dict(
        config_path=config_path
    )

    # changes directory to pyeo_dir, enabling the use of relative paths from the config file
    os.chdir(config_dict["pyeo_dir"])
    
    # get other parameters
    conda_env_name = config_dict["conda_env_name"]
    epsg = config_dict["epsg"]
    level_1_boundaries_path = config_dict["level_1_boundaries_path"]

    # Matt: get the report raster from the previous functions
    # for parallelism reasons, the report path cannot be passed to this function
    # so we run the report glob again

    # get all report.tif that are within the root_dir with search pattern
    report_tif_pattern = "/output/probabilities/report*.tif"
    search_pattern = f"{tile}{report_tif_pattern}"

    change_report_path = glob.glob(
        os.path.join(config_dict["tile_dir"], search_pattern)
    )[0]

    ## setting up the per tile logger
    # get path where the tiles are downloaded to
    tile_directory_path = config_dict["tile_dir"]
    # check for and create the folder structure pyeo expects
    individual_tile_directory_path = os.path.join(tile_directory_path, tile)
    # get the logger for this tile
    tile_log = filesystem_utilities.init_log_acd(
        log_path=os.path.join(individual_tile_directory_path, "log", tile + "_log.txt"),
        logger_name=f"pyeo_1_tile_{tile}_log",
    )

    tile_log.info("--" * 20)
    tile_log.info(f"Starting Vectorisation of the Change Report Raster of Tile: {tile}")
    tile_log.info("--" * 20)

    path_vectorised_binary = vectorisation.vectorise_from_band(
        change_report_path=change_report_path,
        band=15,
        log=tile_log,
        conda_env_name=conda_env_name,
    )
    # was band=6

    path_vectorised_binary_filtered = vectorisation.clean_zero_nodata_vectorised_band(
        vectorised_band_path=path_vectorised_binary,
        log=tile_log,
        conda_env_name=conda_env_name,
    )

    rb_ndetections_zstats_df = vectorisation.zonal_statistics(
        raster_path=change_report_path,
        shapefile_path=path_vectorised_binary_filtered,
        report_band=5,
        log=tile_log,
        conda_env_name=conda_env_name,
    )
    # was band=2

    rb_confidence_zstats_df = vectorisation.zonal_statistics(
        raster_path=change_report_path,
        shapefile_path=path_vectorised_binary_filtered,
        report_band=9,
        log=tile_log,
        conda_env_name=conda_env_name,
    )
    # was band=5

    rb_first_changedate_zstats_df = vectorisation.zonal_statistics(
        raster_path=change_report_path,
        shapefile_path=path_vectorised_binary_filtered,
        report_band=4,
        log=tile_log,
        conda_env_name=conda_env_name,
    )
    # was band=7

    # table joins, area, lat lon, county
    vectorisation.merge_and_calculate_spatial(
        rb_ndetections_zstats_df=rb_ndetections_zstats_df,
        rb_confidence_zstats_df=rb_confidence_zstats_df,
        rb_first_changedate_zstats_df=rb_first_changedate_zstats_df,
        path_to_vectorised_binary_filtered=path_vectorised_binary_filtered,
        write_csv=False,
        write_shapefile=True,
        write_pkl=False,
        delete_intermediates=True,
        change_report_path=change_report_path,
        log=tile_log,
        epsg=epsg,
        level_1_boundaries_path=level_1_boundaries_path,
        tileid=tile,
        conda_env_name=conda_env_name,
    )

    tile_log.info("---------------------------------------------------------------")
    tile_log.info("Vectorisation of the Change Report Raster complete")
    tile_log.info("---------------------------------------------------------------")

    return


# if run from terminal, do this:
if __name__ == "__main__":
    # assuming argv[0] is script name, config_path passed as index 1 and tile string as index 2
    vector_report_generation(config_path=sys.argv[1], tile=sys.argv[2])
