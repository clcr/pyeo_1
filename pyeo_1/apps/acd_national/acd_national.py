def acd_national(path_to_ini: str):
    """
    This function:
        - acts as the singular call to run automatic change detection per tile, then aggregate to national, then distribute the change alerts.
    
    """
    
    acd_roi_tile_intersection()
    
    acd_by_tile_raster()
    
    acd_by_tile_vector()
    
    acd_national_integration()
    
    acd_national_filtering()
    
    acd_national_dataframe_to_shapefile()
    
    acd_national_manual_validation()
    
    acd_national_distribution()
    
    log.info("all done")
    
############################

def acd_roi_tile_intersection():
    """
    
    This function:
        - accepts a Region of Interest (RoI) and writes a tilelist.txt of the Sentinel-2 tiles that the RoI covers.
    
        - the tilelist.txt is then used to perform the tile-based processes, raster and vector.

    """

def acd_by_tile_raster():
    """
    
    This function:
        - sequentially calls acd_per_tile_raster for all active tiles and waits for completion/failure 
    
    """
    

def acd_by_tile_vector():
    """
    
    This function:
        - Vectorises the change report raster by calling acd_per_tile_vector for all active tiles and waits for completion/failure 

        - Adds two additional columns to allocate and record to support acd_national_manual_validation
        
            - "assesor" and "decision"

    """

def acd_national_vectorisation(root_dir: str,
                               log,
                               epsg: int,
                               level_1_boundaries_path: str,
                               conda_env_name: str,
                               delete_existing: bool):
    """
    This function:
        - Globs through the tile outputs, running acd_by_tile_vectorisation()
  
    
    """

    import glob
    import os

    tiles_name_pattern = "[0-9][0-9][A-Z][A-Z][A-Z]"
    report_tif_pattern = "/output/probabilities/report*.tif"
    search_pattern = f"{tiles_name_pattern}{report_tif_pattern}"

    tiles_paths = glob.glob(os.path.join(root_dir, search_pattern))
    
    log.info("Tiles to vectorise the change report rasters of:  ")
    log.info(tiles_paths)
    log.info("--"*20)
    
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

    log.info("--"*20)
    log.info("--"*20)
    log.info("National Vectorisation of the Change Reports Complete")
    log.info("--"*20)
    log.info("--"*20)

    return tiles_paths

def acd_national_integration():
    """

    This function:
        - glob to find 1 report_xxx.pkl file per tile in output/probabilities,
        - from which to read in a Pandas DataFrame of the vectorised changes,
        - then concatenate DataFrame for each tile to form a national change event DataFrame
        - save to disc in /integrated/acd_national_integration.pkl
       
    """

def acd_national_filtering():
    """
    
    This function:
        - Applies filters to the national vectorised change report, as specified in the pyeo.ini
            - The planned filters are:
                - Counties
                - Minimum Area
                - Date Period     

    """
    
def acd_national_dataframe_to_shapefile():
    """
    
    This function:
        - converts an event DataFrame stored in a .pkl format to a shapefile suitable for importing in QGIS
    
    """
    
 def acd_national_dataframe_to_csv():
    """
    
    This function:
        - converts an event DataFrame stored in a .pkl format to a csv for Excel or a text editor
    
    """
    
   
def acd_national_qgis_bookmark_generation():
    """
    
    This function:
       - Generates a QGIS Project file using pyQGIS
       - Generates QGIS Spatial Bookmarks (.xml) from a filtered dataframe for import into QGIS
       - Import report.tif
       - Import ROI
    
    """



def acd_national_manual_validation():
    """
    
    This function:
        - is a placeholder for manual validation to assess each event, flagging for on the ground observation
    
    """

def acd_national_distribution():
    """

    This function:
        - Once the user is happy and has verified the change alerts, Maps.Me and WhatsApp messages are sent from this function.
    
    """
    
############################

def acd_per_tile_raster():
    """
    
    
    This function:
        
        - queries available Sentinel-2 imagery that matches the tilelist and environmental parameters specified in the pyeo.ini file

        - for the composite, this downloads Sentinel-2 imagery, applies atmospheric correction (if necessary), converts from .jp2 to .tif and applies cloud masking

        - for the change images, this downloads Sentinel-2 imagery, applies atmospheric correction (if necessary), converts from .jp2 to .tif and applies cloud masking

        - classifies the composite and change images

        - performs change detection between the classified composite and the classified change images, searching for land cover changes specified in the from_classes and to_classes in the pyeo.ini

        - outputs a change report raster with dates land cover change


    """
    
    
def acd_per_tile_vector():
    """
    
    This function:
        - Vectorises the change report raster

        - Adds two additional columns to allocate and record to support acd_national_manual_validation
        
            - "assesor" and "decision"

    """
