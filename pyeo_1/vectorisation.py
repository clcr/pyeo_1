def band_naming(band: int, log):
    """
    This function provides a variable name (string) based on the input integer.

    Parameters
    ----------
    band : int
        the band to interpet as a name.
        The integer format used here is starting from 1, not 0
    log : logging.Logger


    Returns
    ----------
    band_name : str

    """
    # note to self : + 1 from Python
    # fields may get shortened (laundered)

    if band == 1:
        band_name = "band1"
    elif band == 2:
        band_name = "band2"
    elif band == 3:
        band_name = "band3"
    elif band == 4:
        band_name = "band4"
    elif band == 5:
        band_name = "band5"
    elif band == 6:
        band_name = "band6"
    elif band == 7:
        band_name = "band7"
    elif band == 8:
        band_name = "band8"
    elif band == 9:
        band_name = "band9"
    elif band == 10:
        band_name = "band10"
    elif band == 11:
        band_name = "band11"
    elif band == 12:
        band_name = "band12"
    elif band == 13:
        band_name = "band13"
    elif band == 14:
        band_name = "band14"
    elif band == 15:
        band_name = "band15"
    elif band == 16:
        band_name = "band16"
    elif band == 17:
        band_name = "band17"
    elif band == 18:
        band_name = "band18"

    else:
        log.error(f"band was not an integer from 1 - 18, {band} was supplied, instead.")
        pass

    return band_name


def vectorise_from_band(change_report_path: str, band: int, log, conda_env_name):
    """
    This function takes the path of a change report raster and using a band integer, vectorises a band layer.


    Parameters:
    ----------------

    change_report_path : str
        path to a change report raster
    band : int
        an integer from 1 - 18, indicating the desired band to vectorise.
        the integer corresponds to GDAL numbering, i.e. starting at 1
        instead of 0 as in Python.
    log : logging.Logger
        log variable
    conda_env_name : str
        String of the conda_env_name for specifying which GDAL and PROJ_LIB installation to use

    Returns
    ----------------
    out_filename : str
        the output path of the vectorised band

    """
    import os
    from tempfile import TemporaryDirectory
    from osgeo import gdal, ogr, osr
    from pathlib import Path

    # specify gdal and proj installation, this is GDAL's
    home = str(Path.home())
    os.environ["GDAL_DATA"] = f"{home}/miniconda3/envs/{conda_env_name}/share/gdal"
    os.environ["PROJ_LIB"] = f"{home}/miniconda3/envs/{conda_env_name}/share/proj"

    # log.info(f"PROJ_LIB path has been set to : {os.environ['PROJ_LIB']}")

    # let GDAL use Python to raise Exceptions, instead of printing to sys.stdout
    gdal.UseExceptions()

    with TemporaryDirectory(dir=os.getcwd()) as td:
        # get Raster datasource
        src_ds = gdal.Open(change_report_path)
        log.info(f"Opening {change_report_path}")

        if src_ds is None:
            log.error(f"Unable to open {change_report_path}")

        log.info(f"Successfully opened {change_report_path}")

        try:
            src_band = src_ds.GetRasterBand(band)

            # get projection of report.tif, assign to the vector
            proj = osr.SpatialReference(src_ds.GetProjection())
        except RuntimeError as error:
            log.error(
                f"Could not open band {band}. encountered the following error \n {error}"
            )

        # create output datasource
        dst_layername = band_naming(band, log=log)

        drv = ogr.GetDriverByName("ESRI Shapefile")
        out_filename = f"{change_report_path[:-4]}_{dst_layername}.shp"
        dst_ds = drv.CreateDataSource(out_filename)
        dst_layer = dst_ds.CreateLayer(dst_layername, srs=proj)

        # make number of Detections column from pixel values
        field = ogr.FieldDefn(dst_layername, ogr.OFTInteger)
        dst_layer.CreateField(field)
        dst_field = dst_layer.GetLayerDefn().GetFieldIndex(dst_layername)

        # polygonise the raster band
        log.info("Now vectorising the raster band")
        try:
            gdal.Polygonize(
                src_band,
                # src_band.GetMaskBand(),  # use .msk to only polygonise values > 0 # can't get gdal.Polygonize to respect .msk
                None,  # no mask
                dst_layer,
                dst_field,  # -1 for no field column
                [],
            )
            
        except RuntimeError as error:
            log.error(f"GDAL Polygonize failed: \n {error}")
        except Exception as error:
            log.error(f"GDAL Polygonize failed, error received : \n {error}")

        # close dst_ds and src_band
        dst_ds = None
        src_band = None
        if dst_ds is None:
            log.info(f"Band {band} of {change_report_path} was successfully vectorised")
            log.info(f"Band {band} was written to {out_filename}")

    return out_filename


def filter_vectorised_band(vectorised_band_path: str, log, conda_env_name: str):
    """

    This function filters the vectorised bands.

    Parameters
    ----------------

    vectorised_band_path : str
        path to the band to filter
    log : logging.Logger
        The logger object
    conda_env_name : str
        name of the conda environment

    Returns
    ----------------
    filename : variable

    """
    from tempfile import TemporaryDirectory
    import geopandas as gpd
    import os
    from pathlib import Path

    # specify gdal and proj installation, here it is geopandas'
    home = str(Path.home())
    os.environ[
        "PROJ_LIB"
    ] = f"{home}/miniconda3/envs/{conda_env_name}/lib/python3.10/site-packages/pyproj/proj_dir/share/proj"

    log.info(f"filtering out zeroes and nodata from: {vectorised_band_path}")

    with TemporaryDirectory(dir=os.getcwd()) as td:
        # read in shapefile
        shp = gpd.read_file(vectorised_band_path)

        # create fieldname variable
        fieldname = os.path.splitext(vectorised_band_path.split("_")[-1])[0]

        # filter out 0 and 32767 (nodata) values
        filtered = shp.loc[(shp[fieldname] != 0) & (shp[fieldname] != 32767)]

        # assign explicit id from index
        filtered["id"] = filtered.reset_index().index

        # save to shapefile
        filename = f"{os.path.splitext(vectorised_band_path)[0]}_filtered.shp"
        filtered.to_file(filename=filename, driver="ESRI Shapefile")

    # remove variables to reduce memory (RAM) consumption
    del (shp, filtered)

    log.info(f"filtering complete and saved at  : {filename}")

    return filename


def boundingBoxToOffsets(bbox, geot):

    """

    This function converts a bounding box to offsets.
    The contents of which were written by Konrad Hafen, \n taken from: https://opensourceoptions.com/blog/zonal-statistics-algorithm-with-python-in-4-steps/

    Parameters
    ----------------

    Returns
    ----------------
    [row1, row2, col1, col2] : list
        list of offsets (integers)

    """

    col1 = int((bbox[0] - geot[0]) / geot[1])
    col2 = int((bbox[1] - geot[0]) / geot[1]) + 1
    row1 = int((bbox[3] - geot[3]) / geot[5])
    row2 = int((bbox[2] - geot[3]) / geot[5]) + 1
    return [row1, row2, col1, col2]


def geotFromOffsets(row_offset, col_offset, geot):

    """

    This function calculates a new geotransform from offsets.
    The contents of which were written by Konrad Hafen, \n taken from: https://opensourceoptions.com/blog/zonal-statistics-algorithm-with-python-in-4-steps/

    Parameters
    ----------------
    row_offset : int
    col_offset : int
    geot : variable

    Returns
    ----------------
    new_geot : float

    """

    new_geot = [
        geot[0] + (col_offset * geot[1]),
        geot[1],
        0.0,
        geot[3] + (row_offset * geot[5]),
        0.0,
        geot[5],
    ]
    return new_geot


def setFeatureStats(fid, min, max, mean, median, sd, sum, count, report_band):

    """ 
    
    This function sets the feature stats to calculate from the array.

    Parameters
    ----------

    fid : int
    min : int
    max : int
    mean : float
    median : float
    sd : float
    sum : int
    count : int
    report_band : int

    Returns
    ---------

    featstats : dict
    """

    names = [
        f"rb{report_band}_min",
        f"rb{report_band}_max",
        f"rb{report_band}_mean",
        f"rb{report_band}_median",
        f"rb{report_band}_sd",
        f"rb{report_band}_sum",
        f"rb{report_band}_count",
        "id",
    ]

    featstats = {
        names[0]: min,
        names[1]: max,
        names[2]: mean,
        names[3]: median,
        names[4]: sd,
        names[5]: sum,
        names[6]: count,
        names[7]: fid,
    }

    return featstats


def zonal_statistics(
    raster_path: str, shapefile_path: str, report_band: int, log, conda_env_name: str
):

    """
    This function calculates zonal statistics on a raster.
    The contents of which were written by Konrad Hafen, \n taken from: https://opensourceoptions.com/blog/zonal-statistics-algorithm-with-python-in-4-steps/
    Matt Payne has amended aspects of the function to accommodate library updates from GDAL, OGR and numpy.ma.MaskedArray() on 30/03/2023.

    Note, the raster at raster_path needs to be an even shape, e.g. 10980, 10980, not 10979, 10979.

    Parameters
    ---------------
    raster_path : str
        the path to the raster to obtain the values from.
    shapefile_path : str
        the path to the shapefile which we will use as the "zones"
    band : int
        the band to run zonal statistics on.

    Returns
    ----------------
    zstats_df

    """

    from osgeo import gdal, ogr
    import numpy as np
    import pandas as pd
    import os
    import csv
    from pathlib import Path
    from tempfile import TemporaryDirectory

    # enable gdal to raise exceptions
    gdal.UseExceptions()

    # specify gdal and proj installation, this is GDAL's
    home = str(Path.home())
    os.environ["GDAL_DATA"] = f"{home}/miniconda3/envs/{conda_env_name}/share/gdal"
    os.environ["PROJ_LIB"] = f"{home}/miniconda3/envs/{conda_env_name}/share/proj"

    #log.info(f"PROJ_LIB path has been set to : {os.environ['PROJ_LIB']}")

    with TemporaryDirectory(dir=os.getcwd()) as td:
        mem_driver = ogr.GetDriverByName("Memory")
        mem_driver_gdal = gdal.GetDriverByName("MEM")
        shp_name = "temp"

        fn_raster = raster_path
        fn_zones = shapefile_path

        r_ds = gdal.Open(fn_raster)
        p_ds = ogr.Open(fn_zones)

        # lyr = shapefile layer
        lyr = p_ds.GetLayer()
        # get projection to apply to temporary files
        proj = lyr.GetSpatialRef()
        geot = r_ds.GetGeoTransform()
        nodata = r_ds.GetRasterBand(1).GetNoDataValue()

        zstats = []

        # p_feat = polygon feature
        p_feat = lyr.GetNextFeature()
        niter = 0

        # while lyr.GetNextFeature() returns a polygon feature, do the following:
        while p_feat:

            try:
                # if a geometry is returned from p_feat, do the following:
                if p_feat.GetGeometryRef() is not None:
                    if os.path.exists(shp_name):

                        mem_driver.DeleteDataSource(shp_name)

                    # tp_ds = temporary datasource
                    tp_ds = mem_driver.CreateDataSource(shp_name)
                    tp_lyr = tp_ds.CreateLayer(
                        "polygons", srs=proj, geom_type=ogr.wkbPolygon
                    )
                    tp_lyr.CreateFeature(p_feat.Clone())
                    offsets = boundingBoxToOffsets(
                        p_feat.GetGeometryRef().GetEnvelope(), geot
                    )
                    new_geot = geotFromOffsets(offsets[0], offsets[2], geot)

                    # tr_ds = target datasource
                    tr_ds = mem_driver_gdal.Create(
                        "",
                        offsets[3] - offsets[2],
                        offsets[1] - offsets[0],
                        1,
                        gdal.GDT_Byte,
                    )

                    tr_ds.SetGeoTransform(new_geot)
                    gdal.RasterizeLayer(tr_ds, [1], tp_lyr, burn_values=[1])
                    tr_array = tr_ds.ReadAsArray()

                    r_array = r_ds.GetRasterBand(report_band).ReadAsArray(
                        offsets[2],
                        offsets[0],
                        offsets[3] - offsets[2],
                        offsets[1] - offsets[0],
                    )

                    # get identifier for
                    id = p_feat.GetFID()

                    # if raster array was successfully read, do the following:
                    if r_array is not None:
                        maskarray = np.ma.MaskedArray(
                            r_array,
                            mask=np.logical_or(
                                r_array == nodata, np.logical_not(tr_array)
                            ),
                        )

                        if maskarray is not None:
                            zstats.append(
                                setFeatureStats(
                                    id,
                                    maskarray.min(),
                                    maskarray.max(),
                                    maskarray.mean(),
                                    np.ma.median(maskarray),
                                    maskarray.std(),
                                    maskarray.sum(),
                                    maskarray.count(),
                                    report_band=report_band,
                                )
                            )
                        else:
                            zstats.append(
                                setFeatureStats(
                                    id,
                                    nodata,
                                    nodata,
                                    nodata,
                                    nodata,
                                    nodata,
                                    nodata,
                                    nodata,
                                    report_band=report_band,
                                )
                            )
                    else:
                        zstats.append(
                            setFeatureStats(
                                id,
                                nodata,
                                nodata,
                                nodata,
                                nodata,
                                nodata,
                                nodata,
                                nodata,
                                report_band=report_band,
                            )
                        )

                    # close temporary variables, resetting them for the next iteration
                    tp_ds = None
                    tp_lyr = None
                    tr_ds = None

                    # once there are no more features to retrieve, p_feat will return as None, exiting the loop
                    p_feat = lyr.GetNextFeature()

            except RuntimeError as error:
                print(error)

        fn_csv = f"{os.path.splitext(raster_path)[0]}_zstats_over_{band_naming(report_band, log=log)}.csv"
        col_names = zstats[0].keys()

        zstats_df = pd.DataFrame(data=zstats, columns=col_names)

        with open(fn_csv, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, col_names)
            writer.writeheader()
            writer.writerows(zstats)

    return zstats_df


def merge_and_calculate_spatial(
    rb_ndetections_zstats_df,
    rb_confidence_zstats_df,
    rb_first_changedate_zstats_df,
    path_to_vectorised_binary_filtered: str,
    write_csv: bool,
    write_shapefile: bool,
    write_pkl: bool,
    change_report_path: str,
    delete_intermediates: bool,
    log,
    epsg: int,
    level_1_boundaries_path: str,
    tileid: str,
    conda_env_name: str,
):

    """
    This function takes the zonal statistics Pandas DataFrames and performs a table join
    to the vectorised binary polygons that are the basis of the vectorised change report.

    Parameters
    ------------

    rb_ndetections_zstats_df : pd.DataFrame()
        Pandas DataFrame object for report band 5 (ndetections)

    rb_confidence_zstats_df : pd.DataFrame()
        Pandas DataFrame object for report band 9 (confidence)

    rb_first_changedate_zstats_df : pd.DataFrame()
        Pandas DataFrame object for report band 4 (approved first change date)

    path_to_vectorised_binary : str
        Path to the vectorised binary shapefile

    write_pkl : bool (optional)
        whether to write to pkl, defaults to False

    write_csv : bool (optional)
        whether to write to csv, defaults to False

    write_shapefile : bool (optional)
        whether to write to shapefile, defaults to False

    change_report_path : str
        the path of the original change_report tiff, used for filenaming if saving outputs

    Returns
    ----------------
    None

    """

    import os
    import glob
    import pandas as pd
    import geopandas as gpd
    from pathlib import Path
    from pyeo_1.filesystem_utilities import serial_date_to_string

    # specify gdal and proj installation, this is geopandas'
    home = str(Path.home())
    os.environ[
        "GDAL_DATA"
    ] = f"{home}/miniconda3/envs/{conda_env_name}/lib/python3.10/site-packages/pyproj/proj_dir/share/gdal"
    os.environ[
        "PROJ_LIB"
    ] = f"{home}/miniconda3/envs/{conda_env_name}/lib/python3.10/site-packages/pyproj/proj_dir/share/proj"

    binary_dec = gpd.read_file(path_to_vectorised_binary_filtered)

    # convert first date of change detection in days, to change date
    # columns_to_apply = ["rb7_min", "rb7_max", "rb7_mean", "rb7_median"]
    columns_to_apply = ["rb4_min", "rb4_max", "rb4_mean", "rb4_median"]

    for column in columns_to_apply:
        rb_first_changedate_zstats_df[column] = rb_first_changedate_zstats_df[
            column
        ].apply(serial_date_to_string)

    # table join on id
    merged = binary_dec.merge(rb_ndetections_zstats_df, on="id", how="inner")
    merged2 = merged.merge(rb_confidence_zstats_df, on="id", how="inner")
    merged3 = merged2.merge(rb_first_changedate_zstats_df, on="id", how="inner")
    merged = merged3

    log.info("Merging Complete")
    # housekeeping, remove unused variables
    del (merged3, merged2, binary_dec)

    # add area
    merged["area_m2"] = merged.area

    # add lat long from centroid that falls within the polygon
    merged["long"] = merged.representative_point().map(lambda p: p.x)
    merged["lat"] = merged.representative_point().map(lambda p: p.y)

    # read in county boundaries from ini, and keep only County and geometry columns
    log.info(
        f"reading in administrative boundary information from {level_1_boundaries_path}"
    )
    boundaries = gpd.read_file(level_1_boundaries_path)
    boundaries = boundaries.filter(["NAME_1", "geometry"]).rename(
        columns={"NAME_1": "County"}
    )

    # check crs logic
    if boundaries.crs is not epsg:
        log.info(
            f"boundary epsg is {boundaries.crs}, but merged dataframe has {merged.crs}"
        )
        log.info(f"reprojecting")
        boundaries = boundaries.to_crs(epsg)
        log.info(f"boundaries reprojected to {boundaries.crs}")
    else:
        pass

    # county spatial join
    merged = merged.sjoin(boundaries, predicate="within", how="left").drop(
        ["index_right"], axis=1
    )

    # add user and decision columns, for verification
    merged["tileid"] = tileid
    merged["user"] = pd.Series(dtype="string")
    merged["eventClass"] = pd.Series(dtype="string")
    merged["follow_up"] = pd.Series(dtype="string")
    merged["comments"] = pd.Series(dtype="string")

    # reorder geometry to be the last column
    columns = list(merged.columns)
    columns.remove("geometry")
    columns.append("geometry")
    merged = merged.reindex(columns=columns)

    shp_fname = f"{os.path.splitext(change_report_path)[0]}.shp"
    csv_fname = f"{os.path.splitext(change_report_path)[0]}.csv"
    pkl_fname = f"{os.path.splitext(change_report_path)[0]}.pkl"

    if write_shapefile:
        merged.to_file(shp_fname)
        log.info(f"Shapefile written as ESRI Shapefile, to:  {shp_fname}")

    if write_pkl:
        merged.to_pickle(pkl_fname)
        log.info(f"GeoDataFrame written as pickle, to:  {pkl_fname}")

    if write_csv:
        merged.to_csv(csv_fname)
        log.info(f"DataFrame written as csv, to:   {csv_fname}")

    if delete_intermediates:
        try:
            log.info("Deleting intermediate change report vectorisation files")
            directory = os.path.dirname(change_report_path)
            binary_dec_pattern = f"{directory}/*band*"
            zstats_pattern = f"{directory}/*zstats*"

            intermediate_files = glob.glob(binary_dec_pattern)
            zstat_files = glob.glob(zstats_pattern)

            intermediate_files.extend(zstat_files)

            for file in intermediate_files:
                os.remove(file)
        except:
            log.info("Could not delete intermediate files")

    return


def vector_report_generation(
    raster_change_report_path: str,
    write_csv: bool,
    write_shapefile: bool,
    write_pkl: bool,
    log,
    epsg: int,
    level_1_boundaries_path: str,
    conda_env_name: str,
    delete_intermediates: bool,
):
    """

    This function calls all the individual functions necessary to create a vectorised change report.

    Parameters
    ---------------

    raster_change_report : str
        path to the report raster

    conda_env_name : str
        string of the name of the conda environment, used to permit interchangeable use of GDAL and geopandas' installations.


    Returns
    ----------------
    None

    """

    change_report_path = raster_change_report_path
    tileid = change_report_path.split("/")[-1].split("_")[
        2
    ]  # I should take tileid from .ini but this is quicker for now (12/04/2023)

    log.info("--" * 20)
    log.info(f"Starting Vectorisation of the Change Report Raster of Tile: {tileid}")
    log.info("--" * 20)

    # 22 minutes
    path_vectorised_binary = vectorise_from_band(
        change_report_path=change_report_path,
        band=15,
        log=log,
        conda_env_name=conda_env_name,
    )
    # was band=6

    path_vectorised_binary_filtered = filter_vectorised_band(
        vectorised_band_path=path_vectorised_binary,
        log=log,
        conda_env_name=conda_env_name,
    )

    rb_ndetections_zstats_df = zonal_statistics(
        raster_path=change_report_path,
        shapefile_path=path_vectorised_binary_filtered,
        report_band=5,
        log=log,
        conda_env_name=conda_env_name,
    )
    # was band=2

    rb_confidence_zstats_df = zonal_statistics(
        raster_path=change_report_path,
        shapefile_path=path_vectorised_binary_filtered,
        report_band=9,
        log=log,
        conda_env_name=conda_env_name,
    )
    # was band=5

    rb_first_changedate_zstats_df = zonal_statistics(
        raster_path=change_report_path,
        shapefile_path=path_vectorised_binary_filtered,
        report_band=4,
        log=log,
        conda_env_name=conda_env_name,
    )
    # was band=7
    # table joins, area, lat lon, county
    merge_and_calculate_spatial(
        rb_ndetections_zstats_df=rb_ndetections_zstats_df,
        rb_confidence_zstats_df=rb_confidence_zstats_df,
        rb_first_changedate_zstats_df=rb_first_changedate_zstats_df,
        path_to_vectorised_binary_filtered=path_vectorised_binary_filtered,
        write_csv=write_csv,
        write_shapefile=write_shapefile,
        write_pkl=write_pkl,
        delete_intermediates=delete_intermediates,
        change_report_path=change_report_path,
        log=log,
        epsg=epsg,
        level_1_boundaries_path=level_1_boundaries_path,
        tileid=tileid,
        conda_env_name=conda_env_name,
    )

    log.info("---------------------------------------------------------------")
    log.info("Vectorisation of the Change Report Raster complete")
    log.info("---------------------------------------------------------------")

    return
