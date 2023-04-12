def band_naming(band: int,
                log):
    """
    This function provides a variable name (string) based on the input integer.

    Returns
        band_name (str)
    """

    if band == 1:
        band_name = "ChangeDays"
    elif band == 2:
        band_name = "NDetection"
    elif band == 3:
        band_name = "NNoChange"
    elif band == 4:
        band_name = "NClassif"
    elif band == 5:
        band_name = "Confidence"
    elif band == 6:
        band_name = "BinaryDec"
    elif band == 7:
        band_name = "ApprChange"
    else:
        log.error(f"band was not an integer from 1 - 7, {band} was supplied, instead.")

    return band_name


def vectorise_from_band(change_report_path: str,
                        band: int,
                        log):
    """
    This function takes the path of a change report raster and using a band integer, vectorises a band layer.

    ----------------
    Parameters:
    change_report_path (str):
        path to a change report raster
    band (int):
        an integer from 1 - 7, indicating the desired band to vectorise.
    """
    import os
    from tempfile import TemporaryDirectory
    from osgeo import gdal, ogr, osr

    # let GDAL use Python to raise Exceptions, instead of printing to sys.stdout
    gdal.UseExceptions()

    with TemporaryDirectory(dir=os.getcwd()) as td:
        # get Raster datasource
        src_ds = gdal.Open(change_report_path)
        log.info(f"Opening {change_report_path}")

        if src_ds is None:
            log.info(f"Unable to open {change_report_path}")

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
        gdal.Polygonize(
            src_band,
            # src_band.GetMaskBand(),  # use .msk to only polygonise values > 0 # can't get gdal.Polygonize to respect .msk
            None,  # no mask
            dst_layer,
            dst_field,  # -1 for no field column
            [],
        )

        # close dst_ds and src_band
        dst_ds = None
        src_band = None
        if dst_ds is None:
            log.info(f"Band {band} of {change_report_path} was successfully vectorised")
            log.info(f"Band {band} was written to {out_filename}")

    return out_filename


def filter_vectorised_band(vectorised_band_path: str,
                           log):
    """
    This function filters the vectorised bands.

    """
    from tempfile import TemporaryDirectory
    import geopandas as gpd
    import os
    from pathlib import Path

    # specify gdal and proj installation, here it is geopandas'
    home = str(Path.home())
    # os.environ["GDAL_DATA"] = f"{home}/miniconda3/envs/pyeo_1_env/share/gdal"
    os.environ[
        "PROJ_LIB"
    ] = f"{home}/miniconda3/envs/pyeo_prod_0_8_0_env/lib/python3.10/site-packages/pyproj/proj_dir/share/proj"

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

    """ """

    col1 = int((bbox[0] - geot[0]) / geot[1])
    col2 = int((bbox[1] - geot[0]) / geot[1]) + 1
    row1 = int((bbox[3] - geot[3]) / geot[5])
    row2 = int((bbox[2] - geot[3]) / geot[5]) + 1
    return [row1, row2, col1, col2]


def geotFromOffsets(row_offset, col_offset, geot):

    """ """

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

    """ """

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


def zonal_statistics(raster_path: str, shapefile_path: str, report_band: int, log):

    """
    This function, the contents of which were written by Konrad Hafen, \n taken from: https://opensourceoptions.com/blog/zonal-statistics-algorithm-with-python-in-4-steps/
    Matt Payne has amended aspects of the function to accommodate library updates from GDAL, OGR and numpy.ma.MaskedArray() on 30/03/2023.

    Note, the raster at raster_path needs to be an even shape, e.g. 10980, 10980, not 10979, 10979.

    Parameters
    ---------------
    raster_path (str)
        the path to the raster to obtain the values from.

    shapefile_path (str)
        the path to the shapefile which we will use as the "zones"

    band (int)
        the band to run zonal statistics on.
    ----------------
    Returns
    """

    from osgeo import gdal, ogr
    import numpy as np
    import pandas as pd
    import os
    import csv
    from pathlib import Path

    # enable gdal to raise exceptions
    gdal.UseExceptions()

    # specify gdal and proj installation, this is GDAL's
    home = str(Path.home())
    os.environ["GDAL_DATA"] = f"{home}/miniconda3/envs/pyeo_prod_0_8_0_env/share/gdal"
    os.environ["PROJ_LIB"] = f"{home}/miniconda3/envs/pyeo_prod_0_8_0_env/share/proj"

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
                        mask=np.logical_or(r_array == nodata, np.logical_not(tr_array)),
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

    fn_csv = (
        f"{os.path.splitext(raster_path)[0]}_zstats_over_{band_naming(report_band, log=log)}.csv"
    )
    col_names = zstats[0].keys()

    zstats_df = pd.DataFrame(data=zstats, columns=col_names)

    with open(fn_csv, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, col_names)
        writer.writeheader()
        writer.writerows(zstats)

    return zstats_df


def merge_and_calculate_spatial(
    rb2_zstats_df,
    rb5_zstats_df,
    rb7_zstats_df,
    path_to_vectorised_binary_filtered: str,
    write_csv: bool,
    write_shapefile: bool,
    write_pkl: bool,
    change_report_path: str,
    log,
    epsg,
    level_1_boundaries_path
):

    """
    This function takes the zonal statistics Pandas DataFrames and performs a table join
    to the vectorised binary polygons that are the basis of the vectorised change report.

    Parameters
    ------------

    rb2_zstats_df (pd.DataFrame())
        Pandas DataFrame object for report band 2 (ndetections)

    rb5_zstats_df (pd.DataFrame())
        Pandas DataFrame object for report band 5 (confidence)

    rb7_zstats_df (pd.DataFrame())
        Pandas DataFrame object for report band 7 (approved first change date)

    path_to_vectorised_binary (str)
        Path to the vectorised binary shapefile

    write_pkl (bool, optional)
        whether to write to pkl, defaults to False

    write_csv (bool, optional)
        whether to write to csv, defaults to False

    write_shapefile (bool, optional)
        whether to write to shapefile, defaults to False

    change_report_path (str)
        the path of the original change_report tiff, used for filenaming if saving outputs

    ------------
    Returns
    """

    import os
    import pandas as pd
    import geopandas as gpd
    from pathlib import Path
    from pyeo_1.filesystem_utilities import serial_date_to_string

    # specify gdal and proj installation, this is geopandas'
    home = str(Path.home())
    os.environ[
        "GDAL_DATA"
    ] = f"{home}/miniconda3/envs/pyeo_prod_0_8_0_env/lib/python3.10/site-packages/pyproj/proj_dir/share/gdal"
    os.environ[
        "PROJ_LIB"
    ] = f"{home}/miniconda3/envs/pyeo_prod_0_8_0_env/lib/python3.10/site-packages/pyproj/proj_dir/share/proj"

    binary_dec = gpd.read_file(path_to_vectorised_binary_filtered)

    # convert first date of change detection in days, to change date
    columns_to_apply = ["rb7_min", "rb7_max", "rb7_mean", "rb7_median"]

    for column in columns_to_apply:
        rb7_zstats_df[column] = rb7_zstats_df[column].apply(serial_date_to_string)

    # table join on id
    merged = binary_dec.merge(rb2_zstats_df, on="id", how="inner")
    merged2 = merged.merge(rb5_zstats_df, on="id", how="inner")
    merged3 = merged2.merge(rb7_zstats_df, on="id", how="inner")
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
    merged["user"] = pd.Series(dtype="string")
    merged["event_class"] = pd.Series(dtype="string")
    merged["follow_up_yes_no"] = pd.Series(dtype="string")
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

    else:
        None

    if write_pkl:
        merged.to_pickle(pkl_fname)
        log.info(f"GeoDataFrame written as pickle, to:  {pkl_fname}")

    else:
        None

    if write_csv:
        merged.to_csv(csv_fname)
        log.info(f"DataFrame written as csv, to:   {csv_fname}")

    else:
        None

    return


def vector_report_generation(
    raster_change_report_path: str,
    write_csv: bool,
    write_shapefile: bool,
    write_pkl: bool,
    log,
    epsg,
    level_1_boundaries_path
):
    """
    This function calls all the individual functions necessary to create a vectorised change report.

    Parameters
    ---------------

    raster_change_report (str):
        path to the report raster

    ----------------
    Returns
    """


    change_report_path = raster_change_report_path
    tileid = change_report_path.split("/")[-1].split("_")[2] # I should take tileid from .ini but this is quicker for now (12/04/2023)

    log.info("--" * 20)
    log.info(f"Starting Vectorisation of the Change Report Raster of Tile: {tileid}")
    log.info("--" * 20)

    # 22 minutes
    path_vectorised_binary = vectorise_from_band(
        change_report_path=change_report_path, band=6, log=log
    )

    # 1.5 minutes
    path_vectorised_binary_filtered = filter_vectorised_band(
        vectorised_band_path=path_vectorised_binary,
        log=log
    )

    # 6 minutes
    rb2_zstats_df = zonal_statistics(
        raster_path=change_report_path,
        shapefile_path=path_vectorised_binary_filtered,
        report_band=2,
        log=log,
    )

    # 6 minutes
    rb5_zstats_df = zonal_statistics(
        raster_path=change_report_path,
        shapefile_path=path_vectorised_binary_filtered,
        report_band=5,
        log=log
    )

    # 6 minutes
    rb7_zstats_df = zonal_statistics(
        raster_path=change_report_path,
        shapefile_path=path_vectorised_binary_filtered,
        report_band=7,
        log=log
    )

    # table joins, area, lat lon, county
    merge_and_calculate_spatial(
        rb2_zstats_df=rb2_zstats_df,
        rb5_zstats_df=rb5_zstats_df,
        rb7_zstats_df=rb7_zstats_df,
        path_to_vectorised_binary_filtered=path_vectorised_binary_filtered,
        write_csv=write_csv,
        write_shapefile=write_shapefile,
        write_pkl=write_pkl,
        change_report_path=change_report_path,
        log=log,
        epsg=epsg,
        level_1_boundaries_path=level_1_boundaries_path
    )

    log.info("--" * 20)
    log.info("Vectorisation of the Change Report Raster complete")
    log.info("--" * 20)

    return
