import asyncio
import logging
import os
import shutil
from datetime import datetime

import geopandas as gpd
import toml

from opensearch_queries_and_processing import (
    get_access_token,
    get_s2_tile_centroids,
    query_by_polygon,
    filter_valid_size_s2_products,
    stratify_products_by_orbit_number,
    download_product,
    check_product_exists,
    unzip_downloaded_product,
)

CONFIG = toml.load(os.path.abspath("config/image_acquisition_config.toml"))

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    filename=f"pyeo_run_{datetime.now().strftime('%d-%b-%Y(%H:%M:%S.%f)')}.log",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOGGER = logging.getLogger("pyeo")
ROI_GEOMETRY = gpd.read_file(CONFIG["sentinel2_properties"]["tiles_to_process"])
ROI_GEOMETRY["centroid"] = ROI_GEOMETRY.representative_point()
TILE_DICT = {
    ROI_GEOMETRY["name"][i]: str(ROI_GEOMETRY["centroid"][i])
    for i in range(ROI_GEOMETRY.shape[0])
}

API_ROOT = "http://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel2/search.json"
DOWNLOAD_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
REFRESH_TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"


async def download(geom_path: str,
                   max_cloud_cover: str,
                   start_date: str,
                   end_date: str,
                   safe_download_path: str,
                   username: str,
                   password: str
                   ) -> None:
    s2_geometry_path = geom_path
    max_cloud_cover = max_cloud_cover
    start_date = start_date
    end_date = end_date
    max_records = 100

    auth_token = get_access_token(username=username, password=password, refresh=False)

    centroids = get_s2_tile_centroids(s2_geometry_path=s2_geometry_path)
    for centroid in centroids.itertuples(index=False):
        tile_name = centroid[2]
        geom = centroid[-1]

        response = query_by_polygon(
            max_cloud_cover=max_cloud_cover,
            start_date=start_date,
            end_date=end_date,
            area_of_interest=geom,
            max_records=max_records,
        )
        response = filter_valid_size_s2_products(response_dataframe=response)
        response = stratify_products_by_orbit_number(response_dataframe=response)

        LOGGER.info(f"Found {len(response)} products for the tile: {tile_name}")

        for product in response.itertuples(index=False):
            product_status = product[1]
            product_name = product[5]
            product_uuid = product[-1]

            if "L1C" in product_name:
                # ToDo: Handle L1C processing
                LOGGER.info(f"Found {product_name} in product list. Skipping as not currently handling L1C.")
                continue

            if check_product_exists(
                write_directory=safe_download_path, product_name=product_name
            ):
                continue  # Product already downloaded, skipping to next product
            else:
                if product_status == "ONLINE":
                    try:
                        LOGGER.info(
                            f"Starting download of file: {product_name} with UUID {product_uuid}."
                        )
                        download_product(
                            product_uuid=product_uuid,
                            auth_token=auth_token,
                            product_name=product_name,
                        )
                        LOGGER.info(
                            f"Finished downloading product: {product_name} with UUID: {product_uuid}."
                        )
                        try:
                            unzip_downloaded_product(
                                write_directory=safe_download_path,
                                product_name=product_name,
                            )
                        except UnboundLocalError:
                            shutil.rmtree(f"{safe_download_path}/{product_name}")
                            LOGGER.info(f"Removed likely incomplete download of product: {product_name}")
                    except:
                        LOGGER.info(
                            f"Download for product: {product_name} with UUID: {product_uuid} failed!"
                        )
                        try:
                            shutil.rmtree(f"{safe_download_path}/{product_name}")
                            LOGGER.info(
                                f"Incomplete or otherwise faulty product: {product_name} removed. This product will have to be redownloaded."
                            )
                        except:
                            LOGGER.info(
                                f"Problem removing product: {product_name}. Does the product exist on disk?"
                            )
                            continue
                        finally:
                            LOGGER.info("Continuing the download.")
                            continue

                else:
                    """
                    The new API seems to have retired the Offline data activation mechanism.
                    Currently, we are not expecting any offline data, but this may change in the future,
                    so the offline data retrieval flow may still need to be implemented. Check legacy_main.py for
                    reference on how to activate offline data (though this mechanism may also change in the future).
                    For now, we are logging the names of offline products should we come across one (unlikely).
                    """
                    LOGGER.info(
                        f"Product: {product_name} is not online. Adding to activation queue."
                    )

    return


if __name__ == "__main__":
    asyncio.run(main())

