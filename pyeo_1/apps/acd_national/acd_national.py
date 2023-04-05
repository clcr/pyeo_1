def acd_national():
    """
    This function:
        - acts as the singular call to run automatic change detection per tile, then aggregate to national, then distribute the change alerts.

    """

def acd_roi_tile_intersection():
    """
    
    This function:
        - accepts a Region of Interest (RoI) and writes a tilelist.txt of the Sentinel-2 tiles that the RoI covers.
    
        - the tilelist.txt is then used to perform the tile-based processes, raster and vector.

    """

def acd_by_tile_rasterisation():
    """
    
    This function:
        - queries available Sentinel-2 imagery that matches the tilelist and environmental parameters specified in the pyeo.ini file

        - for the composite, this downloads Sentinel-2 imagery, applies atmospheric correction (if necessary), converts from .jp2 to .tif and applies cloud masking

        - for the change images, this downloads Sentinel-2 imagery, applies atmospheric correction (if necessary), converts from .jp2 to .tif and applies cloud masking

        - classifies the composite and change images

        - performs change detection between the classified composite and the classified change images, searching for land cover changes specified in the from_classes and to_classes in the pyeo.ini

        - outputs a change report raster with dates land cover change


    """

def acd_by_tile_vectorisation():
    """
    
    This function:
        - Vectorises the change report raster

        - 

    """

def acd_national_integration():
    """

    This function:
        - Collects the per tile vectorised change reports and aggregates them to the national scale.

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

def acd_national_distribution():
    """

    This function:
        - Once the user is happy and has verified the change alerts, Maps.Me and WhatsApp messages are sent from this function.
    
    """

