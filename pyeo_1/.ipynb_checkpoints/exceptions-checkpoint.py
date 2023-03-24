"""
Contains ForestSentinel exception classes.
"""

import requests


class pyeo_1Exception(Exception):
    pass


class StackImagesException(pyeo_1Exception):
    pass


class CreateNewStacksException(pyeo_1Exception):
    pass


class StackImageException(pyeo_1Exception):
    pass


class BadS2Exception(pyeo_1Exception):
    pass


class BadGoogleURLExceeption(pyeo_1Exception):
    pass


class BadDataSourceExpection(pyeo_1Exception):
    pass


class NoL2DataAvailableException(pyeo_1Exception):
    pass


class FMaskException(pyeo_1Exception):
    pass


class InvalidGeometryFormatException(pyeo_1Exception):
    pass


class NonSquarePixelException(pyeo_1Exception):
    pass

class InvalidDateFormatException(pyeo_1Exception):
    pass

class TooManyRequests(requests.RequestException):
    """Too many requests; do exponential backoff"""