"""
This module provides utility functions for working with URLs within nested
data structures. It includes functions to check if a string is a URL,
find all URLs within nested dictionaries or lists, and extract filenames
from URLs.
"""
import re
from typing import Dict, List, Union
from urllib.parse import urlparse, unquote


def _is_url(string: str) -> bool:
    """
    Check if a given string is a valid URL.

    Args:
        string (str): The string to be checked.

    Returns:
        bool: True if the string is a valid URL, False otherwise.
    """
    url_pattern = re.compile(
        r'^(https?://)?'
        r'([a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,6})'
        r'(:[0-9]{1,5})?'
        r'(/.*)?$', re.IGNORECASE)
    return re.match(url_pattern, string) is not None


def find_urls(data: Union[Dict[str, Union[str, dict, list]], list]) -> List[str]:
    """
    Recursively find all URLs in a nested dictionary or list.

    Args:
        data (Union[Dict[str, Union[str, dict, list]], list]): The data to search for URLs.

    Returns:
        List[str]: A list of found URLs.
    """
    urls = []
    if isinstance(data, dict):
        for _, value in data.items():
            if isinstance(value, (dict, list)):
                urls += find_urls(value)
            elif isinstance(value, str) and _is_url(value):
                urls.append(value)
    elif isinstance(data, list):
        for item in data:
            urls += find_urls(item)
    return urls


def extract_filename_from_url(url: str) -> str:
    """
    Extract the filename from a given URL.

    Args:
        url (str): The URL to extract the filename from.

    Returns:
        str: The extracted filename.
    """
    parsed_url = urlparse(url)
    path_segments = parsed_url.path.split('/')
    filename = path_segments[-1]
    original_filename = unquote(filename)
    return original_filename
