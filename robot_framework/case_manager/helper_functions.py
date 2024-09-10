"""
This module provides helper functions.
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


def find_name_url_pairs(data: Union[Dict[str, Union[str, dict, list]], list]) -> Dict[str, str]:
    """
    Recursively find all name and URL pairs in a nested dictionary or list,
    specifically looking for 'name' and 'url' keys in 'attachments'.

    Args:
        data (Union[Dict[str, Union[str, dict, list]], list]): The data to search for name-URL pairs.

    Returns:
        Dict[str, str]: A dictionary of name-URL pairs.
    """
    name_url_pairs = {}

    if isinstance(data, dict):
        for key, value in data.items():
            if key == "attachments" and isinstance(value, dict):
                for attachment_key, attachment_value in value.items():
                    if isinstance(attachment_value, dict) and "name" in attachment_value and "url" in attachment_value:
                        name_url_pairs[attachment_value["name"]] = attachment_value["url"]
                        print(attachment_key)
            elif isinstance(value, (dict, list)):
                name_url_pairs.update(find_name_url_pairs(value))

    elif isinstance(data, list):
        for item in data:
            name_url_pairs.update(find_name_url_pairs(item))

    return name_url_pairs


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


def extract_key_value_pairs_from_json(json_data, node_name=None, separator=";#", target_type=str):
    """
    Recursively traverses a JSON object (a dictionary or list) and extracts key-value pairs
    from values that match the specified target type (by default, strings). The key-value pairs
    are extracted from strings using the provided separator. The node to target can be specified
    by name, and the function will find that node anywhere in the structure.

    Parameters:
    -----------
    json_data : dict or list
        The input JSON-like object (nested dictionary or list) to traverse.
    node_name : str, optional
        The name of the node to search for. If None, the function will search the entire JSON
        structure for values that match the target type and contain the separator.
    separator : str, optional
        The separator used in strings to split key-value pairs (default is ";#").
    target_type : type, optional
        The type of values to process for key-value pair extraction. By default, it is `str`,
        so it extracts from strings, but you can specify other types (e.g., list, dict).

    Returns:
    --------
    dict
        A dictionary of extracted key-value pairs from the JSON object.
    """
    result = {}

    def extract_pairs(value):
        """
        Splits a value (usually a string) using the specified separator and creates key-value pairs
        by pairing adjacent items.

        Parameters:
        -----------
        value : str
            The string to be split and processed into key-value pairs.

        Returns:
        --------
        dict
            A dictionary with key-value pairs extracted from the string.
        """
        categories = value.split(separator)
        return {categories[i].strip(): categories[i + 1].strip()
                for i in range(0, len(categories) - 1, 2)}

    def find_and_extract_from_node(data):
        """
        Recursively traverses the JSON structure to find nodes with the specified name and extracts
        key-value pairs from them if they match the target type.

        Parameters:
        -----------
        data : dict, list, or any type
            The JSON-like structure to traverse and extract key-value pairs from.
        """
        if isinstance(data, dict):
            for key, value in data.items():
                if key == node_name and isinstance(value, target_type) and separator in str(value):
                    result.update(extract_pairs(value))
                elif isinstance(value, (dict, list)):
                    find_and_extract_from_node(value)
        elif isinstance(data, list):
            for item in data:
                find_and_extract_from_node(item)

    find_and_extract_from_node(json_data)

    return result
