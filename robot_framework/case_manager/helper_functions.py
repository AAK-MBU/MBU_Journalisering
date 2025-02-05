"""
This module provides helper functions.
"""
import re
import os
from typing import Dict, List, Union
from urllib.parse import urlparse, unquote
import json
from io import BytesIO
import pyodbc
from itk_dev_shared_components.smtp import smtp_util


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

    def extract_attachments(attachments: dict):
        """Extract name-URL pairs from attachments."""
        for attachment_value in attachments.values():
            if isinstance(attachment_value, dict) and "name" in attachment_value and "url" in attachment_value:
                name_url_pairs[attachment_value["name"]] = attachment_value["url"]

    def extract_linked(linked: dict):
        """Extract id-URL pairs from linked items."""
        for linked_value in linked.values():
            if isinstance(linked_value, dict):
                for item_data in linked_value.values():
                    if isinstance(item_data, dict) and "id" in item_data and "url" in item_data:
                        name_url_pairs[item_data["id"]] = item_data["url"]

    def recursive_search(data):
        """Recursively search for attachments and linked sections in nested structures."""
        if isinstance(data, dict):
            for key, value in data.items():
                if key == "attachments" and isinstance(value, dict):
                    extract_attachments(value)
                elif key == "linked" and isinstance(value, dict):
                    extract_linked(value)
                elif isinstance(value, (dict, list)):
                    recursive_search(value)
        elif isinstance(data, list):
            for item in data:
                recursive_search(item)

    recursive_search(data)

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


def extract_filename_from_url_without_extension(url: str) -> str:
    """
    Extract the filename from a given URL without the extension.

    Args:
        url (str): The URL to extract the filename from.

    Returns:
        str: The extracted filename without extension.
    """
    parsed_url = urlparse(url)
    path_segments = parsed_url.path.split('/')
    filename = path_segments[-1]
    original_filename = unquote(filename)
    filename_without_extension, _ = os.path.splitext(original_filename)
    return filename_without_extension


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
        return {categories[i+1].strip(): categories[i].strip()
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


def fetch_case_metadata(connection_string, os2formwebform_id):
    """Retrieve metadata for a specific os2formWebformId."""
    try:
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT os2formWebformId, caseType, spUpdateResponseData,
                spUpdateProcessStatus, caseData, documentData
                FROM [RPA].[journalizing].[Metadata]
                WHERE os2formWebformId = ?;""",
                (os2formwebform_id,)
            )
            row = cursor.fetchone()
            if row is not None:

                try:
                    case_data_parsed = json.loads(row.caseData) if row.caseData else None
                    document_data_parsed = json.loads(row.documentData) if row.documentData else None

                    # Clean up the case data by removing non-breaking spaces
                    case_data_parsed = {key: value.replace('\xa0', '') if isinstance(value, str) else value for key, value in case_data_parsed.items()}

                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON data: {e}")
                    case_data_parsed = None
                    document_data_parsed = None

                case_metadata = {
                    'os2formWebformId': row.os2formWebformId,
                    'caseType': row.caseType,
                    'spUpdateResponseData': row.spUpdateResponseData,
                    'spUpdateProcessStatus': row.spUpdateProcessStatus,
                    'caseData': case_data_parsed,
                    'documentData': document_data_parsed
                }
                return case_metadata

            print("No data found for the given os2formWebformId.")
            return None

    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def notify_stakeholders(form_type, case_id, case_title, orchestrator_connection, error_message, attachment_bytes):
    """Notify stakeholders about the journalized case."""
    try:
        email_sender = orchestrator_connection.get_constant("e-mail_noreply").value
        email_subject = None
        email_body = None
        email_recipient = None
        caseid = case_id if case_id else "Ukendt"
        casetitle = case_title if case_title else "Ukendt"

        if error_message:
            email_recipient = "rpa@mbu.aarhus.dk"
            email_subject = "Fejl ved journalisering af sag"
            email_body = (
                f"<p>Der opstod en fejl ved journalisering af en sag.</p>"
                f"<p>"
                f"<strong>Sagsid:</strong> {caseid}<br>"
                f"<strong>Sagstitel:</strong> {casetitle}<br>"
                f"<strong>Fejlbesked:</strong> {error_message}"
                f"</p>"
            )

        if form_type in ("indmeld_kraenkelser_af_boern", "respekt_for_graenser_privat", "respekt_for_graenser"):
            email_recipient = "respekt@mbu.aarhus.dk"
            email_subject = "Ny sag er blevet journaliseret: Respekt For Grænser"
            email_body = (
                f"<p>Vi vil informere dig om, at en ny sag er blevet journaliseret.</p>"
                f"<p>"
                f"<strong>Sagsid:</strong> {caseid}<br>"
                f"<strong>Sagstitel:</strong> {casetitle}"
                f"</p>"
            )

        if form_type in ("pasningstid", "anmeldelse_af_hjemmeundervisning"):
            subject_dict = {
                "pasningstid": "Ændring af pasningstid i forbindelse med barselsorlov",
                "anmeldelse_af_hjemmeundervisning": "Erklæring af hjemmeundervisning"
            }
            email_recipient = "pladsanvisningen@mbu.aarhus.dk"
            email_subject = f"Ny sag er blevet journaliseret: {subject_dict.get(form_type)}"
            email_body = (
                f"<p>Vi vil informere dig om, at en ny sag er blevet journaliseret.</p>"
                f"<p>"
                f"<strong>Sagsid:</strong> {caseid}<br>"
                f"<strong>Sagstitel:</strong> {casetitle}"
                f"</p>"
            )

        attachments = []
        if attachment_bytes:
            attachment_file = BytesIO(attachment_bytes)
            attachments.append(smtp_util.EmailAttachment(file=attachment_file, file_name=f"journalisering_{caseid}.pdf"))

        if email_recipient is not None:
            smtp_util.send_email(
                receiver=email_recipient,
                sender=email_sender,
                subject=email_subject,
                body=email_body,
                html_body=email_body,
                smtp_server="smtp.aarhuskommune.local",
                smtp_port=25,
                attachments=attachments if attachments else None
            )
            orchestrator_connection.log_trace("Notification sent to stakeholder")
        else:
            orchestrator_connection.log_trace("Stakeholders not notified. No recipient found for notification")

    except Exception as e:
        orchestrator_connection.log_trace(f"Error sending notification mail, {case_id}: {e}")
        print(f"Error sending notification mail, {case_id}: {e}")
