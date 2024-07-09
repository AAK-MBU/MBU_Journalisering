"""This module contains helper functions for the robot process."""

import os
import json
from typing import Dict, Any, Optional, List
import pyodbc

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from mbu_dev_shared_components.utils.db_stored_procedure_executor import execute_stored_procedure
from mbu_dev_shared_components.os2forms.documents import download_file_bytes
from mbu_dev_shared_components.getorganized.documents import upload_file_to_case

from robot_framework.case_manager.url_processing import find_urls, extract_filename_from_url


def get_forms_data(conn_string: str, table_name: str, params: Optional[List[Any]] = None) -> List[str]:
    """Retrieve the data for the specific form"""

    query = f"SELECT uuid, data FROM rpa.rpa.{table_name} WHERE process_status IS NULL"
    with pyodbc.connect(conn_string) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params or [])
            columns = [column[0] for column in cursor.description]
            forms_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return forms_data


def get_credentials_and_constants(orchestrator_connection: OrchestratorConnection) -> Dict[str, Any]:
    """Retrieve necessary credentials and constants."""
    uuid = orchestrator_connection.get_constant('test_uuid').value
    ssn = orchestrator_connection.get_credential('test_person').password

    if not any(ssn) or not any(uuid):
        raise ValueError("No ssn given.")
    credentials = {
        "go_api_endpoint": os.getenv('GoApiBaseUrl'),
        "go_api_username": orchestrator_connection.get_credential('go_api').username,
        "go_api_password": orchestrator_connection.get_credential('go_api').password,
        "os2_api_key": orchestrator_connection.get_credential('os2_api').password,
        "sql_conn_string": orchestrator_connection.get_constant('DbConnectionString').value,
        "journalizing_tmp_path": orchestrator_connection.get_constant('journalizing_tmp_path').value,
    }
    return credentials


def update_status(conn_string: str, sp_name: str, params: str) -> None:
    """Execute stored procedure to update status."""
    execute_stored_procedure(conn_string, sp_name, params)


def contact_lookup(case_handler,
                   ssn: str,
                   conn_string: str,
                   db_update_sp: str,
                   status_sp: str,
                   status_params_failed: str,
                   uuid: str,
                   table_name: str) -> str:
    """Perform contact lookup and update database."""
    person_full_name = None
    person_go_id = None
    response = case_handler.contact_lookup(ssn, '/borgersager/_goapi/contacts/readite')
    print(response)
    print(response.ok)

    if response.ok:
        person_full_name = response.json()["FullName"]
        person_go_id = response.json()["ID"]
        sql_data_params = {
            "StepName": ("str", "ContactLookup"),
            "JsonFragment": ("str", f'{{"ContactId": "{person_go_id}"}}'),
            "uuid": ("str", f'{uuid}'),
            "TableName": ("str", f'{table_name}')
        }
        sql_update_result = execute_stored_procedure(conn_string, db_update_sp, sql_data_params)
        if not sql_update_result['success']:
            update_status(conn_string, status_sp, status_params_failed)
            raise RuntimeError("Contact lookup failed.")
    else:
        update_status(conn_string, status_sp, status_params_failed)
        raise RuntimeError("Contact lookup failed.")
    return person_full_name, person_go_id


def check_case_folder(case_handler,
                      case_data_handler,
                      case_type: str,
                      person_full_name: str,
                      person_go_id: str,
                      ssn: str,
                      conn_string: str,
                      db_update_sp: str,
                      status_sp: str,
                      status_params_failed: str,
                      uuid: str,
                      table_name: str) -> str:
    """Check if case folder exists and update database."""
    search_data = case_data_handler.search_case_folder_data_json(case_type, person_full_name, person_go_id, ssn)
    response = case_handler.search_for_case_folder(search_data, '/_goapi/cases/findbycaseproperties')
    if response.ok:
        case_folder_id = response.json()['CasesInfo'][0]['CaseID']
        sql_data_params = {
            "StepName": ("str", "CaseFolder"),
            "JsonFragment": ("str", f'{{"CaseFolderId": "{case_folder_id}"}}'),
            "uuid": ("str", f'{uuid}'),
            "TableName": ("str", f'{table_name}')
        }
        sql_update_result = execute_stored_procedure(conn_string, db_update_sp, sql_data_params)
        if not sql_update_result['success']:
            update_status(conn_string, status_sp, status_params_failed)
    else:
        update_status(conn_string, status_sp, status_params_failed)
        case_folder_id = None
    return case_folder_id


def create_case_folder(case_handler,
                       case_type: str,
                       person_full_name: str,
                       person_go_id: str,
                       ssn: str,
                       conn_string: str,
                       db_update_sp: str,
                       status_sp: str,
                       status_params_failed: str,
                       uuid: str,
                       table_name: str) -> str:
    """Create a new case folder if it doesn't exist."""
    case_folder_data = case_handler.create_case_folder_data(case_type, person_full_name, person_go_id, ssn)
    response = case_handler.create_case_folder(case_folder_data, '/_goapi/Cases')
    if response.ok:
        case_folder_id = response.json()['CasesInfo'][0]['CaseID']
        sql_data_params = {
            "StepName": ("str", "CaseFolder"),
            "JsonFragment": ("str", f'{{"CaseFolderId": "{case_folder_id}"}}'),
            "uuid": ("str", f'{uuid}'),
            "TableName": ("str", f'{table_name}')
        }
        sql_update_result = execute_stored_procedure(conn_string, db_update_sp, sql_data_params)
        if not sql_update_result['success']:
            update_status(conn_string, status_sp, status_params_failed)
    else:
        update_status(conn_string, status_sp, status_params_failed)
        case_folder_id = None
    return case_folder_id


def create_case(case_handler,
                orchestrator_connection: str,
                person_full_name: str,
                ssn: str,
                case_type: str,
                case_folder_id: str,
                oc_args_json: str,
                conn_string: str,
                db_update_sp: str,
                status_sp: str,
                status_params_failed: str,
                uuid: str,
                table_name: str):
    """Create a new case."""
    match orchestrator_connection.process_name:
        case "Journalisering_Modersmaal":
            case_title = f"Modersmålsundervisning {person_full_name}"
        case "Journalisering_indmeldelse_i_modtagelsesklasse":
            case_title = f"Visitering af {person_full_name} {ssn}"

    case_data = case_handler.create_case_data(
        case_type,
        oc_args_json['case_owner_id'],
        oc_args_json['case_owner_name'],
        oc_args_json['case_profile_id'],
        oc_args_json['case_profile_name'],
        case_title,
        case_folder_id,
        oc_args_json['supplementary_case_owners'],
        oc_args_json['department_id'],
        oc_args_json['department_name'],
        oc_args_json['supplementary_departments'],
        True
    )
    response = case_handler.create_case(case_data, '/_goapi/Cases')
    if response.ok:
        case_id = response.json()['CaseID']
        sql_data_params = {
            "StepName": ("str", "Case"),
            "JsonFragment": ("str", f'{{"CaseId": "{case_id}"}}'),
            "uuid": ("str", f'{uuid}'),
            "TableName": ("str", f'{table_name}')
        }
        sql_update_result = execute_stored_procedure(conn_string, db_update_sp, sql_data_params)
        if not sql_update_result['success']:
            update_status(conn_string, status_sp, status_params_failed)
    else:
        update_status(conn_string, status_sp, status_params_failed)
        case_id = None
    return case_id


def journalize_file(case_id: str,
                    parsed_form: Dict[str, Any],
                    os2_api_key: str,
                    go_api_endpoint: str,
                    go_api_username: str,
                    go_api_password: str,
                    conn_string: str,
                    db_update_sp: str,
                    status_sp: str,
                    status_params_failed: str,
                    uuid: str,
                    table_name: str):
    """Journalize associated files in the 'Document' folder under the citizen case."""
    urls = find_urls(parsed_form)
    documents = []

    for url in urls:
        filename = extract_filename_from_url(url)
        file_bytes = download_file_bytes(url, os2_api_key)
        body = {
            "CaseId": f"{case_id}",
            "ListName": "Dokumenter",
            "FolderPath": "null",
            "FileName": f"{filename}",
            "Metadata": "<z:row xmlns:z=\"#RowsetSchema\" />",
            "Overwrite": "true",
            "Bytes": file_bytes
            }
        endpoint = go_api_endpoint + '/_goapi/Documents/AddToCase'
        response = upload_file_to_case(body, endpoint, go_api_username, go_api_password)
        if response.ok:
            document_id = response.json()["DocId"]
            document_json_str = {"DocumentId": str(document_id)}
            documents.append(document_json_str)
        else:
            update_status(conn_string, status_sp, status_params_failed)

    sql_data_params = {
        "StepName": ("str", "Case Files"),
        "JsonFragment": ("str", f'{json.dumps(documents)}'),
        "uuid": ("str", f'{uuid}'),
        "TableName": ("str", f'{table_name}')
    }
    sql_update_result = execute_stored_procedure(conn_string, db_update_sp, sql_data_params)
    if not sql_update_result['success']:
        update_status(conn_string, status_sp, status_params_failed)
    else:
        update_status(conn_string, status_sp, status_params_failed)
