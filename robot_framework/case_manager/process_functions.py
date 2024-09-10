"""This module contains helper functions for the robot process."""
import os
import json
from typing import Dict, Any, Optional, List
import pyodbc

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from mbu_dev_shared_components.utils.db_stored_procedure_executor import execute_stored_procedure
from mbu_dev_shared_components.os2forms.documents import download_file_bytes
from robot_framework.case_manager.helper_functions import extract_filename_from_url, find_name_url_pairs, extract_key_value_pairs_from_json


class DatabaseError(Exception):
    """Custom exception for database related errors."""


class RequestError(Exception):
    """Custom exception for request related errors."""


def get_forms_data(conn_string: str, table_name: str, params: Optional[List[Any]] = None) -> List[str]:
    """Retrieve the data for the specific form"""
    try:
        query = f"SELECT uuid, data FROM rpa.rpa.{table_name} WHERE process_status IS NULL"
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params or [])
                columns = [column[0] for column in cursor.description]
                forms_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return forms_data
    except pyodbc.Error as e:
        raise SystemExit(e) from e


def get_credentials_and_constants(orchestrator_connection: OrchestratorConnection) -> Dict[str, Any]:
    """Retrieve necessary credentials and constants."""
    try:
        credentials = {
            "go_api_endpoint": os.getenv('GoApiBaseUrl'),
            "go_api_username": orchestrator_connection.get_credential('go_api').username,
            "go_api_password": orchestrator_connection.get_credential('go_api').password,
            "os2_api_key": orchestrator_connection.get_credential('os2_api').password,
            "sql_conn_string": orchestrator_connection.get_constant('DbConnectionString').value,
            "journalizing_tmp_path": orchestrator_connection.get_constant('journalizing_tmp_path').value,
        }
        return credentials
    except AttributeError as e:
        raise SystemExit(e) from e


def contact_lookup(
    case_handler,
    ssn: str,
    conn_string: str,
    update_response_data: str,
    update_process_status: str,
    process_status_params_failed: str,
    uuid: str,
    table_name: str
) -> str:
    """Perform contact lookup and update database."""
    try:
        response = case_handler.contact_lookup(ssn, '/borgersager/_goapi/contacts/readitem')
        if response.ok:
            person_full_name = response.json()["FullName"]
            person_go_id = response.json()["ID"]
            sql_data_params = {
                "StepName": ("str", "ContactLookup"),
                "JsonFragment": ("str", f'{{"ContactId": "{person_go_id}"}}'),
                "uuid": ("str", f'{uuid}'),
                "TableName": ("str", f'{table_name}')
            }
            sql_update_result = execute_stored_procedure(conn_string, update_response_data, sql_data_params)
            if not sql_update_result['success']:
                raise DatabaseError("SQL - Update response data failed.")
        else:
            raise RequestError("Request response failed.")
        return person_full_name, person_go_id
    except (DatabaseError, RequestError) as e:
        execute_stored_procedure(conn_string, update_process_status, process_status_params_failed)
        raise e
    except Exception as e:
        execute_stored_procedure(conn_string, update_process_status, process_status_params_failed)
        raise RuntimeError(f"An unexpected error occurred during contact lookup: {e}") from e


def check_case_folder(
    case_handler,
    case_data_handler,
    case_type: str,
    person_full_name: str,
    person_go_id: str,
    ssn: str,
    conn_string: str,
    update_response_data: str,
    update_process_status: str,
    process_status_params_failed: str,
    uuid: str,
    table_name: str
) -> str:
    """Check if case folder exists and update database."""
    try:
        search_data = case_data_handler.search_case_folder_data_json(case_type, person_full_name, person_go_id, ssn)
        response = case_handler.search_for_case_folder(search_data, '/_goapi/cases/findbycaseproperties')
        if response.ok:
            cases_info = response.json().get('CasesInfo', [])
            if cases_info:
                case_folder_id = cases_info[0].get('CaseID', None)
            else:
                case_folder_id = None

            if case_folder_id:
                sql_data_params = {
                    "StepName": ("str", "CaseFolder"),
                    "JsonFragment": ("str", f'{{"CaseFolderId": "{case_folder_id}"}}'),
                    "uuid": ("str", f'{uuid}'),
                    "TableName": ("str", f'{table_name}')
                }
                sql_update_result = execute_stored_procedure(conn_string, update_response_data, sql_data_params)
                if not sql_update_result['success']:
                    raise DatabaseError("SQL - Update response data failed.")
            else:
                print("No CaseID found. Setting case_folder_id to None.")
                case_folder_id = None
        else:
            raise RequestError("Request response failed.")
        return case_folder_id
    except (DatabaseError, RequestError) as e:
        execute_stored_procedure(conn_string, update_process_status, process_status_params_failed)
        raise e
    except Exception as e:
        execute_stored_procedure(conn_string, update_process_status, process_status_params_failed)
        raise RuntimeError(f"An unexpected error occurred during case folder check: {e}") from e


def create_case_folder(
    case_handler,
    case_type: str,
    person_full_name: str,
    person_go_id: str,
    ssn: str,
    conn_string: str,
    update_response_data: str,
    update_process_status: str,
    process_status_params_failed: str,
    uuid: str,
    table_name: str
) -> str:
    """Create a new case folder if it doesn't exist."""
    try:
        case_folder_data = case_handler.create_case_folder_data(case_type, person_full_name, person_go_id, ssn)
        response = case_handler.create_case_folder(case_folder_data, '/_goapi/Cases')
        if response.ok:
            case_folder_id = response.json()['CaseID']
            sql_data_params = {
                "StepName": ("str", "CaseFolder"),
                "JsonFragment": ("str", f'{{"CaseFolderId": "{case_folder_id}"}}'),
                "uuid": ("str", f'{uuid}'),
                "TableName": ("str", f'{table_name}')
            }
            sql_update_result = execute_stored_procedure(conn_string, update_response_data, sql_data_params)
            if not sql_update_result['success']:
                raise DatabaseError("SQL - Update response data failed.")
        else:
            raise RequestError("Request response failed.")
        return case_folder_id
    except (DatabaseError, RequestError) as e:
        execute_stored_procedure(conn_string, update_process_status, process_status_params_failed)
        raise e
    except Exception as e:
        execute_stored_procedure(conn_string, update_process_status, process_status_params_failed)
        raise RuntimeError(f"An unexpected error occurred during case folder creation: {e}") from e


def create_case(
    case_handler,
    os2form_webform_id: str,
    case_type: str,
    oc_args_json: str,
    conn_string: str,
    update_response_data: str,
    update_process_status: str,
    process_status_params_failed: str,
    uuid: str,
    table_name: str,
    ssn: str = None,
    person_full_name: str = None,
    case_folder_id: str = None,
    received_date: str = None
) -> Any:
    """Create a new case."""
    try:
        #  Define the title of the case for each webform id.
        match os2form_webform_id:
            case "tilmelding_til_modersmaalsunderv":
                case_title = f"Modersmålsundervisning {person_full_name}"
            case "indmeldelse_i_modtagelsesklasse":
                case_title = f"Visitering af {person_full_name} {ssn}"
            case "ansoegning_om_koersel_af_skoleel" | "ansoegning_om_midlertidig_koerse":
                case_title = f"Kørsel til {person_full_name}"

        case_data = case_handler.create_case_data(
            case_type,
            oc_args_json['case_category'],
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
            oc_args_json['kle_number'],
            oc_args_json['facet'],
            received_date or oc_args_json.get('start_date'),
            oc_args_json['special_group'],
            oc_args_json['custom_master_case'],
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
            sql_update_result = execute_stored_procedure(conn_string, update_response_data, sql_data_params)
            if not sql_update_result['success']:
                raise DatabaseError("SQL - Update response data failed.")
        else:
            raise RequestError("Request response failed.")
        return case_id
    except (DatabaseError, RequestError) as e:
        execute_stored_procedure(conn_string, update_process_status, process_status_params_failed)
        raise e
    except Exception as e:
        execute_stored_procedure(conn_string, update_process_status, process_status_params_failed)
        raise RuntimeError(f"An unexpected error occurred during case creation: {e}") from e


def journalize_file(
    document_handler,
    case_id: str,
    parsed_form_data: Dict[str, Any],
    os2_api_key: str,
    conn_string: str,
    process_status_params_failed: str,
    uuid: str,
    oc_args_json: str,
    orchestrator_connection: OrchestratorConnection
):
    """Journalize associated files in the 'Document' folder under the citizen case."""
    try:
        orchestrator_connection.log_trace("Uploading document(s) to the case.")

        urls = find_name_url_pairs(parsed_form_data)
        documents = []
        document_ids = []

        if oc_args_json['document_data']['use_completed_date_from_form_as_date'] == "True":
            received_date = parsed_form_data['entity']['completed'][0]['value']

        document_category_json = extract_key_value_pairs_from_json(oc_args_json['document_data'], node_name="document_category")

        for name, url in urls.items():
            filename = extract_filename_from_url(url)
            file_bytes = download_file_bytes(url, os2_api_key)

            for key, value in document_category_json.items():
                if value == name:
                    document_category = key

            document_data = document_handler.create_document_metadata(
                case_id=case_id,
                filename=filename,
                data_in_bytes=list(file_bytes),
                document_date=received_date,
                document_title=name,
                document_receiver="",
                document_category=document_category,
                overwrite="true"
            )

            orchestrator_connection.log_trace("Uploading document(s).")
            response = document_handler.upload_document(document_data, '/_goapi/Documents/AddToCase')

            if response.ok:
                document_id = response.json()["DocId"]
                document_json_str = {"DocumentId": str(document_id)}
                documents.append(document_json_str)
                document_ids.append(document_id)
                orchestrator_connection.log_trace("The document was uploaded.")
            else:
                orchestrator_connection.log_error("An error occured when trying to upload the document to the case.")
                raise RequestError("Request response failed.")

        if oc_args_json['document_data']['journalize_documents'] == "True":
            orchestrator_connection.log_trace("Journalizing document.")
            response_journalize_document = document_handler.journalize_document(document_ids, '/_goapi/Documents/MarkMultipleAsCaseRecord/ByDocumentId')
            if not response_journalize_document.ok:
                orchestrator_connection.log_error("An error occured when trying to journalizing the document.")
                raise RequestError("Request response failed.")
            orchestrator_connection.log_trace("Document was journalized.")

        if oc_args_json['document_data']['finalize_documents'] == "True":
            orchestrator_connection.log_trace("Finalizing document.")
            response_journalize_document = document_handler.finalize_document(document_ids, '/_goapi/Documents/FinalizeMultiple/ByDocumentId')
            if not response_journalize_document.ok:
                orchestrator_connection.log_error("An error occured when trying to journalizing the document.")
                raise RequestError("Request response failed.")
            orchestrator_connection.log_trace("Document was finalized.")

        table_name = oc_args_json['table_name']
        sql_data_params = {
            "StepName": ("str", "Case Files"),
            "JsonFragment": ("str", f'{json.dumps(documents)}'),
            "uuid": ("str", f'{uuid}'),
            "TableName": ("str", f'{table_name}')
        }
        sql_update_result = execute_stored_procedure(conn_string, oc_args_json['hub_update_response_data'], sql_data_params)
        if not sql_update_result['success']:
            raise DatabaseError("SQL - Update response data failed.")

    except (DatabaseError, RequestError) as e:
        execute_stored_procedure(conn_string, oc_args_json['hub_update_process_status'], process_status_params_failed)
        raise e

    except Exception as e:
        execute_stored_procedure(conn_string, oc_args_json['hub_update_process_status'], process_status_params_failed)
        raise RuntimeError(f"An unexpected error occurred during file journalization: {e}") from e
