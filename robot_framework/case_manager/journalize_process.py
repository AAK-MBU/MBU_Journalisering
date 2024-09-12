"""
This module handles the journalization process for case management.
It contains functionality to upload and journalize documents, and manage case data.
"""
import os
import json
from typing import Dict, Any, Optional, List, Tuple
import pyodbc

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from mbu_dev_shared_components.utils.db_stored_procedure_executor import execute_stored_procedure
from mbu_dev_shared_components.os2forms.documents import download_file_bytes
from robot_framework.case_manager.helper_functions import extract_filename_from_url, find_name_url_pairs, extract_key_value_pairs_from_json


class DatabaseError(Exception):
    """Custom exception for database related errors."""


class RequestError(Exception):
    """Custom exception for request related errors."""


def execute_sql_update(conn_string: str, procedure_name: str, params: Dict[str, tuple]) -> None:
    """
    Execute a stored procedure to update data in the database.

    Args:
        conn_string (str): Connection string for the database.
        procedure_name (str): Name of the stored procedure to execute.
        params (Dict[str, tuple]): Parameters for the SQL procedure, in the form {param_name: (param_type, param_value)}.

    Raises:
        DatabaseError: If the SQL procedure execution fails.
    """
    sql_update_result = execute_stored_procedure(conn_string, procedure_name, params)
    if not sql_update_result['success']:
        raise DatabaseError(f"SQL - {procedure_name} failed.")


def log_and_raise_error(orchestrator_connection: OrchestratorConnection, error_message: str, exception: Exception) -> None:
    """
    Log an error and raise the specified exception.

    Args:
        orchestrator_connection (OrchestratorConnection): Connection object to log errors.
        error_message (str): The error message to log.
        exception (Exception): The exception to raise.

    Raises:
        exception: The passed-in exception is raised after logging the error.
    """
    orchestrator_connection.log_error(error_message)
    raise exception


def handle_database_error(
    conn_string: str, procedure_name: str, process_status_params_failed: str, exception: Exception
) -> None:
    """
    Handle database errors by executing the failure procedure and raising the exception.

    Args:
        conn_string (str): Connection string for the database.
        procedure_name (str): Name of the stored procedure to execute upon failure.
        process_status_params_failed (str): Parameters for the failure procedure.
        exception (Exception): The exception that occurred.

    Raises:
        exception: Re-raises the original exception.
    """
    execute_stored_procedure(conn_string, procedure_name, process_status_params_failed)
    raise exception


def get_forms_data(conn_string: str, table_name: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
    """Retrieve the data for the specific form from the database."""
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
    """Retrieve necessary credentials and constants from the orchestrator connection."""
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
) -> Optional[Tuple[str, str]]:
    """
    Perform contact lookup and update the database with the contact information.

    Returns:
        A tuple containing the person's full name and ID if successful, otherwise None.
    """
    try:
        response = case_handler.contact_lookup(ssn, '/borgersager/_goapi/contacts/readitem')
        if not response.ok:
            raise RequestError("Request response failed.")

        person_data = response.json()
        person_full_name = person_data["FullName"]
        person_go_id = person_data["ID"]

        # SQL data update
        sql_data_params = {
            "StepName": ("str", "ContactLookup"),
            "JsonFragment": ("str", json.dumps({"ContactId": person_go_id})),
            "uuid": ("str", uuid),
            "TableName": ("str", table_name)
        }
        execute_sql_update(conn_string, update_response_data, sql_data_params)

        return person_full_name, person_go_id

    except (DatabaseError, RequestError) as e:
        handle_database_error(conn_string, update_process_status, process_status_params_failed, e)
        return None

    except Exception as e:
        handle_database_error(conn_string, update_process_status, process_status_params_failed, RuntimeError(
            f"An unexpected error occurred during contact lookup: {e}"))
        return None


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
) -> Optional[str]:
    """
    Check if a case folder exists for the person and update the database.

    Returns:
        The case folder ID if it exists, otherwise None.
    """
    try:
        search_data = case_data_handler.search_case_folder_data_json(case_type, person_full_name, person_go_id, ssn)
        response = case_handler.search_for_case_folder(search_data, '/_goapi/cases/findbycaseproperties')

        if not response.ok:
            raise RequestError("Request response failed.")

        cases_info = response.json().get('CasesInfo', [])
        case_folder_id = cases_info[0].get('CaseID') if cases_info else None

        if case_folder_id:
            sql_data_params = {
                "StepName": ("str", "CaseFolder"),
                "JsonFragment": ("str", json.dumps({"CaseFolderId": case_folder_id})),
                "uuid": ("str", uuid),
                "TableName": ("str", table_name)
            }
            execute_sql_update(conn_string, update_response_data, sql_data_params)

        return case_folder_id

    except (DatabaseError, RequestError) as e:
        handle_database_error(conn_string, update_process_status, process_status_params_failed, e)
        return None

    except Exception as e:
        handle_database_error(conn_string, update_process_status, process_status_params_failed, RuntimeError(
            f"An unexpected error occurred during case folder check: {e}"))
        return None


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
) -> Optional[str]:
    """
    Create a new case folder if it doesn't exist.

    Returns:
        Optional[str]: The case folder ID if created successfully, otherwise None in case of an error.
    """
    try:
        case_folder_data = case_handler.create_case_folder_data(case_type, person_full_name, person_go_id, ssn)
        response = case_handler.create_case_folder(case_folder_data, '/_goapi/Cases')
        if not response.ok:
            raise RequestError("Request response failed.")

        case_folder_id = response.json()['CaseID']

        sql_data_params = {
            "StepName": ("str", "CaseFolder"),
            "JsonFragment": ("str", json.dumps({"CaseFolderId": case_folder_id})),
            "uuid": ("str", uuid),
            "TableName": ("str", table_name)
        }
        execute_sql_update(conn_string, update_response_data, sql_data_params)

        return case_folder_id

    except (DatabaseError, RequestError) as e:
        handle_database_error(conn_string, update_process_status, process_status_params_failed, e)
        return None

    except Exception as e:
        handle_database_error(conn_string, update_process_status, process_status_params_failed, RuntimeError(
            f"An unexpected error occurred during case folder creation: {e}"))
        return None


def create_case_data(case_handler, case_type: str, oc_args_json: Dict[str, Any], case_title: str, case_folder_id: str, received_date: str) -> Dict[str, Any]:
    """Create the data needed to create a new case."""
    return case_handler.create_case_data(
        case_type,
        oc_args_json['caseCategory'],
        oc_args_json['caseOwnerId'],
        oc_args_json['caseOwnerName'],
        oc_args_json['caseProfileId'],
        oc_args_json['caseProfileName'],
        case_title,
        case_folder_id,
        oc_args_json['supplementaryCaseOwners'],
        oc_args_json['departmentId'],
        oc_args_json['departmentName'],
        oc_args_json['supplementaryDepartments'],
        oc_args_json['kleNumber'],
        oc_args_json['facet'],
        received_date or oc_args_json.get('startDate'),
        oc_args_json['specialGroup'],
        oc_args_json['customMasterCase'],
        True
    )


def determine_case_title(os2form_webform_id: str, person_full_name: str, ssn: str) -> str:
    """Determine the title of the case based on the webform ID."""
    match os2form_webform_id:
        case "tilmelding_til_modersmaalsunderv":
            return f"Modersmålsundervisning {person_full_name}"
        case "indmeldelse_i_modtagelsesklasse":
            return f"Visitering af {person_full_name} {ssn}"
        case "ansoegning_om_koersel_af_skoleel" | "ansoegning_om_midlertidig_koerse":
            return f"Kørsel til {person_full_name}"


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
) -> Optional[str]:
    """
    Create a new case and update the database.

    Returns:
        Optional[str]: The case ID if created successfully, otherwise None in case of an error.
    """
    try:
        case_title = determine_case_title(os2form_webform_id, person_full_name, ssn)
        case_data = create_case_data(case_handler, case_type, oc_args_json, case_title, case_folder_id, received_date)

        response = case_handler.create_case(case_data, '/_goapi/Cases')
        if not response.ok:
            raise RequestError("Request response failed.")

        case_id = response.json()['CaseID']

        sql_data_params = {
            "StepName": ("str", "Case"),
            "JsonFragment": ("str", json.dumps({"CaseId": case_id})),
            "uuid": ("str", uuid),
            "TableName": ("str", table_name)
        }
        execute_sql_update(conn_string, update_response_data, sql_data_params)

        return case_id

    except (DatabaseError, RequestError) as e:
        handle_database_error(conn_string, update_process_status, process_status_params_failed, e)
        return None

    except Exception as e:
        handle_database_error(conn_string, update_process_status, process_status_params_failed, RuntimeError(
            f"An unexpected error occurred during case creation: {e}"))
        return None


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
) -> None:
    """Journalize associated files in the 'Document' folder under the citizen case."""
    try:
        orchestrator_connection.log_trace("Uploading document(s) to the case.")

        urls = find_name_url_pairs(parsed_form_data)
        documents = []
        document_ids = []

        if oc_args_json['documentData']['useCompletedDateFromFormAsDate'] == "True":
            received_date = parsed_form_data['entity']['completed'][0]['value']
        else:
            received_date = ""

        document_category_json = extract_key_value_pairs_from_json(oc_args_json['documentData'], node_name="documentCategory")

        for name, url in urls.items():
            filename = extract_filename_from_url(url)
            file_bytes = download_file_bytes(url, os2_api_key)

            document_category = ""
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

            if not response.ok:
                log_and_raise_error(orchestrator_connection, "An error occurred when uploading the document.", RequestError("Request response failed."))

            document_id = response.json()["DocId"]
            documents.append({"DocumentId": str(document_id)})
            document_ids.append(document_id)
            orchestrator_connection.log_trace("The document was uploaded.")

        table_name = oc_args_json['tableName']
        sql_data_params = {
            "StepName": ("str", "Case Files"),
            "JsonFragment": ("str", json.dumps(documents)),
            "uuid": ("str", uuid),
            "TableName": ("str", table_name)
        }
        execute_sql_update(conn_string, oc_args_json['hubUpdateResponseData'], sql_data_params)

        if oc_args_json['documentData']['journalizeDocuments'] == "True":
            orchestrator_connection.log_trace("Journalizing document.")
            response_journalize_document = document_handler.journalize_document(document_ids, '/_goapi/Documents/MarkMultipleAsCaseRecord/ByDocumentId')
            if not response_journalize_document.ok:
                log_and_raise_error(orchestrator_connection, "An error occurred while journalizing the document.", RequestError("Request response failed."))
            orchestrator_connection.log_trace("Document was journalized.")

        if oc_args_json['documentData']['finalizeDocuments'] == "True":
            orchestrator_connection.log_trace("Finalizing document.")
            response_journalize_document = document_handler.finalize_document(document_ids, '/_goapi/Documents/FinalizeMultiple/ByDocumentId')
            if not response_journalize_document.ok:
                log_and_raise_error(orchestrator_connection, "An error occurred while finalizing the document.", RequestError("Request response failed."))
            orchestrator_connection.log_trace("Document was finalized.")

    except (DatabaseError, RequestError) as e:
        handle_database_error(conn_string, oc_args_json['hubUpdateProcessStatus'], process_status_params_failed, e)

    except Exception as e:
        handle_database_error(conn_string, oc_args_json['hubUpdateProcessStatus'], process_status_params_failed, RuntimeError(
            f"An unexpected error occurred during file journalization: {e}"))
