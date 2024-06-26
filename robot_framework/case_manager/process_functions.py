"""This module contains helper functions for the robot process."""

import os

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from mbu_dev_shared_components.utils.db_stored_procedure_executor import execute_stored_procedure


def get_credentials_and_constants(orchestrator_connection: OrchestratorConnection):
    """Retrieve necessary credentials and constants."""
    uuid = orchestrator_connection.get_constant('test_uuid').value
    ssn = orchestrator_connection.get_credential('test_person').password
    if not any(ssn) or not any(uuid):
        raise ValueError("No ssn given.")
    credentials = {
        "endpoint": os.getenv('GoApiBaseUrl'),
        "username": orchestrator_connection.get_credential('go_api').username,
        "password": orchestrator_connection.get_credential('go_api').password,
        "conn_string": orchestrator_connection.get_constant('DbConnectionString').value,
        "uuid": uuid,
        "ssn": ssn
    }
    return credentials


def update_status(conn_string, sp_name, params):
    """Execute stored procedure to update status."""
    execute_stored_procedure(conn_string, sp_name, params)


def contact_lookup(case_handler, ssn, conn_string, db_update_sp, status_sp, status_params_failed, uuid, table_name):
    """Perform contact lookup and update database."""
    response = case_handler.contact_lookup(ssn, '/borgersager/_goapi/contacts/readitem')
    person_full_name = None
    person_go_id = None

    if response.ok:
        person_full_name = response.json()["FullName"]
        person_go_id = response.json()["ID"]
        response_data_params = {
            "StepName": ("str", "contact_lookup"),
            "JsonFragment": ("str", f'{{"ContactId": "{person_go_id}"}}'),
            "uuid": ("str", f'{uuid}'),
            "TableName": ("str", f'{table_name}')
        }
        db_update_result = execute_stored_procedure(conn_string, db_update_sp, response_data_params)
        if not db_update_result['success']:
            update_status(conn_string, status_sp, status_params_failed)
    else:
        update_status(conn_string, status_sp, status_params_failed)
    return person_full_name, person_go_id


def check_case_folder(case_handler, case_data_handler, case_type, person_full_name, person_go_id, ssn, conn_string, db_update_sp, status_sp, status_params_failed, uuid, table_name):
    """Check if case folder exists and update database."""
    search_data = case_data_handler.search_case_folder_data_json(case_type, person_full_name, person_go_id, ssn)
    response = case_handler.search_for_case_folder(search_data, '/_goapi/cases/findbycaseproperties')
    if response.ok:
        case_folder_id = response.json()['CasesInfo'][0]['CaseID']
        response_data_params = {
            "StepName": ("str", "find_by_case_properties"),
            "JsonFragment": ("str", f'{{"CaseFolderId": "{case_folder_id}"}}'),
            "uuid": ("str", f'{uuid}'),
            "TableName": ("str", f'{table_name}')
        }
        db_update_result = execute_stored_procedure(conn_string, db_update_sp, response_data_params)
        if not db_update_result['success']:
            update_status(conn_string, status_sp, status_params_failed)
    else:
        update_status(conn_string, status_sp, status_params_failed)
        case_folder_id = None
    return case_folder_id


def create_case_folder(case_handler, case_type, person_full_name, person_go_id, ssn):
    """Create a new case folder if it doesn't exist."""
    case_folder_data = case_handler.create_case_folder_data(case_type, person_full_name, person_go_id, ssn)
    response = case_handler.create_case_folder(case_folder_data, '/_goapi/Cases')
    return response['CaseID']


def create_case(case_handler, orchestrator_connection, person_full_name, ssn, case_type, case_folder_id, oc_args_json):
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
    print(case_data)
    response = case_handler.create_case(case_data, '/_goapi/Cases')
    return response
