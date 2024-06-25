"""This module contains the main process of the robot."""

import os
import json

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from mbu_dev_shared_components.utils.db_stored_procedure_executor import execute_stored_procedure
from mbu_dev_shared_components.getorganized.objects import CaseDataJson

from robot_framework.case_manager.case_handler import CaseHandler


def process(orchestrator_connection: OrchestratorConnection) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")


if __name__ == "__main__":
    oc = OrchestratorConnection.create_connection_from_args()
    oc_args_json = json.loads(oc.process_arguments)

    UUID = oc.get_constant('test_UUID').value
    SSN = oc.get_credential('test_person').password

    if not any(SSN) or not any(UUID):
        raise ValueError("No SSN given. ")

    ENDPOINT = os.getenv('GoApiBaseUrl')
    USERNAME = oc.get_credential('go_api').username
    PASSWORD = oc.get_credential('go_api').password
    CONN_STRING = oc.get_constant('DbConnectionString').value
    TABLE_NAME = oc_args_json['table_name']
    CASE_TYPE = oc_args_json['case_type']
    STATUS_SP = oc_args_json['status_sp']
    DB_UPDATE_SP = oc_args_json['db_update_sp']
    STATUS_PARAMS_FAILED = {
        "Status": "FAILED",
        "UUID": f'{UUID}',
        "TableName": f'{TABLE_NAME}'
    }

    case_handler = CaseHandler(ENDPOINT, USERNAME, PASSWORD)

    # 1. contact lookup
    contact_lookup_response = case_handler.contact_lookup(SSN, '/borgersager/_goapi/contacts/readitem')
    if contact_lookup_response.ok:
        person_full_name = contact_lookup_response.json()["FullName"]
        person_id = contact_lookup_response.json()["ID"]
        response_data_params = {
            "StepName": ("str", "contact_lookup"),
            "JsonFragment": ("str", f'{{"ContactId": "{person_id}"}}'),
            "UUID": ("str", f'{UUID}'),
            "TableName": ("str", f'{TABLE_NAME}')
        }
        db_update_result = execute_stored_procedure(CONN_STRING, DB_UPDATE_SP, response_data_params)
        if db_update_result['success'] is False:
            execute_stored_procedure(CONN_STRING, STATUS_SP, STATUS_PARAMS_FAILED)
    else:
        execute_stored_procedure(CONN_STRING, STATUS_SP, STATUS_PARAMS_FAILED)

    case_data_handler = CaseDataJson()

    # 2. check if case folder exists
    search_data = case_data_handler.search_case_folder_data_json(CASE_TYPE, person_full_name, person_id, SSN)
    search_case_folder_response = case_handler.search_for_case_folder(search_data, '/_goapi/cases/findbycaseproperties')
    if search_case_folder_response.ok:
        case_folder_id = search_case_folder_response.json()['CasesInfo'][0]['CaseID']
        response_data_params = {
            "StepName": ("str", "find_by_case_properties"),
            "JsonFragment": ("str", f'{{"CaseFolderId": "{case_folder_id}"}}'),
            "UUID": ("str", f'{UUID}'),
            "TableName": ("str", f'{TABLE_NAME}')
        }
        db_update_result = execute_stored_procedure(CONN_STRING, DB_UPDATE_SP, response_data_params)
        if db_update_result['success'] is False:
            execute_stored_procedure(CONN_STRING, STATUS_SP, STATUS_PARAMS_FAILED)
    else:
        execute_stored_procedure(CONN_STRING, STATUS_SP, STATUS_PARAMS_FAILED)

    # 3. create case folder
    if not case_folder_id:
        case_folder_data = case_handler.create_case_folder_data(CASE_TYPE, person_full_name, person_id, SSN)
        create_case_folder_response = case_handler.create_case_folder(case_folder_data, '/_goapi/Cases')
        case_folder_id = create_case_folder_response['CaseID']

    # 4. create case
    match oc.process_name:
        case "Journalisering_Modersmaal":
            case_title = f"Modersmålsundervisning {person_full_name}"
        case "Journalisering_indmeldelse_i_modtagelsesklasse":
            case_title = f"Visitering af {person_full_name} {SSN}"

    case_data = case_handler.create_case_data(
        CASE_TYPE,
        oc_args_json['case_owner_id'],
        oc_args_json['case_owner_name'],
        oc_args_json['case_profile_id'],
        oc_args_json['case_profile_name'],
        case_title,
        case_folder_id,
        oc_args_json['supplementary_case_owner_id'],
        oc_args_json['supplementary_case_owner_name'],
        oc_args_json['department_id'],
        oc_args_json['department_name'],
        oc_args_json['supplementary_department_id'],
        oc_args_json['supplementary_department_name'],
        True
    )
    case_response = case_handler.create_case(case_data, '/_goapi/Cases')
    print(case_response.content)
