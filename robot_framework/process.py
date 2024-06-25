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
    oc.log_info(oc.get_constant('test_UUID').value)

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
    PERSON_FULL_NAME = None
    PERSON_GO_ID = None

    case_handler = CaseHandler(ENDPOINT, USERNAME, PASSWORD)

    # 1. contact lookup
    oc.log_info("contact lookup")
    contact_lookup_response = case_handler.contact_lookup(SSN, '/borgersager/_goapi/contacts/readitem')

    if contact_lookup_response.ok:
        PERSON_FULL_NAME = contact_lookup_response.json()["FullName"]
        PERSON_GO_ID = contact_lookup_response.json()["ID"]
        response_data_params = {
            "StepName": ("str", "contact_lookup"),
            "JsonFragment": ("str", f'{{"ContactId": "{PERSON_GO_ID}"}}'),
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
    oc.log_info("check if case folder exists")
    search_data = case_data_handler.search_case_folder_data_json(CASE_TYPE, PERSON_FULL_NAME, PERSON_GO_ID, SSN)
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
        oc.log_info("create case folder")
        case_folder_data = case_handler.create_case_folder_data(CASE_TYPE, PERSON_FULL_NAME, PERSON_GO_ID, SSN)
        create_case_folder_response = case_handler.create_case_folder(case_folder_data, '/_goapi/Cases')
        case_folder_id = create_case_folder_response['CaseID']

    # 4. create case
    oc.log_info("create case")
    match oc.process_name:
        case "Journalisering_Modersmaal":
            case_title = f"Modersmålsundervisning {PERSON_FULL_NAME}"
        case "Journalisering_indmeldelse_i_modtagelsesklasse":
            case_title = f"Visitering af {PERSON_FULL_NAME} {SSN}"

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
