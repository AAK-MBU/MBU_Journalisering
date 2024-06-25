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
    oc_args_json = json.loads(orchestrator_connection.process_arguments)
    orchestrator_connection.log_info(orchestrator_connection.get_constant('test_uuid').value)

    uuid = orchestrator_connection.get_constant('test_uuid').value
    ssn = orchestrator_connection.get_credential('test_person').password

    if not any(ssn) or not any(uuid):
        raise ValueError("No ssn given. ")

    endpoint = os.getenv('GoApiBaseUrl')
    username = orchestrator_connection.get_credential('go_api').username
    password = orchestrator_connection.get_credential('go_api').password
    conn_string = orchestrator_connection.get_constant('DbConnectionString').value
    table_name = oc_args_json['table_name']
    case_type = oc_args_json['case_type']
    status_sp = oc_args_json['status_sp']
    db_update_sp = oc_args_json['db_update_sp']
    status_params_failed = {
        "Status": "FAILED",
        "uuid": f'{uuid}',
        "TableName": f'{table_name}'
    }
    person_full_name = None
    person_go_id = None

    case_handler = CaseHandler(endpoint, username, password)

    # 1. contact lookup
    orchestrator_connection.log_info("contact lookup")
    contact_lookup_response = case_handler.contact_lookup(ssn, '/borgersager/_goapi/contacts/readitem')

    if contact_lookup_response.ok:
        person_full_name = contact_lookup_response.json()["FullName"]
        person_go_id = contact_lookup_response.json()["ID"]
        response_data_params = {
            "StepName": ("str", "contact_lookup"),
            "JsonFragment": ("str", f'{{"ContactId": "{person_go_id}"}}'),
            "uuid": ("str", f'{uuid}'),
            "TableName": ("str", f'{table_name}')
        }
        db_update_result = execute_stored_procedure(conn_string, db_update_sp, response_data_params)
        if db_update_result['success'] is False:
            execute_stored_procedure(conn_string, status_sp, status_params_failed)
    else:
        execute_stored_procedure(conn_string, status_sp, status_params_failed)

    case_data_handler = CaseDataJson()

    # 2. check if case folder exists
    orchestrator_connection.log_info("check if case folder exists")
    search_data = case_data_handler.search_case_folder_data_json(case_type, person_full_name, person_go_id, ssn)
    search_case_folder_response = case_handler.search_for_case_folder(search_data, '/_goapi/cases/findbycaseproperties')
    if search_case_folder_response.ok:
        case_folder_id = search_case_folder_response.json()['CasesInfo'][0]['CaseID']
        response_data_params = {
            "StepName": ("str", "find_by_case_properties"),
            "JsonFragment": ("str", f'{{"CaseFolderId": "{case_folder_id}"}}'),
            "uuid": ("str", f'{uuid}'),
            "TableName": ("str", f'{table_name}')
        }
        db_update_result = execute_stored_procedure(conn_string, db_update_sp, response_data_params)
        if db_update_result['success'] is False:
            execute_stored_procedure(conn_string, status_sp, status_params_failed)
    else:
        execute_stored_procedure(conn_string, status_sp, status_params_failed)

    # 3. create case folder
    if not case_folder_id:
        orchestrator_connection.log_info("create case folder")
        case_folder_data = case_handler.create_case_folder_data(case_type, person_full_name, person_go_id, ssn)
        create_case_folder_response = case_handler.create_case_folder(case_folder_data, '/_goapi/Cases')
        case_folder_id = create_case_folder_response['CaseID']

    # 4. create case
    orchestrator_connection.log_info("create case")
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


if __name__ == "__main__":
    oc = OrchestratorConnection.create_connection_from_args()
    process(oc)
