"""This module contains the main process of the robot."""

import json

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from mbu_dev_shared_components.getorganized.objects import CaseDataJson

from robot_framework.case_manager.case_handler import CaseHandler
from robot_framework.case_manager import process_functions as pf


def process(orchestrator_connection: OrchestratorConnection) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")
    oc_args_json = json.loads(orchestrator_connection.process_arguments)
    orchestrator_connection.log_trace(oc_args_json)
    orchestrator_connection.log_info(orchestrator_connection.get_constant('test_uuid').value)

    credentials = pf.get_credentials_and_constants(orchestrator_connection)
    case_handler = CaseHandler(credentials['endpoint'], credentials['username'], credentials['password'])
    case_data_handler = CaseDataJson()

    status_params_failed = {
        "Status": "FAILED",
        "uuid": f'{credentials["uuid"]}',
        "TableName": f'{oc_args_json["table_name"]}'
    }

    person_full_name, person_go_id = pf.contact_lookup(
        case_handler,
        credentials['ssn'],
        credentials['conn_string'],
        oc_args_json['db_update_sp'],
        oc_args_json['status_sp'],
        status_params_failed,
        credentials['uuid'],
        oc_args_json['table_name']
    )

    case_folder_id = pf.check_case_folder(
        case_handler,
        case_data_handler,
        oc_args_json['case_type'],
        person_full_name,
        person_go_id,
        credentials['ssn'],
        credentials['conn_string'],
        oc_args_json['db_update_sp'],
        oc_args_json['status_sp'],
        status_params_failed,
        credentials['uuid'],
        oc_args_json['table_name']
    )

    if not case_folder_id:
        case_folder_id = pf.create_case_folder(case_handler, oc_args_json['case_type'], person_full_name, person_go_id, credentials['ssn'])

    case_response = pf.create_case(
        case_handler,
        orchestrator_connection,
        person_full_name,
        credentials['ssn'],
        case_folder_id,
        oc_args_json['case_data']
    )

    print(case_response.content)


if __name__ == "__main__":
    oc = OrchestratorConnection.create_connection_from_args()
    process(oc)
