"""This module contains the main process of the robot."""

import json
import os

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from mbu_dev_shared_components.getorganized.objects import CaseDataJson

from robot_framework.case_manager.case_handler import CaseHandler
from robot_framework.case_manager import process_functions as pf


def process(orchestrator_connection: OrchestratorConnection) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")
    oc_args_json = json.loads(orchestrator_connection.process_arguments)

    credentials = pf.get_credentials_and_constants(orchestrator_connection)
    forms_data = pf.get_forms_data(credentials['conn_string'], oc_args_json["table_name"])

    for form in forms_data:
        case_handler = CaseHandler(credentials['endpoint'], credentials['username'], credentials['password'])
        case_data_handler = CaseDataJson()

        uuid = form['uuid']
        orchestrator_connection.log_info(uuid)

        parsed_form = json.loads(form['data'])
        ssn = parsed_form['data']['barnets_cpr_nummer'].replace('-', '')

        status_params_failed = {
            "Status": "FAILED",
            "uuid": f'{uuid}',
            "TableName": f'{oc_args_json["table_name"]}'
        }

        #  Step 1: Lookup the citizen
        person_full_name, person_go_id = pf.contact_lookup(
            case_handler,
            ssn,
            credentials['conn_string'],
            oc_args_json['db_update_sp'],
            oc_args_json['status_sp'],
            status_params_failed,
            uuid,
            oc_args_json['table_name']
        )

        #  Step 2: Check for existing citizen folder
        case_folder_id = pf.check_case_folder(
            case_handler,
            case_data_handler,
            oc_args_json['case_type'],
            person_full_name,
            person_go_id,
            ssn,
            credentials['conn_string'],
            oc_args_json['db_update_sp'],
            oc_args_json['status_sp'],
            status_params_failed,
            uuid,
            oc_args_json['table_name']
        )

        if not case_folder_id:
            case_folder_id = pf.create_case_folder(case_handler,
                                                   oc_args_json['case_type'],
                                                   person_full_name,
                                                   person_go_id,
                                                   ssn,
                                                   credentials['conn_string'],
                                                   oc_args_json['db_update_sp'],
                                                   oc_args_json['status_sp'],
                                                   status_params_failed,
                                                   uuid,
                                                   oc_args_json['table_name'])

        #  Step 4: Create a new citizen case
        case_id = pf.create_case(
            case_handler,
            orchestrator_connection,
            person_full_name,
            ssn,
            oc_args_json['case_type'],
            case_folder_id,
            oc_args_json['case_data'],
            credentials['conn_string'],
            oc_args_json['db_update_sp'],
            oc_args_json['status_sp'],
            status_params_failed,
            uuid,
            oc_args_json['table_name']
        )
        print(case_id)


if __name__ == "__main__":
    #  oc = OrchestratorConnection.create_connection_from_args()
    oc = OrchestratorConnection("test", os.getenv('OpenOrchestratorConnString'), os.getenv('OpenOrchestratorKey'), '{"table_name":"Hub_GO_Modersmaal","case_type":"BOR","status_sp":"rpa.Hub_UpdateProcessStatus","db_update_sp":"rpa.Hub_AddOrUpdateJson","case_data":{"case_owner_id":"1204","case_owner_name":"Rikke Agerholm Andersen(azktefx)","case_profile_id":"526","case_profile_name":"MBU SK Modersmålsundervisning","case_title":"","case_folder_id":"","supplementary_case_owners":"","department_id":"459","department_name":"Møllevangskolen - Administration","supplementary_departments":"","return_when_case_fully_created":"True"}}')
    process(oc)
