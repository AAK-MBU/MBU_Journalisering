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

    credentials = pf.get_credentials_and_constants(orchestrator_connection)
    forms_data = pf.get_forms_data(credentials['sql_conn_string'], oc_args_json["table_name"])

    for form in forms_data:
        case_handler = CaseHandler(credentials['go_api_endpoint'], credentials['go_api_username'], credentials['go_api_password'])
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
            credentials['sql_conn_string'],
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
            credentials['sql_conn_string'],
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
                                                   credentials['sql_conn_string'],
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
            credentials['sql_conn_string'],
            oc_args_json['db_update_sp'],
            oc_args_json['status_sp'],
            status_params_failed,
            uuid,
            oc_args_json['table_name']
        )

        #  Step 5: Journalize files
        pf.journalize_file(case_id,
                           parsed_form,
                           credentials['os2_api_key'],
                           credentials['go_api_endpoint'],
                           credentials['go_api_username'],
                           credentials['go_api_password'],
                           credentials['sql_conn_string'],
                           oc_args_json['db_update_sp'],
                           oc_args_json['status_sp'],
                           status_params_failed,
                           uuid,
                           oc_args_json['table_name'])


if __name__ == "__main__":
    oc = OrchestratorConnection.create_connection_from_args()
    process(oc)
