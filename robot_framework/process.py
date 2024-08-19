"""This module contains the main process of the robot."""
import os
import json

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from mbu_dev_shared_components.utils.db_stored_procedure_executor import execute_stored_procedure
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
        orchestrator_connection.log_trace(f"UUID: {uuid}")

        parsed_form = json.loads(form['data'])
        match oc_args_json['os2form_webform_id']:
            case "tilmelding_til_modersmaalsunderv" | "indmeldelse_i_modtagelsesklasse" | "ansoegning_om_koersel_af_skoleel" | "ansoegning_om_midlertidig_koerse":
                if 'cpr_barnets_nummer' in parsed_form['data']:
                    ssn = parsed_form['data']['cpr_barnets_nummer'].replace('-', '')
                elif 'barnets_cpr_nummer' in parsed_form['data']:
                    ssn = parsed_form['data']['barnets_cpr_nummer'].replace('-', '')
            case _:
                ssn = None

        person_full_name = None
        case_folder_id = None

        status_params_inprogress = {
            "Status": ("str", "InProgress"),
            "uuid": ("str", f'{uuid}'),
            "TableName": ("str", f'{oc_args_json["table_name"]}')
        }
        status_params_success = {
            "Status": ("str", "Successful"),
            "uuid": ("str", f'{uuid}'),
            "TableName": ("str", f'{oc_args_json["table_name"]}')
        }
        status_params_failed = {
            "Status": ("str", "Failed"),
            "uuid": ("str", f'{uuid}'),
            "TableName": ("str", f'{oc_args_json["table_name"]}')
        }

        execute_stored_procedure(credentials['sql_conn_string'], oc_args_json['hub_update_process_status'], status_params_inprogress)

        if oc_args_json['case_type'] == "BOR":
            orchestrator_connection.log_trace("Lookup the citizen.")
            try:
                person_full_name, person_go_id = pf.contact_lookup(
                    case_handler,
                    ssn,
                    credentials['sql_conn_string'],
                    oc_args_json['hub_update_response_data'],
                    oc_args_json['hub_update_process_status'],
                    status_params_failed,
                    uuid,
                    oc_args_json['table_name']
                )
            except Exception:
                continue

            orchestrator_connection.log_trace("Check for existing citizen folder.")
            try:
                case_folder_id = pf.check_case_folder(
                    case_handler,
                    case_data_handler,
                    oc_args_json['case_type'],
                    person_full_name,
                    person_go_id,
                    ssn,
                    credentials['sql_conn_string'],
                    oc_args_json['hub_update_response_data'],
                    oc_args_json['hub_update_process_status'],
                    status_params_failed,
                    uuid,
                    oc_args_json['table_name']
                )
            except Exception:
                continue

            if not case_folder_id:
                orchestrator_connection.log_trace("Create citizen folder.")
                try:
                    case_folder_id = pf.create_case_folder(
                        case_handler,
                        oc_args_json['case_type'],
                        person_full_name,
                        person_go_id,
                        ssn,
                        credentials['sql_conn_string'],
                        oc_args_json['hub_update_response_data'],
                        oc_args_json['hub_update_process_status'],
                        status_params_failed,
                        uuid,
                        oc_args_json['table_name']
                    )
                except Exception:
                    continue

        orchestrator_connection.log_trace("Create case.")
        try:
            case_id = pf.create_case(
                case_handler,
                oc_args_json['os2form_webform_id'],
                oc_args_json['case_type'],
                oc_args_json['case_data'],
                credentials['sql_conn_string'],
                oc_args_json['hub_update_response_data'],
                oc_args_json['hub_update_process_status'],
                status_params_failed,
                uuid,
                oc_args_json['table_name'],
                ssn,
                person_full_name,
                case_folder_id
            )
        except Exception as e:
            print(e)
            continue

        orchestrator_connection.log_trace("Journalize files.")
        try:
            pf.journalize_file(
                case_id,
                parsed_form,
                credentials['os2_api_key'],
                credentials['go_api_endpoint'],
                credentials['go_api_username'],
                credentials['go_api_password'],
                credentials['sql_conn_string'],
                oc_args_json['hub_update_response_data'],
                oc_args_json['hub_update_process_status'],
                status_params_failed,
                uuid,
                oc_args_json['table_name']
            )
        except Exception:
            continue

        execute_stored_procedure(credentials['sql_conn_string'], oc_args_json['hub_update_process_status'], status_params_success)


if __name__ == "__main__":
    oc = OrchestratorConnection.create_connection_from_args()
    process(oc)
