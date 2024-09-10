"""This module contains the main process of the robot."""
import json

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from mbu_dev_shared_components.utils.db_stored_procedure_executor import execute_stored_procedure
from mbu_dev_shared_components.getorganized.objects import CaseDataJson

from robot_framework.case_manager.case_handler import CaseHandler
from robot_framework.case_manager.document_handler import DocumentHandler
from robot_framework.case_manager import journalize_process as jp


def process(orchestrator_connection: OrchestratorConnection) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")
    oc_args_json = json.loads(orchestrator_connection.process_arguments)

    credentials = jp.get_credentials_and_constants(orchestrator_connection)
    forms_data = jp.get_forms_data(credentials['sql_conn_string'], oc_args_json["table_name"])

    for form in forms_data:
        case_handler = CaseHandler(credentials['go_api_endpoint'], credentials['go_api_username'], credentials['go_api_password'])
        case_data_handler = CaseDataJson()

        document_handler = DocumentHandler(credentials['go_api_endpoint'], credentials['go_api_username'], credentials['go_api_password'])

        uuid = form['uuid']
        orchestrator_connection.log_trace(f"UUID: {uuid}")

        parsed_form_data = json.loads(form['data'])
        ssn = extract_ssn(oc_args_json, parsed_form_data)
        person_full_name = None
        case_folder_id = None

        status_params_inprogress, status_params_success, status_params_failed = get_status_params(uuid, oc_args_json)

        execute_stored_procedure(credentials['sql_conn_string'], oc_args_json['hub_update_process_status'], status_params_inprogress)

        if oc_args_json['case_type'] == "BOR":
            orchestrator_connection.log_trace("Lookup the citizen.")
            try:
                person_full_name, person_go_id = jp.contact_lookup(
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
                case_folder_id = jp.check_case_folder(
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
                    case_folder_id = jp.create_case_folder(
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
            case_id = jp.create_case(
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
        except Exception:
            continue

        orchestrator_connection.log_trace("Journalize files.")
        try:
            jp.journalize_file(
                document_handler,
                case_id,
                parsed_form_data,
                credentials['os2_api_key'],
                credentials['sql_conn_string'],
                status_params_failed,
                uuid,
                oc_args_json,
                orchestrator_connection
            )
        except Exception:
            continue

        execute_stored_procedure(credentials['sql_conn_string'], oc_args_json['hub_update_process_status'], status_params_success)


def get_status_params(uuid, oc_args_json):
    """
    Generates a set of status parameters for the process, based on the given UUID and JSON arguments.

    Args:
        uuid (str): The unique identifier for the current process.
        oc_args_json (dict): A dictionary containing various process-related arguments, including table names.

    Returns:
        tuple: A tuple containing three dictionaries:
            - status_params_inprogress: Parameters indicating that the process is in progress.
            - status_params_success: Parameters indicating that the process completed successfully.
            - status_params_failed: Parameters indicating that the process has failed.
    """
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
    return status_params_inprogress, status_params_success, status_params_failed


def extract_ssn(oc_args_json, parsed_form_data):
    """
    Extracts the Social Security Number (SSN) from the parsed form data based on the provided webform ID.

    Args:
        oc_args_json (dict): A dictionary containing various process-related arguments, including the webform ID.
        parsed_form_data (dict): A dictionary containing the parsed form data, including potential SSN fields.

    Returns:
        str or None: The extracted SSN as a string with hyphens removed, or None if the SSN is not present in the form data.
    """
    match oc_args_json['os2form_webform_id']:
        case "tilmelding_til_modersmaalsunderv" | "indmeldelse_i_modtagelsesklasse" | "ansoegning_om_koersel_af_skoleel" | "ansoegning_om_midlertidig_koerse":
            if 'cpr_barnets_nummer' in parsed_form_data['data']:
                return parsed_form_data['data']['cpr_barnets_nummer'].replace('-', '')
            if 'barnets_cpr_nummer' in parsed_form_data['data']:
                return parsed_form_data['data']['barnets_cpr_nummer'].replace('-', '')
        case _:
            return None


if __name__ == "__main__":
    oc = OrchestratorConnection.create_connection_from_args()
    process(oc)
