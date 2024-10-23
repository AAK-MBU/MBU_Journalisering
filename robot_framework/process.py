"""This module contains the main process of the robot."""
import json

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from mbu_dev_shared_components.utils.db_stored_procedure_executor import execute_stored_procedure
from mbu_dev_shared_components.getorganized.objects import CaseDataJson

from robot_framework.case_manager.case_handler import CaseHandler
from robot_framework.case_manager.document_handler import DocumentHandler
from robot_framework.case_manager import journalize_process as jp
from robot_framework.case_manager.helper_functions import fetch_case_metadata, notify_stakeholders


def process(orchestrator_connection: OrchestratorConnection) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")
    oc_args_json = json.loads(orchestrator_connection.process_arguments)
    os2formwebform_id = oc_args_json['os2formWebformId']
    credentials = jp.get_credentials_and_constants(orchestrator_connection)
    case_metadata = fetch_case_metadata(credentials['sql_conn_string'], os2formwebform_id)
    forms_data = jp.get_forms_data(credentials['sql_conn_string'], case_metadata['tableName'])

    for form in forms_data:
        case_handler = CaseHandler(credentials['go_api_endpoint'], credentials['go_api_username'], credentials['go_api_password'])
        case_data_handler = CaseDataJson()
        document_handler = DocumentHandler(credentials['go_api_endpoint'], credentials['go_api_username'], credentials['go_api_password'])
        uuid = form['uuid']
        orchestrator_connection.log_trace(f"UUID: {uuid}")
        parsed_form_data = json.loads(form['data'])
        ssn = extract_ssn(case_metadata, parsed_form_data)
        person_full_name = None
        case_folder_id = None

        status_params_inprogress, status_params_success, status_params_failed = get_status_params(uuid, case_metadata)
        execute_stored_procedure(credentials['sql_conn_string'], case_metadata['hubUpdateProcessStatus'], status_params_inprogress)

        if case_metadata['caseType'] == "BOR":
            orchestrator_connection.log_trace("Lookup the citizen.")
            try:
                person_full_name, person_go_id = jp.contact_lookup(
                    case_handler,
                    ssn,
                    credentials['sql_conn_string'],
                    case_metadata['hubUpdateResponseData'],
                    case_metadata['hubUpdateProcessStatus'],
                    status_params_failed,
                    uuid,
                    case_metadata['tableName']
                )
            except Exception:
                print("Error looking up the citizen.")
                continue

            orchestrator_connection.log_trace("Check for existing citizen folder.")
            try:
                case_folder_id = jp.check_case_folder(
                    case_handler,
                    case_data_handler,
                    case_metadata['caseType'],
                    person_full_name,
                    person_go_id,
                    ssn,
                    credentials['sql_conn_string'],
                    case_metadata['hubUpdateResponseData'],
                    case_metadata['hubUpdateProcessStatus'],
                    status_params_failed,
                    uuid,
                    case_metadata['tableName']
                )
            except Exception:
                continue

            if not case_folder_id:
                orchestrator_connection.log_trace("Create citizen folder.")
                try:
                    case_folder_id = jp.create_case_folder(
                        case_handler,
                        case_metadata['caseType'],
                        person_full_name,
                        person_go_id,
                        ssn,
                        credentials['sql_conn_string'],
                        case_metadata['hubUpdateResponseData'],
                        case_metadata['hubUpdateProcessStatus'],
                        status_params_failed,
                        uuid,
                        case_metadata['tableName']
                    )
                except Exception:
                    print("Error creating citizen folder.")
                    continue

        orchestrator_connection.log_trace("Create case.")
        try:
            case_id, case_title = jp.create_case(
                case_handler,
                orchestrator_connection,
                parsed_form_data,
                case_metadata['os2formWebformId'],
                case_metadata['caseType'],
                case_metadata['caseData'],
                credentials['sql_conn_string'],
                case_metadata['hubUpdateResponseData'],
                case_metadata['hubUpdateProcessStatus'],
                status_params_failed,
                uuid,
                case_metadata['tableName'],
                ssn,
                person_full_name,
                case_folder_id,

            )
        except Exception as e:
            message = (f"Error creating case: {e}")
            print(message)
            notify_stakeholders(None, None, orchestrator_connection, message)
            continue

        orchestrator_connection.log_trace("Journalize files.")
        try:
            jp.journalize_file(
                document_handler,
                case_id,
                case_title,
                parsed_form_data,
                credentials['os2_api_key'],
                credentials['sql_conn_string'],
                status_params_failed,
                uuid,
                case_metadata,
                orchestrator_connection
            )
        except Exception as e:
            message = (f"Error journalizing files. {e}")
            print(message)
            notify_stakeholders(case_id, case_title, orchestrator_connection, message)
            continue

        execute_stored_procedure(credentials['sql_conn_string'], case_metadata['hubUpdateProcessStatus'], status_params_success)


def get_status_params(uuid, case_metadata):
    """
    Generates a set of status parameters for the process, based on the given UUID and JSON arguments.

    Args:
        uuid (str): The unique identifier for the current process.
        case_metadata (dict): A dictionary containing various process-related arguments, including table names.

    Returns:
        tuple: A tuple containing three dictionaries:
            - status_params_inprogress: Parameters indicating that the process is in progress.
            - status_params_success: Parameters indicating that the process completed successfully.
            - status_params_failed: Parameters indicating that the process has failed.
    """
    status_params_inprogress = {
        "Status": ("str", "InProgress"),
        "uuid": ("str", f'{uuid}'),
        "TableName": ("str", f'{case_metadata["tableName"]}')
    }
    status_params_success = {
        "Status": ("str", "Successful"),
        "uuid": ("str", f'{uuid}'),
        "TableName": ("str", f'{case_metadata["tableName"]}')
    }
    status_params_failed = {
        "Status": ("str", "Failed"),
        "uuid": ("str", f'{uuid}'),
        "TableName": ("str", f'{case_metadata["tableName"]}')
    }
    return status_params_inprogress, status_params_success, status_params_failed


def extract_ssn(case_metadata, parsed_form_data):
    """
    Extracts the Social Security Number (SSN) from the parsed form data based on the provided webform ID.

    Args:
        case_metadata (dict): A dictionary containing various process-related arguments, including the webform ID.
        parsed_form_data (dict): A dictionary containing the parsed form data, including potential SSN fields.

    Returns:
        str or None: The extracted SSN as a string with hyphens removed, or None if the SSN is not present in the form data.
    """
    match case_metadata['os2formWebformId']:
        case "tilmelding_til_modersmaalsunderv" | "indmeldelse_i_modtagelsesklasse" | "ansoegning_om_koersel_af_skoleel" | "ansoegning_om_midlertidig_koerse":
            if 'cpr_barnets_nummer' in parsed_form_data['data']:
                return parsed_form_data['data']['cpr_barnets_nummer'].replace('-', '')
            if 'barnets_cpr_nummer' in parsed_form_data['data']:
                return parsed_form_data['data']['barnets_cpr_nummer'].replace('-', '')
            if 'cpr_elevens_nummer' in parsed_form_data['data']:
                return parsed_form_data['data']['cpr_elevens_nummer'].replace('-', '')
            if 'elevens_cpr_nummer' in parsed_form_data['data']:
                return parsed_form_data['data']['elevens_cpr_nummer'].replace('-', '')
            if 'cpr_barnet' in parsed_form_data['data']:
                return parsed_form_data['data']['cpr_barnet'].replace('-', '')
        case _:
            return None
