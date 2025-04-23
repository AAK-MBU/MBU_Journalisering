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
    case_metadata = fetch_case_metadata(
        connection_string=credentials['sql_conn_string'],
        os2formwebform_id=os2formwebform_id)
    forms_data = jp.get_forms_data(conn_string=credentials['sql_conn_string'], form_type=os2formwebform_id)

    for form in forms_data:
        # Maybe outside loop
        case_handler = CaseHandler(
            credentials['go_api_endpoint'],
            credentials['go_api_username'],
            credentials['go_api_password'])
        case_data_handler = CaseDataJson()
        document_handler = DocumentHandler(
            credentials['go_api_endpoint'],
            credentials['go_api_username'],
            credentials['go_api_password'])

        form_id = form['form_id']
        form_submitted_date = form['form_submitted_date']
        parsed_form_data = json.loads(form['form_data'])
        ssn = extract_ssn(os2formwebform_id=os2formwebform_id, parsed_form_data=parsed_form_data)

        # Maybe outside loop
        status_params_inprogress, status_params_success, status_params_failed = get_status_params(form_id)

        if ssn is None and os2formwebform_id not in ('respekt_for_graenser', 'respekt_for_graenser_privat', 'indmeld_kraenkelser_af_boern'):
            execute_stored_procedure(
                credentials['sql_conn_string'],
                case_metadata['spUpdateProcessStatus'],
                status_params_failed)

            raise ValueError("SSN is None")

        person_full_name = None
        case_folder_id = None

        orchestrator_connection.log_trace(f"form_id: {form_id}")
        orchestrator_connection.log_trace(f"form_submitted_date: {form_submitted_date}")

        execute_stored_procedure(
            credentials['sql_conn_string'],
            case_metadata['spUpdateProcessStatus'],
            status_params_inprogress)

        if case_metadata['caseType'] == "BOR":
            orchestrator_connection.log_trace("Lookup the citizen.")
            try:
                person_full_name, person_go_id = jp.contact_lookup(
                    case_handler=case_handler,
                    ssn=ssn,
                    conn_string=credentials['sql_conn_string'],
                    update_response_data=case_metadata['spUpdateResponseData'],
                    update_process_status=case_metadata['spUpdateProcessStatus'],
                    process_status_params_failed=status_params_failed,
                    form_id=form_id
                )
                orchestrator_connection.log_trace("Citizen lookup successful.")
            except Exception as e:
                message = "Error looking up the citizen"
                orchestrator_connection.log_trace(message)
                notify_stakeholders(
                    case_metadata,
                    None,
                    None,
                    None,
                    orchestrator_connection,
                    f"{message}: {e}",
                    None,
                )
                continue

            orchestrator_connection.log_trace("Check for existing citizen folder.")
            try:
                case_folder_id = jp.check_case_folder(
                    case_handler=case_handler,
                    case_data_handler=case_data_handler,
                    case_type=case_metadata['caseType'],
                    person_full_name=person_full_name,
                    person_go_id=person_go_id,
                    ssn=ssn,
                    conn_string=credentials['sql_conn_string'],
                    update_response_data=case_metadata['spUpdateResponseData'],
                    update_process_status=case_metadata['spUpdateProcessStatus'],
                    process_status_params_failed=status_params_failed,
                    form_id=form_id
                )
                orchestrator_connection.log_trace("Citizen folder check successful.")
            except Exception as e:
                message = "Error checking for existing citizen folder."
                orchestrator_connection.log_trace(message)
                notify_stakeholders(
                    case_metadata,
                    None,
                    None,
                    None,
                    orchestrator_connection,
                    f"{message}: {e}",
                    None,
                )
                continue

            if not case_folder_id:
                orchestrator_connection.log_trace("Create citizen folder.")
                try:
                    case_folder_id = jp.create_case_folder(
                        case_handler=case_handler,
                        case_type=case_metadata['caseType'],
                        person_full_name=person_full_name,
                        person_go_id=person_go_id,
                        ssn=ssn,
                        conn_string=credentials['sql_conn_string'],
                        update_response_data=case_metadata['spUpdateResponseData'],
                        update_process_status=case_metadata['spUpdateProcessStatus'],
                        process_status_params_failed=status_params_failed,
                        form_id=form_id
                    )
                    orchestrator_connection.log_trace("Citizen folder creation successful.")
                except Exception as e:
                    message = "Error creating citizen folder."
                    orchestrator_connection.log_trace(message)
                    notify_stakeholders(
                        case_metadata,
                        None,
                        None,
                        None,
                        orchestrator_connection,
                        f"{message}: {e}",
                        None,
                    )
                    continue

        orchestrator_connection.log_trace("Create case.")
        try:
            case_id, case_title, case_rel_url = jp.create_case(
                case_handler=case_handler,
                orchestrator_connection=orchestrator_connection,
                parsed_form_data=parsed_form_data,
                os2form_webform_id=os2formwebform_id,
                case_type=case_metadata['caseType'],
                case_data=case_metadata['caseData'],
                conn_string=credentials['sql_conn_string'],
                update_response_data=case_metadata['spUpdateResponseData'],
                update_process_status=case_metadata['spUpdateProcessStatus'],
                process_status_params_failed=status_params_failed,
                form_id=form_id,
                ssn=ssn,
                person_full_name=person_full_name,
                case_folder_id=case_folder_id
            )
            orchestrator_connection.log_trace("Case creation successful.")
        except Exception as e:
            message = f"Error creating case: {e}"
            print(message)
            notify_stakeholders(case_metadata, None, None, None, orchestrator_connection, message, None)
            continue

        orchestrator_connection.log_trace("Journalize files.")
        try:
            jp.journalize_file(
                document_handler=document_handler,
                case_id=case_id,
                case_title=case_title,
                case_rel_url=case_rel_url,
                parsed_form_data=parsed_form_data,
                os2_api_key=credentials['os2_api_key'],
                conn_string=credentials['sql_conn_string'],
                process_status_params_failed=status_params_failed,
                form_id=form_id,
                case_metadata=case_metadata,
                orchestrator_connection=orchestrator_connection
            )
            orchestrator_connection.log_trace("Journalization successful.")
        except Exception as e:
            message = f"Error journalizing files. {e}"
            print(message)
            notify_stakeholders(
                case_metadata=case_metadata,
                case_id=case_id,
                case_title=case_title,
                case_rel_url=case_rel_url,
                orchestrator_connection=orchestrator_connection,
                error_message=message,
                attachment_bytes=None)
            continue

        execute_stored_procedure(
            credentials['sql_conn_string'],
            case_metadata['spUpdateProcessStatus'],
            status_params_success)


def get_status_params(form_id: str):
    """
    Generates a set of status parameters for the process, based on the given form_id and JSON arguments.

    Args:
        form_id (str): The unique identifier for the current process.
        case_metadata (dict): A dictionary containing various process-related arguments, including table names.

    Returns:
        tuple: A tuple containing three dictionaries:
            - status_params_inprogress: Parameters indicating that the process is in progress.
            - status_params_success: Parameters indicating that the process completed successfully.
            - status_params_failed: Parameters indicating that the process has failed.
    """
    status_params_inprogress = {
        "Status": ("str", "InProgress"),
        "form_id": ("str", f'{form_id}')
    }
    status_params_success = {
        "Status": ("str", "Successful"),
        "form_id": ("str", f'{form_id}')
    }
    status_params_failed = {
        "Status": ("str", "Failed"),
        "form_id": ("str", f'{form_id}')
    }
    return status_params_inprogress, status_params_success, status_params_failed


def extract_ssn(os2formwebform_id, parsed_form_data):
    """
    Extracts the Social Security Number (SSN) from the parsed form data based on the provided webform ID.

    Args:
        os2formwebform_id (str): A string containing the webform ID.
        parsed_form_data (dict): A dictionary containing the parsed form data, including potential SSN fields.

    Returns:
        str or None: The extracted SSN as a string with hyphens removed,
            or None if the SSN is not present in the form data.
    """
    match os2formwebform_id:
        case (
            "indmeldelse_i_modtagelsesklasse"
            | "ansoegning_om_koersel_af_skoleel"
            | "ansoegning_om_midlertidig_koerse"
        ):
            if "cpr_barnets_nummer" in parsed_form_data["data"]:
                return parsed_form_data["data"]["cpr_barnets_nummer"].replace("-", "")
            if "barnets_cpr_nummer" in parsed_form_data["data"]:
                return parsed_form_data["data"]["barnets_cpr_nummer"].replace("-", "")
            if "cpr_elevens_nummer" in parsed_form_data["data"]:
                return parsed_form_data["data"]["cpr_elevens_nummer"].replace("-", "")
            if "elevens_cpr_nummer" in parsed_form_data["data"]:
                return parsed_form_data["data"]["elevens_cpr_nummer"].replace("-", "")
            if "cpr_barnet" in parsed_form_data["data"]:
                return parsed_form_data["data"]["cpr_barnet"].replace("-", "")
            # TEST webform_id'er. Prod id i journalize_process.py
        case "tilmelding_til_modersmaalsunderv":
            if parsed_form_data['data']['elevens_cpr_nummer_mitid'] != '':  # Hvis cpr kommer fra MitID
                return parsed_form_data['data']['elevens_cpr_nummer_mitid'].replace('-', '')
            if parsed_form_data['data']['elevens_cpr_nummer'] != '':  # Hvis cpr er indtastet manuelt
                return parsed_form_data["data"]["elevens_cpr_nummer"].replace("-", "")
        case "anmeldelse_af_hjemmeundervisning":
            if parsed_form_data['data']['barnets_cpr_nummer_mitid'] != '':  # Hvis cpr kommer fra MitID
                return parsed_form_data['data']['barnets_cpr_nummer_mitid'].replace('-', '')
            if parsed_form_data['data']['cpr_barnets_nummer_'] != '':  # Hvis cpr er indtastet manuelt
                return parsed_form_data['data']['cpr_barnets_nummer_'].replace('-', '')
        case "pasningstid":
            if parsed_form_data['data']['barnets_cpr_nummer'] != '':  # Hvis cpr kommer fra MitID
                return parsed_form_data['data']['barnets_cpr_nummer'].replace('-', '')
            if parsed_form_data['data']['cpr_barnets_nummer_'] != '':  # Hvis cpr er indtastet manuelt
                return parsed_form_data['data']['cpr_barnets_nummer_'].replace('-', '')
        case "skriv_dit_barn_paa_venteliste":
            if parsed_form_data['data']['barnets_cpr_nummer_mitid'] != '':  # Hvis cpr kommer fra MitID
                return parsed_form_data['data']['barnets_cpr_nummer_mitid'].replace('-', '')
            if parsed_form_data['data']['cpr_barnets_nummer_'] != '':
                return parsed_form_data['data']['cpr_barnets_nummer_'].replace('-', '')
        case _:
            return None
