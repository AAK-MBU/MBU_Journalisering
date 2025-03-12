"""Test the ServiceNow API."""

import requests

INCIDENT_SYS_ID = "384b72753329de10ae57c0934d5c7b80"

PROD_INSTANCE = "aarhuskommune"
TEST_INSTANCE = "aarhuskommunedev"


def post_incident(service_now_api_username, service_now_api_password, error_dict):
    """POST a CSM case by sys_id or case number."""

    print("inside post_incident() function ...")

    error_type = error_dict.get("type", "")  # Would typically be "ApplicationException"
    error_count = error_dict.get("error_count", "")  # The error_count
    error_message = error_dict.get("message", "")  # The actual Exception message in str format
    error_trace = error_dict.get("trace", "")  # The traceback.format_exc() in str format

    # url = f"https://{PROD_INSTANCE}.service-now.com/api/buno/databus/incident/{INCIDENT_SYS_ID}"
    url = f"https://{TEST_INSTANCE}.service-now.com/api/buno/databus/incident/{INCIDENT_SYS_ID}"

    incident_data = {
        "correlationId": "test",  # How do we handle requirement for unique correlationId?
        "contactType": "integation",  # Should always be 'integration' - this just means the incident was created using the ServiceNow API
        "shortDescription": f"Error type: {error_type}, Error message: {error_message}, Error count: {error_count}",
        "description": F"Full error trace message: {error_trace}",
        "caller": "SYS_ID FOR INCIDENT CREATER ROBOT/USER",  # Incident Creator Caller - create new user?
        "businessService": "",  # What should this be?
        "serviceOffering": "",  # What should this be?
        "assignmentGroup": "bae93519973586504138f286f053afac",  # MBU Proces Udvikling Assignment Group - should this be a constant in Orchestrator?
        "assignedTo": ""  # Should remain empty as placeholder for future assignment
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    response = requests.post(url, json=incident_data, headers=headers, auth=(service_now_api_username, service_now_api_password))

    print()
    print("Response Status Code:", response.status_code)
    print("Response Text:", response.text)

    if response.status_code == 200:
        return response.json().get("result", {})

    else:
        print(f"Error {response.status_code}: {response.text}")

        return None
