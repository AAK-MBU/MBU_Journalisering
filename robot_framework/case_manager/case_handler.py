"""Module to handle journalisering functionality"""

from mbu_dev_shared_components.getorganized import objects
from mbu_dev_shared_components.getorganized import cases
from mbu_dev_shared_components.getorganized import contacts


class CaseHandler:
    """
    A class to manage the creation of cases in the GetOrganized system.

    Attributes:
    - api_username (str): The username for GetOrganized API.
    - api_password (str): The password for GetOrganized API.
    """
    def __init__(self, api_endpoint: str, api_username: str, api_password: str):
        self.api_username = api_username
        self.api_password = api_password
        self.api_endpoint = api_endpoint
        self.case_obj = objects.CaseDataJson()

    def _get_full_endpoint(self, path: str):
        """
        Constructs the full endpoint URL.

        Parameters:
        - path (str): The specific path for the API endpoint.

        Returns:
        - str: The full endpoint URL.
        """
        if path:
            return f"{self.api_endpoint}{path}"
        return self.api_endpoint

    def create_case_folder_data(self, case_type_prefix: objects.CaseTypePrefix,
                                person_full_name: str,
                                person_id: str,
                                person_ssn: str,
                                return_when_case_fully_created: bool = True) -> str:
        """
        Creates JSON data for a case folder.

        Returns:
        - str: JSON string of case folder data.
        """
        xml_metadata = {
            '<z:row xmlns:z=\"#RowsetSchema\"',
            'ows_CaseCategory=\"Borgermappe\"',
            f'ows_CCMContactData=\"{person_full_name};#{person_id};#{person_ssn};#;#\" />'
        }
        return self.case_obj.case_data_json(case_type_prefix,
                                            xml_metadata,
                                            return_when_case_fully_created)

    def create_case_data(self,
                         case_type_prefix: objects.CaseTypePrefix,
                         case_owner_id: str,
                         case_owner_name: str,
                         case_profile_id: str,
                         case_profile_name: str,
                         case_title: str,
                         case_folder_id: str,
                         supplementary_case_owners: str = None,
                         department_id: str = None,
                         department_name: str = None,
                         supplementary_departments: str = None,
                         return_when_case_fully_created: bool = True) -> str:
        """
        Creates JSON data for a case.

        Returns:
        - str: JSON string of case data.
        """
        xml_metadata = (
            '<z:row xmlns:z=\"#RowsetSchema\" '
            'ows_CaseStatus=\"Åben\" '
            'ows_CaseCategory=\"Standard\" '
            f'ows_Title=\"{case_title}\" '
            f'ows_CaseOwner=\"{case_owner_id};#{case_owner_name}\" '
            f'ows_CCMParentCase=\"{case_folder_id};#BOR\" '
            f'ows_Afdeling=\"{department_id};#{department_name}\" '
            f'ows_Sagsprofil_BOR=\"{case_profile_id};#{case_profile_name}\" '
            + (f'ows_SupplerendeSagsbehandlere=\"{supplementary_case_owners}\" ' if supplementary_case_owners else '')
            + (f'ows_SupplerendeAfdelinger=\"{supplementary_departments}\" ' if supplementary_departments else '')
            + '/>'
        )
        return self.case_obj.case_data_json(case_type_prefix, xml_metadata, return_when_case_fully_created)

    def search_for_case_folder(self, case_folder_search_data: str, endpoint_path: str):
        """
        Search for case folder

        Parameters:
        - case_folder_search_data (str): JSON string of search data.
        """
        endpoint = self._get_full_endpoint(endpoint_path)

        return cases.find_case_by_case_properties(case_folder_search_data, endpoint, self.api_username, self.api_password)

    def create_case_folder(self, case_folder_data: str, endpoint_path: str):
        """
        Creates a case in the GetOrganized system using the provided case data.

        Parameters:
        - case_data (str): JSON string of case data.
        """
        endpoint = self._get_full_endpoint(endpoint_path)

        return cases.create_case_folder(case_folder_data, endpoint, self.api_username, self.api_password)

    def create_case(self, case_data: str, endpoint_path: str):
        """
        Creates a case in the GetOrganized system using the provided case data.

        Parameters:
        - case_data (str): JSON string of case data.
        """
        endpoint = self._get_full_endpoint(endpoint_path)

        return cases.create_case(case_data, endpoint, self.api_username, self.api_password)

    def contact_lookup(self, person_ssn: str, endpoint_path: str):
        """
        Looks up contact information based on a person's social security number (SSN).

        Parameters:
        - person_ssn (str): The social security number of the person.
        - endpoint_path (str): The specific path for the API endpoint.

        Returns:
        - str: JSON string of the contact information.
        """
        endpoint = self._get_full_endpoint(endpoint_path)

        return contacts.contact_lookup(person_ssn=person_ssn, api_endpoint=endpoint, api_username=self.api_username, api_password=self.api_password)
