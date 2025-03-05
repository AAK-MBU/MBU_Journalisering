"""Test module for the case manager."""

METADATA = {
	"caseCategory": "test_category",
	"caseOwnerId": "test_owner_id",
	"caseOwnerName": "test_person",
	"caseProfileId": "test_profile_id",
	"caseProfileName": "test_profile_name",
	"departmentId": "test_department_id",
	"departmentName": "test_department_name",
	"facet": "test_facet",
	"kleNumber": "test_kle_nr",
	"specialGroup": "test_special_group",
	"startDate": "",
	"supplementaryCaseOwners": "",
	"supplementaryDepartments": "",
	"customMasterCase": "",
	"emailRecipient": "test_email",
	"case_title": "Modersmålsundervisning placeholder_person_full_name"
}

TEST_META_TITLER = [
    "Modulændring/overflytning/indmeldelse (placeholder_person_full_name, placeholder_ssn_first_6)",
    "Modersmålsundervisning placeholder_person_full_name",
    "Hjemmeundervisning af placeholder_person_full_name",
    "Visitering af placeholder_person_full_name placeholder_ssn",
    "Kørsel til placeholder_person_full_name",
]

def determine_case_title(os2form_webform_id: str, person_full_name: str, ssn: str, parsed_form_data, meta_case_title) -> str:
    """Determine the title of the case based on the webform ID."""
    
    if os2form_webform_id not in ("indmeld_kraenkelser_af_boern", "respekt_for_graenser_privat", "respekt_for_graenser"):
        placeholder_replacements = [
            ("placeholder_ssn_first_6", ssn[:6]),
            ("placeholder_ssn", ssn),
            ("placeholder_person_full_name", person_full_name)
        ]

        for placeholder, value in placeholder_replacements:
            meta_case_title = meta_case_title.replace(placeholder, value)

        return meta_case_title

    omraade = parsed_form_data['data']['omraade']
    if omraade == "Skole":
        department = parsed_form_data['data'].get('skole', "Ukendt skole")
    elif omraade == "Dagtilbud":
        department = parsed_form_data['data'].get('dagtilbud')
        if not department:
            department = parsed_form_data['data'].get('daginstitution_udv_', "Ukendt dagtilbud")
    elif omraade == "Ungdomsskole":
        department = parsed_form_data['data'].get('ungdomsskole', "Ukendt ungdomsskole")
    elif omraade == "Klub":
        department = parsed_form_data['data'].get('klub', "Ukendt klub")
    else:
        department = "Ukendt afdeling"  # Default if no match
    part_title = None
    if os2form_webform_id == "indmeld_kraenkelser_af_boern":
        part_title = "Forældre/pårørendehenvendelse"
    elif os2form_webform_id == "respekt_for_graenser_privat":
        part_title = "Privat skole/privat dagtilbud-henvendelse"
    elif os2form_webform_id == "respekt_for_graenser":
        part_title = "BU-henvendelse"
    return f"{department} - {part_title}"


if __name__ == "__main__":
    print("testing ...")

    test_form_id = "test_id"
    test_person_full_name = "test_person"
    test_ssn = "0000000000"
    test_parsed_form_data = []

    for title in TEST_META_TITLER:
        print(f"original title: {title}")

        print("changing title ...")

        new_title = determine_case_title(os2form_webform_id=test_form_id, person_full_name=test_person_full_name, ssn=test_ssn, parsed_form_data=test_parsed_form_data, meta_case_title=title)

        print(f"new title: {new_title}")
        print()
        print()

    # print(determine_case_title_new("123", "John Doe", "123456789", {}, "Case Title"))
