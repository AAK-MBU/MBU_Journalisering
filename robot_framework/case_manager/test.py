case_id = "123"
filename = "test"
received_date = "01-01-2024"
file_bytes = [1, 2, 3, 4]
body = {
    "CaseId": f"{case_id}",
    "ListName": "Dokumenter",
    "FolderPath": "null",
    "FileName": f"{filename}",
    "Metadata": '<z:row xmlns:z=\"#RowsetSchema\" ' + (f'ows_Dato=\"{received_date}\"' if received_date else '') + ' />',
    "Overwrite": "true",
    "Bytes": list(file_bytes)
}

print(body)