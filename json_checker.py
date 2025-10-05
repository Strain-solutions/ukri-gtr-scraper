import requests
import json


def print_record_json(project_id="NIHR129703"):
    base_url = "https://nihr.opendatasoft.com/api/records/1.0/search/"
    dataset = "infonihr-open-dataset"

    params = {
        "dataset": dataset,
        "q": project_id,
        "rows": 1
    }

    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()

    if data["nhits"] > 0:
        record = data["records"][0]
        print(json.dumps(record, indent=2))
    else:
        print("No record found for", project_id)


# Example:
print_record_json("NIHR129703")