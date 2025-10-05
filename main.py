import requests

def get_organisation_id(university_name):
    """Fetch the organization ID based on the university name."""
    url = f"https://gtr.ukri.org/gtr/api/organisations?q={university_name}"
    headers = {"Accept": "application/json"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        organisations = data.get('organisation', [])
        if organisations:
            # Return the first matching organization ID
            return organisations[0]['id'], organisations[0]['name']
        else:
            print("No matching university found.")
            return None, None
    else:
        print("Error fetching organization data:", response.status_code)
        return None, None

def get_projects(organisation_id, funder_id):
    """Fetch projects based on organization ID and funder ID."""
    url = f"https://gtr.ukri.org/gtr/api/projects?organisation={organisation_id}&funder={funder_id}"
    headers = {"Accept": "application/json"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get('project', [])
    else:
        print("Error fetching projects:", response.status_code)
        return []

def get_funder_id(funder_name):
    """Fetch the funder ID based on the funder name."""
    url = f"https://gtr.ukri.org/gtr/api/organisations?q={funder_name}"
    headers = {"Accept": "application/json"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        funders = data.get('organisation', [])
        if funders:
            for funder in funders:
                print(f"Name: {funder['name']}, ID: {funder['id']}")
            return funders[0]['id'], funders[0]['name']
        else:
            print("No matching funder found.")
            return None, None
    else:
        print("Error fetching funder data:", response.status_code)
        return None, None


def main():
    university_name = "University of the West of Scotland"
    funder_name = "mrc"

    org_id, org_name = get_organisation_id(university_name)
    funder_id, funder_display_name = get_funder_id(funder_name)

    if org_id and funder_id:
        print(f"\nFetching projects for {org_name} funded by {funder_display_name}...\n")
        projects = get_projects(org_id, funder_id)

        if projects:
            for i, project in enumerate(projects, 1):
                print(f"{i}. {project.get('title', 'No Title')}")
                print(f"   Abstract: {project.get('abstractText', 'No Abstract')[:150]}...\n")
        else:
            print("No projects found for this combination.")

if __name__ == "__main__":
    main()
