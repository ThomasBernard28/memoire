import requests as rq

# Method to get the repositories of the authenticated user
def get_repositories(base_url, headers):
    url = f"{base_url}/user/repos"
    return rq.get(url, headers=headers)

def search_repositories(base_url, headers, params):
    url = f"{base_url}/repos/search"
    return rq.get(url, headers=headers, params=params)

def get_user_by_username(base_url, headers, name):
    url = f"{base_url}/users/{name}"
    return rq.get(url, headers=headers, params={"username": name})


if __name__ == "__main__":
    URL = "https://gitea.com/api/v1"
    TOKEN = "82b9c59c55aa72fabd8bdde707ccdfd165db8160"
    HEADERS = {
        "Authorization": f"token {TOKEN}",
        "Content-Type": "application/json"
    }

    response = get_repositories(URL, HEADERS)
    if response.status_code == 200:
        if response.json():
            print("Repositories:")
            for repo in response.json():
                print(f"  - {repo['name']}")
    else:
        print("Error:", response.status_code, response.text)

    # Get gitea user information
    response = get_user_by_username(URL, HEADERS, "gitea")
    userId = 0
    if response.status_code == 200:
        if response.json():
            userId += response.json()['id']
            print("User ID:", userId)
    else:
        print("Error:", response.status_code, response.text)

    # Search repositories
    params = {
        "uid": userId,
        "archived": False,
        "exclusive": True,
        "sort": "stars",
        "order": "desc"
    }

    response = search_repositories(URL, HEADERS, params)
    if response.status_code == 200:
        if response.json():
            data = response.json()['data']
            print("Repositories:")
            for repo in data:
                print(f"  - {repo['name']}")
    else:
        print("Error:", response.status_code, response.text)