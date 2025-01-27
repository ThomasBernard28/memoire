from ApiInterrogator import ApiInterrogator
from datetime import datetime

if __name__ == "__main__":

    URL = "https://gitea.com/api/v1"
    TOKEN = "82b9c59c55aa72fabd8bdde707ccdfd165db8160"

    api_interrogator = ApiInterrogator(URL, TOKEN)

    response = api_interrogator.get_authenticated_user_repositories()
    if response.status_code == 200:
        if response.json():
            print("Repositories:")
            for repo in response.json():
                print(f"  - {repo['name']}")
    else:
        print("Error:", response.status_code, response.text)

    # Get gitea user information
    response = api_interrogator.get_userid_by_username("gitea")
    userId = 0
    if response.status_code == 200:
        if response.json():
            userId += response.json()['id']
            print("User ID:", userId)
    else:
        print("Error:", response.status_code, response.text)


    # Search repositories

    repositories = []

    for i in range(1, 10):
        print(f"Page {i}")
        params = {
            'sort': 'stars',
            'order': 'desc',
            'limit': 20,
            'page': i
        }

        response = api_interrogator.search_repositories(params)
        if response.status_code == 200:
            if response.json():
                data = response.json()['data']

                for repo in data:

                    repo_last_updated = datetime.strptime(repo['updated_at'], "%Y-%m-%dT%H:%M:%SZ")
                    comparison_date = datetime(2021, 1, 1)

                    if repo_last_updated > comparison_date:
                        repositories.append(repo)


        else:
            print("Error:", response.status_code, response.text)
            break

    print("Repositories:")
    print(len(repositories))

    commits = []
    for repo in repositories:
        response = api_interrogator.get_all_commits(repo['owner']['login'], repo['name'], {})
        if response.status_code == 200:
            if response.json():
                commits.append((repo['name'], response.json()))
        else:
            print("Error:", response.status_code, response.text)

    print("Commits:")
    for commit in commits[0][1]:
        print(f"  - {commit['files']}")