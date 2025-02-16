import requests as rq
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Default URL
URL = "https://gitea.com/api/v1"
# TOKEN


# Request headers
HEADERS = {
    'Authorization': f"token {TOKEN}",
    'Content-Type': 'application/json'
}

# Limit Date set to 01/01/2023 as Gitea Actions were introduced in March 2023
LIMIT_DATE = datetime(2023, 1, 1)

def get_all_repositories():
    repositories = []
    page = 1
    limit = 50
    while True:
        print(f"Page {page}")
        params = {
            'sort': 'stars',
            'order': 'desc',
            'is_private' : 'public',
            'limit': limit,
            'page': page
        }
        response = rq.get(f"{URL}/repos/search", headers=HEADERS, params=params)
        if response.status_code == 200:
            data = response.json()
            repositories.extend(data['data'])
            if len(data['data']) < limit:
                break
            page += 1
    return repositories

def get_repo_contents(owner, repo):
    # Get all files in a repository
    response = rq.get(f"{URL}/repos/{owner}/{repo}/contents/.gitea/workflows", headers=HEADERS)
    return response

def check_workflows_in_gitea(repo):
    # Check if .gitea exists and contains a workflows directory
    contents = get_repo_contents(repo['owner']['login'], repo['name'])
    if contents.status_code == 200:
        contents = contents.json()
        for item in contents:
            if item['type'] == 'dir' and item['name'] == '.gitea/workflows':
                return repo
            elif item['type'] == 'file' and '.gitea/workflows' in item['path']:
                return repo
    return None


def main():
    repositories = get_all_repositories()
    filtered_repos = [repo for repo in repositories if datetime.strptime(repo['updated_at'], "%Y-%m-%dT%H:%M:%SZ") > LIMIT_DATE and not repo['empty']]
    print(len(filtered_repos))
    results = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(check_workflows_in_gitea, repo) for repo in filtered_repos]

        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    return results

if __name__ == "__main__":
    results = main()
    with open('reposWithForkedArchived.txt', 'w') as f:
        for repo in results:
            f.write(f"{repo['owner']['login']}/{repo['name']}\n")
    print(len(results))