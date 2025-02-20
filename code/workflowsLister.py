import requests as rq
import pandas as pd
import ast
from concurrent.futures import ThreadPoolExecutor, as_completed

def save_repositories_to_csv(filename, headers, url, ci_service):
    repos = pd.read_csv(f"../data/{filename}.csv")
    results = []

    repo_list = []
    for i in range((len(repos))):
        if i % 50 == 0:
            print(f"Repo {i}")
        repo = repos.iloc[i].to_dict()
        repo['owner'] = ast.literal_eval(repo['owner'])
        repo_list.append(repo)


    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(check_ci_service_in_gitea, repo, headers, url, ci_service) for repo in repo_list]
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    df = pd.DataFrame(results)
    df.to_csv(f"../data/gitea_repos_with_woodpecker.csv", index=False)


def check_ci_service_in_gitea(repo, headers, url, ci_service):
    if ci_service == 'gitea':
        return check_workflows_in_gitea(repo, headers, url)
    elif ci_service == "woodpecker":
        return check_workflows_in_woodpecker(repo, headers, url)


def get_repo_contents(owner, repo, headers, url, ci_service):
    # Get all files in a repository
    response = rq.get(f"{url}/repos/{owner}/{repo}/contents/{ci_service}", headers=headers)
    return response

def check_workflows_in_gitea(repo, headers, url):
    # Check if .gitea exists and contains a workflows directory
    contents = get_repo_contents(repo['owner']['login'], repo['name'], headers, url, ".gitea/workflows")
    if contents.status_code == 200:
        contents = contents.json()
        for item in contents:
            if item['type'] == 'dir' and item['name'] == '.gitea/workflows':
                return repo
            elif item['type'] == 'file' and '.gitea/workflows' in item['path']:
                return repo
    return None

def check_workflows_in_woodpecker(repo, headers, url):
    owner, repo_name = repo['owner']['login'], repo['name']

    response = get_repo_contents(owner, repo_name, headers, url, ".woodpecker.yaml")
    if response.status_code == 200:
        return repo

    response = get_repo_contents(owner, repo_name, headers, url, ".woodpecker")
    if response.status_code == 200:
        contents = response.json()
        if isinstance(contents, list):
            return repo