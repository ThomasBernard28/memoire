import requests as rq
import pandas as pd
import ast
from concurrent.futures import ThreadPoolExecutor, as_completed

def save_repositories_to_csv(filename, headers, url, ci_service):
    repos = pd.read_csv(f"../data/gitea_actions_time/{filename}.csv", low_memory=False)
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
    df.to_csv(f"../data/gitea_repos_with_jenkins.csv", index=False)

def check_ci_service_in_gitea(repo, headers, url, ci_service):
    match ci_service:
        case "gitea":
            return check_workflows_in_gitea(repo, headers, url)
        case "woodpecker":
            return check_workflows_in_woodpecker(repo, headers, url)
        case "drone":
            return check_workflows_in_drone(repo, headers, url)
        case "gitlab":
            return check_workflows_in_gitlab(repo, headers, url)
        case "travis":
            return check_workflows_in_travis(repo, headers, url)
        case "circle":
            return check_workflows_in_circle(repo, headers, url)
        case "jenkins":
            return check_workflows_in_jenkins(repo, headers, url)


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

    response = get_repo_contents(owner, repo_name, headers, url, ".woodpecker.yml")
    if response.status_code == 200:
        return repo

    response = get_repo_contents(owner, repo_name, headers, url, ".woodpecker")
    if response.status_code == 200:
        contents = response.json()
        if isinstance(contents, list):
            return repo

def check_workflows_in_drone(repo, headers, url):
    owner, repo_name = repo['owner']['login'], repo['name']

    response = get_repo_contents(owner, repo_name, headers, url, ".drone.yml")
    if response.status_code == 200:
        return repo

    response = get_repo_contents(owner, repo_name, headers, url, ".drone.yaml")
    if response.status_code == 200:
        return repo

    return None

def check_workflows_in_gitlab(repo, headers, url):
    owner, repo_name = repo['owner']['login'], repo['name']

    response = get_repo_contents(owner, repo_name, headers, url, ".gitlab-ci.yml")
    if response.status_code == 200:
        return repo

    response = get_repo_contents(owner, repo_name, headers, url, ".gitlab-ci.yaml")
    if response.status_code == 200:
        return repo

    return None

def check_workflows_in_travis(repo, headers, url):
    owner, repo_name = repo['owner']['login'], repo['name']

    response = get_repo_contents(owner, repo_name, headers, url, ".travis.yml")
    if response.status_code == 200:
        return repo

    response = get_repo_contents(owner, repo_name, headers, url, ".travis.yaml")
    if response.status_code == 200:
        return repo

    return None

def check_workflows_in_circle(repo, headers, url):
    owner, repo_name = repo['owner']['login'], repo['name']

    response = get_repo_contents(owner, repo_name, headers, url, ".circleci")
    if response.status_code == 200:
        contents = response.json()
        for item in contents:
            if item['type'] == 'file' and ( item['name'] == "config.yml" or item['name'] == "config.yaml"):
                return repo

    return None

def check_workflows_in_jenkins(repo, headers, url):
    owner, repo_name = repo['owner']['login'], repo['name']

    response = get_repo_contents(owner, repo_name, headers, url, "Jenkinsfile")
    if response.status_code == 200:
        return repo

    return None