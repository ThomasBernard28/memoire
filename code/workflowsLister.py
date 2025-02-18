import requests as rq
import pandas as pd
import ast
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_repo_contents(owner, repo, headers, url):
    # Get all files in a repository
    response = rq.get(f"{url}/repos/{owner}/{repo}/contents/.gitea/workflows", headers=headers)
    return response

def check_workflows_in_gitea(repo, headers, url):
    # Check if .gitea exists and contains a workflows directory
    contents = get_repo_contents(repo['owner']['login'], repo['name'], headers, url)
    if contents.status_code == 200:
        contents = contents.json()
        for item in contents:
            if item['type'] == 'dir' and item['name'] == '.gitea/workflows':
                return repo
            elif item['type'] == 'file' and '.gitea/workflows' in item['path']:
                return repo
    return None

def save_repositories_to_csv(filename, headers, url):
    repos = pd.read_csv(f"../data/{filename}.csv")
    results = []
    for i in range(len(repos)):
        repo = repos.iloc[i].to_dict()
        repo['owner'] = ast.literal_eval(repo['owner'])
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(check_workflows_in_gitea, repo, headers, url)]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)

    df = pd.DataFrame(results)
    df.to_csv(f"data/gitea_repos_with_actions.csv", index=False)