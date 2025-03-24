import json
import requests as rq
import pandas as pd
from datetime import datetime

# Limit Date set to 01/01/2023 as Gitea Actions were introduced in March 2023
LIMIT_DATE = datetime(2023, 1, 1)

def get_all_repositories(headers, url):
    repositories = []
    page = 0
    limit = 50
    while True:
        print(f"Page {page}")
        params = {
            'limit': limit,
            'page': page,
        }
        response = rq.get(f"{url}/repos/search", headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            repositories.extend(data['data'])
            if len(data['data']) < limit:
                break
            page += 1

    no_empty_filter = [repo for repo in repositories if not repo['empty']]
    #filtered_repositories = [repo for repo in repositories if datetime.strptime(repo['updated_at'], "%Y-%m-%dT%H:%M:%SZ") > LIMIT_DATE and not repo['empty']]

    return no_empty_filter

def save_repositories_to_csv(filename, headers, url):
    repos_list = get_all_repositories(headers, url)
    df = pd.DataFrame(repos_list)
    df['owner'] = df['owner'].apply(json.dumps)
    df.to_csv(f"../data/{filename}.csv", index=False)




