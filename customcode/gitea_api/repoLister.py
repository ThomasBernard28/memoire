import json
import requests as rq
import pandas as pd
from datetime import datetime

# Limit Date set to 01/01/2023 as Gitea Actions were introduced in March 2023
LIMIT_DATE = datetime(2023, 1, 1)

def get_all_repositories(headers, url):
    """
    This method is used to get all repositories from gitea
    :param headers: The headers of the request containing authentication
    :param url: The base url of the Gitea API
    :return: A list of repositories from gitea
    """
    repositories = []
    page = 0
    # Maximum number of repositories per page of result i.e per request
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
            # In this case we reached to total amount of repositories on Gitea
            if len(data['data']) < limit:
                break
            page += 1

    # Only filter empty repositories
    no_empty_filter = [repo for repo in repositories if not repo['empty']]
    # Filter empty repositories and also prevent repositories not updated since Gitea Actions creation to be considered
    filtered_repositories = [repo for repo in repositories if datetime.strptime(repo['updated_at'], "%Y-%m-%dT%H:%M:%SZ") > LIMIT_DATE and not repo['empty']]

    return no_empty_filter

def save_repositories_to_csv(filename, headers, url):
    """
    This method is used to save repositories to a csv file
    :param filename: The name of the csv file
    :param headers: The headers of the request containing authentication to trigger the gathering of repositories
    :param url: The url of the Gitea API
    :return: Nothing but saves the repositories to a csv file
    """
    repos_list = get_all_repositories(headers, url)
    df = pd.DataFrame(repos_list)
    df['owner'] = df['owner'].apply(json.dumps)
    df.to_csv(f"../data/{filename}.csv", index=False)




