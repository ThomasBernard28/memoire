import requests as rq

class ApiInterrogator:

    def __init__(self, base_url, token):
        self.URL = base_url
        self.HEADERS = {
            'Authorization': f"token {token}",
            'Content-Type': 'application/json'
        }

    def get_authenticated_user_repositories(self):
        url = f"{self.URL}/user/repos"
        return rq.get(url, headers=self.HEADERS)

    def get_userid_by_username(self, name):
        url = f"{self.URL}/users/{name}"
        return rq.get(url, headers=self.HEADERS, params={'username': name})

    def search_repositories(self, params):
        url = f"{self.URL}/repos/search"
        return rq.get(url, headers=self.HEADERS, params=params)

    def get_all_commits(self, owner, repo, params):
        url = f"{self.URL}/repos/{owner}/{repo}/commits"
        return rq.get(url, headers=self.HEADERS, params=params)