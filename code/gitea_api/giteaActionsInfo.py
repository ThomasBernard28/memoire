import sys
import repoLister as rl
import workflowsLister as wl

# Default URL
URL = "https://gitea.com/api/v1"
# TOKEN
TOKEN = "82b9c59c55aa72fabd8bdde707ccdfd165db8160"

# Request headers
HEADERS = {
    'Authorization': f"token {TOKEN}",
    'Content-Type': 'application/json'
}

if __name__ == "__main__":
    args = sys.argv
    if len(args) >= 1 :
        if args[1] == "getRepos":
            rl.save_repositories_to_csv("gitea_repos_list", HEADERS, URL)
        elif args[1] == "getReposWithWorkflows":
            wl.save_repositories_to_csv("gitea_repos_list", HEADERS, URL, args[2])
    else:
        print("Please enter a task as an argument")