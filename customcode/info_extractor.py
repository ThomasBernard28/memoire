import pandas as pd
from snapshoter import *
from itertools import combinations
import parser


def delete_duplicate_and_reformat_repository_name(not_unique_df):
    """
    This method aims to delete the duplicated repositories from a snapshot and reformat the repository name
    to match the one in the repositories.csv file.
    :param not_unique_df: The dataframe with duplicated repositories and the repository name in the format owner:repo
    :return: A unique dataframe with the repository name in the format owner/repo
    """

    # The first thing to do is to remove duplicates from the snapshot
    # Base on the repository name because a repository can have multiple workflows.
    unique_df = not_unique_df.drop_duplicates(subset=['repository']).copy()

    # Then the repository name from the snapshot must be converted to match the one in the repositories.csv files
    # Inside 200_workflowsonly.csv the repository name is in the format owner:repo
    # But in the repositories.csv file the format is owner/repo
    unique_df['repository'] = unique_df['repository'].str.replace(':', '/', regex=False)

    return unique_df

def extract_languages_by_repository(snapshot):
    """
    This method aims to extract the languages used in different repositories base on a snapshot.
    To do so, we merge the snapshot with the repositories.csv file to retrieve only the repositories that are
    present in the snapshot.
    :param snapshot: The snapshot dataframe from which to extract the languages
    :return: A dictionary with the languages and their counts
    """

    # Process the snapshot to remove duplicates and reformat the repository name
    formatted_snapshot = delete_duplicate_and_reformat_repository_name(snapshot)

    # Load the repositories.csv file
    repositories = pd.read_csv("../dataset/repositories.csv")

    merged = formatted_snapshot.merge(repositories, left_on='repository', right_on='name')

    language_counts = merged['language'].value_counts().to_dict()

    return language_counts, len(merged)

def extract_repos_characteristics(snapshot):
    """
    This method aims to extract the median of the different repositories' characteristics for a given snapshot.
    :param snapshot: The snapshot dataframe from which to extract the characteristics
    :return: A dataframe with the median of the characteristics
    """

    # Process the snapshot to remove duplicates and reformat the repository name
    formatted_snapshot = delete_duplicate_and_reformat_repository_name(snapshot)

    # Load the repositories.csv file
    repositories = pd.read_csv("../dataset/repositories.csv")

    merged = formatted_snapshot.merge(repositories, left_on='repository', right_on='name')

    # Compute and extract the median of characteristics for the all set of repositories in merged into a dataframe
    characteristics = [
        'commits', 'branches', 'releases', 'contributors',
        'stars', 'issues', 'pullrequests', 'codelines', 'size'
    ]

    median_characteristics = merged[characteristics].median()

    return median_characteristics


def extract_events_from_parsed(parsed_workflows):
    """
    This method aims to extract the trigger events from the parsed workflows.
    The returned values are later used to compute different statistics.
    :param parsed_workflows: The parsed workflows from the snapshot.
    :return: A list of tuples with the repository name, file hash and the events. Each tuple represents a workflow file.
    """
    results = []

    for repository, file_hash, parsed_workflow in parsed_workflows:

        events = parser.parse_events(parsed_workflow)

        results.append((repository, file_hash, events))

    return results

def extract_step_type_from_parsed(parsed_workflows):
    """
    This method is similar to the previous one, but it aims to extract the steps from the parsed workflows.
    The returned values are also later used to compute different statistics.
    :param parsed_workflows: The parsed workflows from the snapshot.
    :return: A list of tuples with the repository name, file hash and the steps. Each tuple represents a workflow file.
    """
    results = []

    for repository, file_hash, parsed_workflow in parsed_workflows:
        steps = parser.parse_steps(parsed_workflow)

        results.append((repository, file_hash, steps))

    return results

def extract_strategies_from_parsed(parsed_workflows):
    """
    This method aims to extract the matrix strategies from the parsed workflows.
    At first, it parses jobs and then the matrix strategies from the jobs
    :param parsed_workflows: The parsed workflows from the snapshot.
    :return: A list of tuples with the repository name, file hash and the matrix strategy. Each tuple represents a workflow file.
    """

    results = []

    for repository, file_hash, parsed_workflow in parsed_workflows:

        jobs = parser.extract_jobs(parsed_workflow)

        strategies = parser.parse_strategies(jobs) or []

        results.append((repository, file_hash, strategies))

    return results

def extract_global_permissions_from_parsed(parsed_workflows):
    """
    This method aims to extract the global permissions (at the first level) from the parsed workflows.
    :param parsed_workflows: The parsed workflows from the snapshot.
    :return: A list of tuples with the repository name, file hash and the global permissions. Each tuple represents a workflow file.
    """

    results = []

    for repository, file_hash, parsed_workflow in parsed_workflows:

        permissions = parser.parse_global_permissions(parsed_workflow)

        results.append((repository, file_hash, permissions))

    return results

def extract_job_permissions_from_parsed(parsed_workflows):
    """
    This method aims to extract the job-level permissions from the parsed workflows.
    At first, it parses jobs and then the permissions from the jobs
    :param parsed_workflows: The parsed workflows from the snapshot.
    :return: A list of tuples with the repository name, file hash and the global permissions. Each tuple represents a workflow file.
    """
    results = []

    for repository, file_hash, parsed_workflow in parsed_workflows:

        jobs = parser.extract_jobs(parsed_workflow)
        permissions = parser.parse_jobs_permissions(jobs)
        results.append((repository, file_hash, permissions))

    return results

def extract_jobs_from_parsed(parsed_workflows):
    """
    This method aims to extract the jobs based on the parsed workflows.
    :param parsed_workflows: The parsed workflows from the snapshot.
    :return: A list of tuples with the repository name, file hash and the jobs. Each tuple represents a workflow file.
    """
    results = []
    for repository, file_hash, parsed_workflow in parsed_workflows:
        jobs = parser.extract_jobs(parsed_workflow)

        results.append((repository, file_hash, jobs))

    return results

def count_jobs(jobs_snapshots):
    """
    This method is used to count the jobs for each snapshot to computer the median number of jobs per workflow.
    :param jobs_snapshots: A list of snapshots containing the parsed jobs for each workflow
    :return: A list of tuples with the year and computer median number of jobs per snapshot.
    """
    yearly_jobs_median = []

    for year, jobs in jobs_snapshots:

        jobs_by_workflows_counts = {}

        for repo, workflow, job_list in jobs:

            # The idea is only to iterate through the list, not really considering the content of the job
            for job in job_list:
                if not workflow in jobs_by_workflows_counts:
                    jobs_by_workflows_counts[workflow] = 0
                jobs_by_workflows_counts[workflow] += 1

        workflow_rows = []
        for workflow in jobs_by_workflows_counts:
            workflow_rows.append({
                'workflow': workflow,
                'jobs_count': jobs_by_workflows_counts[workflow],
            })

        job_by_workflow_df = pd.DataFrame(workflow_rows)
        yearly_median = job_by_workflow_df['jobs_count'].median()
        yearly_jobs_median.append((year, yearly_median))

    return yearly_jobs_median

def count_events(events_snapshots, repositories):
    """
    This method aims to count trigger events and to compute metrics on them. Here, the proportion of workflow and
    repositories using each trigger event is computed. Another metric computed is the median number of trigger
    events per workflows per snapshot. Finally, the last thing extracted is the most popular pair of trigger events
    for each snapshot.
    :param events_snapshots: The parsed event for each snapshot.
    :param repositories: A list containing tuple of (year, repositories) where repositories is the list
    of repositories for the snapshot in the corresponding year.
    :return: 3 lists. The first list contains dataframes with the first metrics (the proportion of workflows and
    repositories using each trigger event) for each snapshot. The second list contains the median number of trigger
    events for each snapshot. The last list contains the most popular pair of trigger events for each snapshot.
    """
    events_dataframes = []
    yearly_events_medians = []
    most_popular_events_pairs_dataframes = []
    counter = 0

    # Iterate through the events snapshots
    for year, events in events_snapshots:

        workflows_using_events = {}
        repositories_using_events = {}
        event_by_workflow_counts = {}
        most_used_pairs = {}

        for repo, file_hash, event_list in events:
            # Create all the possible combinations of trigger events if the list is at least composed of 2 elems
            if len(event_list) >= 2:
                # With combinations there are cases like ('A', 'B') and ('B','A') that are considered different pairs
                raw_pairs = combinations(event_list, 2)
                all_pairs = []

                for pair in raw_pairs:
                    # Use sorted() on the pairs so that ('B', 'A') becomes ['A', 'B'] then use tuple() to transform
                    # in ('A', 'B')
                    sorted_pair = tuple(sorted(pair))
                    all_pairs.append(sorted_pair)


                for pair in all_pairs:
                    if not pair in most_used_pairs:
                        most_used_pairs[pair] = 0
                    most_used_pairs[pair] += 1

            for event in event_list:

                # If an event is not yet listed in the dictionary, create a new key for it
                if event not in workflows_using_events:
                    workflows_using_events[event] = 0
                workflows_using_events[event] += 1

                # If an event is not yet listed in the dictionary, create a new key for it
                if event not in repositories_using_events:
                    # Here I create a set so that if the same repo is used multiple times for the same event,
                    # it will not be counted multiple times
                    repositories_using_events[event] = set()
                repositories_using_events[event].add(repo)

                # This count is used to determine the number of events by workflow
                # It is then used to compute the median of the events by workflow
                if file_hash not in event_by_workflow_counts:
                    event_by_workflow_counts[file_hash] = 0
                event_by_workflow_counts[file_hash] += 1

        # Create the different rows for the events_dataframes
        event_rows = []
        for event in workflows_using_events:
            # At a particular key (event name) there is the number of workflows using it
            workflows_count = workflows_using_events[event]
            # At a particular key (event name) there is the set of repositories using it
            repo_count = len(repositories_using_events[event])

            event_rows.append({
                'event': event,
                'workflow_proportion': round((workflows_count / len(events)) * 100, 2),
                'repo_proportion' : round((repo_count / len(repositories[counter][1])) * 100, 2)
            })

        # Create the different rows for the yearly_events_medians

        workflow_rows = []
        for workflow in event_by_workflow_counts:
            workflow_rows.append({
                'workflow': workflow,
                'event_count': event_by_workflow_counts[workflow]
            })

        # Create the different rows for the most popular pairs
        pair_rows = []
        for pair in most_used_pairs:
            pair_rows.append({
                'pair': pair,
                'total': most_used_pairs[pair],
                'workflow_proportion': round((most_used_pairs[pair] / len(events)) * 100, 2)
            })

        # Create the event dataframe
        event_repartition_df = pd.DataFrame(event_rows).sort_values(by='workflow_proportion', ascending=False).reset_index(drop=True)
        events_dataframes.append((year, event_repartition_df))

        # Compute the median of the events by workflow for the present year
        event_by_workflow_df = pd.DataFrame(workflow_rows)
        yearly_median = event_by_workflow_df['event_count'].median()
        yearly_events_medians.append((year, yearly_median))

        # Create the pairs dataframe
        most_popular_events_pairs_df = pd.DataFrame(pair_rows).sort_values(by='total', ascending=False).reset_index(drop=True)
        most_popular_events_pairs_dataframes.append((year, most_popular_events_pairs_df))

        counter += 1

    return events_dataframes, yearly_events_medians, most_popular_events_pairs_dataframes

def count_steps(steps_snapshots, repositories):
    """
    This method aims to count steps and to compute metrics on them. Here, the proportion of steps being 'uses' and 'run'
    is computed alongside the proportion of repositories using such steps. Two other metrics are also computed: the top
    10 most used actions in 'uses' steps for each year, including the proportion of steps being this action and the
    proportion of repositories using it, and the last metric being the median number of steps in each workflow per year.
    :param steps_snapshots: The parsed steps for each snapshot.
    :param repositories: A list containing tuple of (year, repositories) where repositories is the list
    of repositories for the snapshot in the corresponding year.
    :return: 3 different lists. The first one contains dataframes with first metrics (proportion of uses and run in
    steps and repositories). The second one contains dataframes with the second metric (most used actions and their
    proportions). Finally, the third one contains dataframes with the third metric (median number of steps in each
    """

    steps_dataframes = []
    top10_actions_dataframes = []
    yearly_steps_medians = []

    counter = 0

    for year, steps_by_year in steps_snapshots:

        # Counters to track the number of steps
        total_steps = 0
        uses_steps = 0
        run_steps = 0

        # Dictionary to track the different actions used in the workflows
        actions_used_by_workflows = {}
        # Dictionary to track the different actions used in the repositories
        actions_used_by_repositories = {}

        # Dictionary to track the number of steps by workflow
        steps_by_workflow = {}

        # Sets to track repositories using 'run' and 'uses' steps while avoiding duplicates
        repos_with_uses = set()
        repos_with_run = set()

        for repository, workflow, steps in steps_by_year:
            has_uses = False
            has_run = False

            for step in steps:
                total_steps += 1

                if workflow not in steps_by_workflow:
                    steps_by_workflow[workflow] = 0
                steps_by_workflow[workflow] += 1

                if step.get('uses') is not None:
                    uses_steps += 1
                    has_uses = True
                    # Extract the action name from the step
                    # Keep the part before the '@' symbol
                    # The other part is the version of the action
                    action = step['uses'].split('@')[0]

                    # Adding action to the count to compute the proportion of steps using it
                    if action not in actions_used_by_workflows:
                        actions_used_by_workflows[action] = 0
                    actions_used_by_workflows[action] += 1

                    # For each action, create a set in the dictionary.
                    # The key of the dictionary is the action name, the value is a set of repositories using it.
                    if action not in actions_used_by_repositories:
                        actions_used_by_repositories[action] = set()
                    actions_used_by_repositories[action].add(repository)

                if step.get('run') is not None:
                    run_steps += 1
                    has_run = True

            # Only had the repository once to the set and not everytime a 'run' or 'uses' is detected
            if has_uses:
                repos_with_uses.add(repository)

            if has_run:
                repos_with_run.add(repository)

        step_rows = []

        for action in actions_used_by_workflows:
            action_uses_proportion = round((actions_used_by_workflows[action] / uses_steps) * 100, 2)
            action_repo_proportion = round((len(actions_used_by_repositories[action]) / len(repositories[counter][1])) * 100, 2)

            step_rows.append({
                'action': action,
                'step_proportion': action_uses_proportion,
                'repo_proportion': action_repo_proportion
            })

        event_results = {
            'total_steps': total_steps,
            'total_repositories': len(repositories[counter][1]),
            'uses_proportion': round((uses_steps / total_steps) * 100, 2),
            'run_proportion': round((run_steps / total_steps) * 100, 2),
            'repo_uses': round((len(repos_with_uses) / len(repositories[counter][1])) * 100, 2),
            'repo_run': round((len(repos_with_run) / len(repositories[counter][1])) * 100, 2)
        }

        workflow_rows = []
        for workflow in steps_by_workflow:
            workflow_rows.append({
                'workflow': workflow,
                'step_count': steps_by_workflow[workflow]
            })

        # Create the step dataframe
        steps_df = pd.DataFrame([event_results])
        steps_dataframes.append((year, steps_df))

        # Create the top 10 actions dataframe
        actions_df = pd.DataFrame(step_rows).sort_values(by='step_proportion', ascending=False).reset_index(drop=True)
        top10_actions_dataframes.append((year, actions_df.head(30)))

        # Compute the median of the steps by workflow for the present year
        steps_by_workflow_df = pd.DataFrame(workflow_rows)
        yearly_median = steps_by_workflow_df['step_count'].median()
        yearly_steps_medians.append((year, yearly_median))

        counter += 1

    return steps_dataframes, top10_actions_dataframes, yearly_steps_medians


def count_strategies(strategies_snapshots, repositories):
    """
    This method aims to count matrix strategies and to compute metrics on them. Here, the proportion of workflows
    and repositories using matrix strategies is computed. Another computed metric is the median number of matrix
    strategies per workflow through the years.
    :param strategies_snapshots: The parsed matrix strategies for each snapshot.
    :param repositories: A list containing tuple of (year, repositories) where repositories is the list
    of repositories for the snapshot in the corresponding year.
    :return: Two lists. The first one is containing a tuple (year, dataframe) where each dataframe corresponds to the
    repartitions results for matrix strategies. The second one is containing a tuple (year, median_number) where each
    median_number is the median number of matrix per workflow for each snapshot year.
    """
    strategies_dataframes = []
    yearly_strategies_median = []
    counter = 0

    for year, strategies in strategies_snapshots:

        # Using sets as there could be multiple strategies for each workflow or repository.
        workflows_using_strategies = set()
        repo_using_strategies = set()
        # Dictionary used to store the number of matrix strategy per workflow
        strategy_by_workflow_counts = {}

        for repo, workflow, strategy_list in strategies:

            has_strategy = False

            for strategy in strategy_list:

                # Boolean used for better performances. Only add once the workflow and repositories to the sets.
                if strategy:
                    has_strategy = True

                if workflow not in strategy_by_workflow_counts:
                    strategy_by_workflow_counts[workflow] = 0
                strategy_by_workflow_counts[workflow] += 1

            if has_strategy:
                workflows_using_strategies.add(workflow)
                repo_using_strategies.add(repo)

        strategies_proportions = {
            'workflow_proportion': round((len(workflows_using_strategies) / len(strategies)) * 100, 2),
            'repo_proportion': round((len(repo_using_strategies) / len(repositories[counter][1])) * 100, 2)
        }

        workflow_rows = []
        for workflow in strategy_by_workflow_counts:
            workflow_rows.append({
                'workflow': workflow,
                'strategy_count': strategy_by_workflow_counts[workflow]
            })

        strategy_repartition_df = pd.DataFrame([strategies_proportions])
        strategies_dataframes.append((year, strategy_repartition_df))

        strategy_by_workflow_df = pd.DataFrame(workflow_rows)
        yearly_median = strategy_by_workflow_df['strategy_count'].median()
        yearly_strategies_median.append((year, yearly_median))

        counter += 1


    return strategies_dataframes, yearly_strategies_median

def count_permissions(permissions_snapshots, repositories):
    """
    This method aims to count permissions to compute metrics on them. Here, the proportion of workflows
    and repositories using permissions is computed. This method is used for both global and job-level permissions.
    :param permissions_snapshots: The parsed permissions for each snapshot.
    :param repositories: A list containing tuple of (year, repositories) where repositories is the list
    of repositories for the snapshot in the corresponding year.
    :return: A list of tuples containing the dataframes of the repartition of permissions through the years.
    """
    permissions_dataframes = []
    counter = 0

    for year, permissions in permissions_snapshots:

        # Same thing as for matrix strategies but with permissions
        workflows_using_global_permissions = set()
        repo_using_global_permissions = set()

        for repo, workflow, permission_list in permissions:
            has_permission = False

            for permission in permission_list:

                if permission:
                    has_permission = True
                    break

            if has_permission:
                workflows_using_global_permissions.add(workflow)
                repo_using_global_permissions.add(repo)

        permissions_proportions = {
            'workflow_proportion': round((len(workflows_using_global_permissions) / len(permissions)) * 100, 2),
            'repo_proportion': round((len(repo_using_global_permissions) / len(repositories[counter][1])) * 100, 2)
        }

        permission_repartition_df = pd.DataFrame([permissions_proportions])
        permissions_dataframes.append((year, permission_repartition_df))

        counter += 1

    return permissions_dataframes
