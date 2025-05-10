import pandas as pd
from snapshoter import *
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

        events = parser.extract_events(parsed_workflow)

        results.append((repository, file_hash, events))

    return results

def extract_global_strategies_from_parsed(parsed_workflows):

    results = []

    for repository, file_hash, parsed_workflow in parsed_workflows:

        jobs = parser.extract_jobs(parsed_workflow)

        strategies = parser.extract_global_strategies(jobs) or []

        results.append((repository, file_hash, strategies))

    return results

def count_events(events_snapshots, repositories):

    events_dataframes = []
    yearly_events_medians = []
    counter = 0

    # Iterate through the events snapshots
    for year, events in events_snapshots:

        workflows_using_events = {}
        repositories_using_events = {}
        event_by_workflow_counts = {}

        for repo, file_hash, event_list in events:
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

        # Create the event dataframe
        event_repartition_df = pd.DataFrame(event_rows).sort_values(by='workflow_proportion', ascending=False).reset_index(drop=True)
        events_dataframes.append((year, event_repartition_df))

        # Compute the median of the events by workflow for the present year
        event_by_workflow_df = pd.DataFrame(workflow_rows)
        yearly_median = event_by_workflow_df['event_count'].median()
        yearly_events_medians.append((year, yearly_median))

        counter += 1

    return events_dataframes, yearly_events_medians


def extract_step_type_from_parsed(parsed_workflows):
    """
    This method is similar to the previous one, but it aims to extract the steps from the parsed workflows.
    The returned values are also later used to compute different statistics.
    :param parsed_workflows: The parsed workflows from the snapshot.
    :return: A list of tuples with the repository name, file hash and the steps. Each tuple represents a workflow file.
    """
    results = []

    for repository, file_hash, parsed_workflow in parsed_workflows:
        steps = parser.extract_steps(parsed_workflow)

        results.append((repository, file_hash, steps))

    return results

def count_global_strategies(strategies_snapshots, repositories):

    strategies_dataframes = []
    yearly_strategies_medians = []
    counter = 0

    # Iterate through the strategies snapshots
    for year, strategies in strategies_snapshots:

        workflows_using_strategies = set()
        repo_using_strategies = set()
        strategy_by_workflow_counts = {}

        for repo, file_hash, strategy_list in strategies:

            for strategy in strategy_list:

                if strategy:

                    # Add a workflow to the set of workflows using strategies when a strategy is found
                    workflows_using_strategies.add(file_hash)

                    # Add a repository to the set of repositories using strategies when a strategy is found
                    repo_using_strategies.add(repo)

                    # This count is used to determine the number of strategies by workflow to compute the median
                    if file_hash not in strategy_by_workflow_counts:
                        strategy_by_workflow_counts[file_hash] = 0
                    strategy_by_workflow_counts[file_hash] += 1


        strategy_results = {
            'workflow_proportion': round((len(workflows_using_strategies) / len(strategies)) * 100, 2),
            'repo_proportion' : round((len(repo_using_strategies) / len(repositories[counter][1])) * 100, 2)
        }

        workflow_rows = []
        for workflow in strategy_by_workflow_counts:
            workflow_rows.append({
                'workflow': workflow,
                'strategy_count': strategy_by_workflow_counts[workflow]
            })

        # Create the strategy dataframe
        strategy_repartition_df = pd.DataFrame([strategy_results])
        strategies_dataframes.append((year, strategy_repartition_df))

        # Compute the median of the strategies by workflow for the present year
        strategy_by_workflow_df = pd.DataFrame(workflow_rows)
        yearly_median = strategy_by_workflow_df['strategy_count'].median()
        yearly_strategies_medians.append((year, yearly_median))

        counter += 1


    return strategies_dataframes, yearly_strategies_medians




if __name__ == "__main__":
    df = pd.read_csv('../dataset/200_workflowsonly.csv')
    df = df.dropna(subset=['file_hash'])
    df['valid_yaml'] = df['valid_yaml'].astype(bool)

    filtered_df = delete_invalid_yaml_records(df)
    snapshot = extract_snapshot(filtered_df, 2019)
    parsed_workflows = parser.parse_snapshot(snapshot)

    '''
    language_counts = extract_languages_by_repository(snapshot)

    print("Languages counts: ")
    print(language_counts)
    '''
    '''
    median_characteristics = extract_repos_characteristics(snapshot)
    print("Repos median characteristics: ")
    print(median_characteristics)
    '''
    events = extract_events_from_parsed(parsed_workflows)
    #steps = extract_step_type_from_parsed(parsed_workflows)

    print(events[0][0])
    print(events[0][1])
    print(events[0][2])
