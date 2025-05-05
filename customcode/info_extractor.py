import pandas as pd
from snapshoter import *
import parser


def delete_duplicate_and_reformat_repository_name(nunique_df):
    """
    This method aims to delete the duplicated repositories from a snapshot and reformat the repository name
    to match the one in the repositories.csv file.
    :param nunique_df: The dataframe with duplicated repositories and the repository name in the format owner:repo
    :return: A unique dataframe with the repository name in the format owner/repo
    """

    # The first thing to do is to remove duplicates from the snapshot
    # Base on the repository name because a repository can have multiple workflows.
    unique_df = nunique_df.drop_duplicates(subset=['repository']).copy()

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
    repositories = pd.read_csv('../dataset/repositories.csv')

    merged = formatted_snapshot.merge(repositories, left_on='repository', right_on='name')

    language_counts = merged['language'].value_counts().to_dict()

    return language_counts, len(merged)

def extract_repos_characteristics(snapshot):

    # Process the snapshot to remove duplicates and reformat the repository name
    formatted_snapshot = delete_duplicate_and_reformat_repository_name(snapshot)

    # Load the repositories.csv file
    repositories = pd.read_csv('../dataset/repositories.csv')

    merged = formatted_snapshot.merge(repositories, left_on='repository', right_on='name')

    # Compute and extract the median of characteristics for the all set of repositories in merged into a dataframe
    characteristics = [
        'commits', 'branches', 'releases', 'contributors',
        'stars', 'issues', 'pullrequests', 'codelines', 'size'
    ]

    median_characteristics = merged[characteristics].median()

    return median_characteristics


def extract_events_from_parsed(parsed_workflows):
    results = []

    for repository, file_hash, parsed_workflow in parsed_workflows:
        events = parser.extract_events(parsed_workflow)
        if events:
            results.append((repository, file_hash,events))

    return results

def extract_step_type_from_parsed(parsed_workflows):
    results = []

    for repository, file_hash, parsed_workflow in parsed_workflows:
        steps = parser.extract_steps(parsed_workflow)

        if steps:
            results.append((repository, file_hash, steps))

    return results


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
