import pandas as pd
from snapshoter import *


def extract_languages_by_repository(snapshot):

    # First thing to do is to remove duplicates from the snapshot
    # Base on the repository name because a repository can have multiple workflows but still use the same language
    snapshot = snapshot.drop_duplicates(subset=['repository']).copy()

    # Then the repository name must be converted to match the one in the repositories.csv files
    # Inside 200_workflowsonly.csv the repository name is in the format owner:repo
    # But in the repositories.csv file the format is owner/repo
    # So we need to replace the ":" by "/"
    snapshot['repository'] = snapshot['repository'].str.replace(':', '/', regex=False)

    # Load the repositories.csv file
    repositories = pd.read_csv('../dataset/repositories.csv')

    merged = snapshot.merge(repositories, left_on='repository', right_on='name')

    language_counts = merged['language'].value_counts().to_dict()

    return language_counts

if __name__ == "__main__":
    df = pd.read_csv('../dataset/200_workflowsonly.csv')
    df = df.dropna(subset=['file_hash'])
    df['valid_yaml'] = df['valid_yaml'].astype(bool)

    filtered_df = delete_invalid_yaml_records(df)
    snapshot = extract_snapshot(filtered_df, 2019)

    language_counts = extract_languages_by_repository(snapshot)

    print("Languages counts: ")
    print(language_counts)