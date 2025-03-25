import pandas as pd
import datetime

def extract_snapshot(df, year):
    # As repositories have been cloned between 2024-10-09 and 2024-10-10, we will consider snapshots to be around 202x-10-10

    date = datetime.datetime(year, 10, 10)
    lower_bound_date = date - datetime.timedelta(days=30)

    # Convert the committed_date (int) to a datetime object
    df['committed_date'] = pd.to_datetime(df['committed_date'], unit='s')

    # Filter repositories that were committed before the snapshot date with a limit of 1 month
    snapshot_df = df[
        (df['committed_date'] >= lower_bound_date) &
        (df['committed_date'] <= date)
    ].copy()

    return snapshot_df

def delete_uid_with_invalid_yaml(df):
    '''
    This method is used to delete all the records for each uid that has at least one invalid workflow (valid_yaml = True)
    :param df: The dataframe to filter
    :return: Filtered dataframe
    '''

    # First identify the uids that have at least one invalid workflow
    invalid_uids = df.loc[(df['valid_yaml'] == False), 'uid'].unique()

    # Then filter the dataframe to exclude these uids
    filtered_df = df[~df['uid'].isin(invalid_uids)].copy()

    return filtered_df

def delete_invalid_yaml_records(df):
    '''
    This method is used to delete all the records that are invalid (valid_yaml = False)
    It differs from the previous method as it only deletes the invalid and record and not the whole uid
    :param df: The dataframe to filter
    :return: Filtered dataframe
    '''

    filtered_df = df[df['valid_yaml'] == True].copy()

    return filtered_df

def get_most_recent_workflows(snapshot, year):
    '''
    This method is used to get the most recent workflow for each uid in the snapshot.
    The goal is also to exclude invalid workflows (invalid_yaml = True)
    :param year: The year of the snapshot
    :param snapshot:  dataframe
    :return: A filtered dataframe containing the most recent workflow for each uid
    '''
    snapshot_date = datetime.datetime(year, 10, 10)

    snapshot['days_from_date'] = (snapshot['committed_date'] - snapshot_date).abs()

    most_recent_workflows = snapshot.loc[
        snapshot.groupby('uid')['days_from_date'].idxmin()
    ]

    return most_recent_workflows