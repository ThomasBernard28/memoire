import pandas as pd
import datetime

def extract_snapshot(df, year):
    '''
    This method is used to extract a snapshot of the dataframe at a specific date based on the year
    :param df: The dataframe from which to extract the snapshot
    :param year: The year of the snapshot
    :return: The snapshot dataframe
    '''
    # As repositories have been cloned between 2024-10-09 and 2024-10-10, we will consider snapshots to be around 202x-10-10
    local_df = df.copy()
    snpashot_date = datetime.datetime(year, 10, 10)

    # Convert the committed_date (int) to a datetime object
    local_df['committed_date'] = pd.to_datetime(local_df['committed_date'], unit='s')

    # Filter repositories that were committed after the snapshot date to keep only the ones that were committed before
    filtered_df = local_df[local_df['committed_date'] <= snpashot_date].copy()

    # Then sort the dataframe by commited_date and keep only the latest one for each uid
    filtered_df.sort_values(by='committed_date', inplace=True)
    snapshot_df = filtered_df.drop_duplicates(subset='uid', keep='last').copy()

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
    filtered_df = filtered_df[filtered_df['valid_yaml'].notna()].copy()

    return filtered_df


if __name__ == "__main__":
    df = pd.read_csv('../dataset/200_workflowsonly.csv')
    df = df.dropna(subset=['file_hash'])
    df['valid_yaml'] = df['valid_yaml'].astype(bool)
    print(f"Total number of records: {df.shape[0]}")

    print("------ Snapshot without filtering")
    year = 2019
    while year <= 2024:
        snapshot = extract_snapshot(df, year)
        print(f"Year : {year}")
        print(f"Number of workflows in the snapshot: {snapshot.shape[0]}\n")
        year += 1

    print("------ Snapshot with filtering the invalid workflows")

    filtered_df = delete_invalid_yaml_records(df)

    year = 2019
    while year <= 2024:
        snapshot = extract_snapshot(filtered_df, year)
        print(f"Year: {year}")
        print(f"Number of workflows in the snapshot: {snapshot.shape[0]}\n")
        year += 1