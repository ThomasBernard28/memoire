import pandas as pd
import os
from ruamel.yaml import YAML

yaml = YAML(typ='safe')
source_folder = "../dataset/workflows"

def parse_snapshot(snapshot):
    """
    This method will call a parsing method on each of the workflow files from a snapshot.
    :param snapshot: The snapshot of the dataset that contains the workflow files.
    :return: A list of tuples with the repository name, file hash and the parsed workflow.
    """
    parsed_workflows = []
    # The first element returned by iterrows() is the index, so I use _ to ignore it.
    for _, workflow in snapshot.iterrows():
        # Then I call the parse_workflow method to parse the workflow file.
        parsed_workflow = parse_workflow(workflow['file_hash'])
        if parsed_workflow:
            parsed_workflows.append((workflow['repository'], workflow['file_hash'] ,parsed_workflow))
        else:
            print("Workflow could not be parsed")

    return parsed_workflows

def parse_workflow(file_hash):
    """
    The goal of this method is to parse a workflow file using the ruamel.yaml library.
    The workflow file is stored locally in the given dataset folder.
    :param file_hash: The file hash of the workflow file to find it in the dataset.
    :return: The result of the parsing using ruamel.yaml.
    """

    file_path = f"{source_folder}/{file_hash}"
    # Check if the file exists
    if not os.path.isfile(file_path):
        print(f"File {file_hash} not found.")
        return None

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            yaml_data = yaml.load(f)
    except Exception as e:
        print(f"Error while parsing YMAL file: {e}")
        return None

    return yaml_data

def parse_events(yaml_data):
    """
    This method aims to extract the different trigger events from a parsed workflow file.
    This workflow file is represented as yaml_data in the ruamel.yaml format.
    :param yaml_data: The parsed workflow file.
    :return: A list of all the trigger events from the workflow file.
    """

    # If the yaml_data is not a dictionary, return an empty list
    # It means that the file is not a valid workflow file
    if not isinstance(yaml_data, dict):
        return []

    # Get the "on" key from the yaml_data
    event_data = yaml_data.get("on", [])

    # Then check the different types for the representation of the events.
    # Return the events as a list of strings depending on the type of the event_data.
    if isinstance(event_data, dict):
        return list(event_data.keys())
    elif isinstance(event_data, list):
        return event_data
    elif isinstance(event_data, str):
        return [event_data]
    else:
        return []

def extract_jobs(yaml_data):
    if not isinstance(yaml_data, dict):
        return []

    jobs = yaml_data.get("jobs", {})
    if not isinstance(jobs, dict):
        return []

    return jobs

def parse_steps(yaml_data):
    """
    This method aims to extract the different steps from a parsed workflow file.
    :param yaml_data: The parsed workflow file.
    :return: A list of all the steps from the workflow file.
    """

    # If the yaml_data is not a dictionary, return an empty list
    # It means that the file is not a valid workflow file
    if not isinstance(yaml_data, dict):
        return []

    steps_list = []
    # First, get the jobs. They contain the steps
    jobs = yaml_data.get("jobs", {})
    if not isinstance(jobs, dict):
        return []

    # Iterate through all the jobs
    for job_name, job in jobs.items():
        if isinstance(job, dict):
            # Extract the steps if there are any or assign an empty list
            steps = job.get("steps", [])
            if isinstance(steps, list):
                for step in steps:
                    if isinstance(step, dict):
                        step_info = {
                            'job_name': job_name,
                            'run': step.get('run'),
                            'uses': step.get('uses'),
                            'original_step': step
                        }
                        steps_list.append(step_info)

    return steps_list

def parse_strategies(jobs):
    """
    This method parses matrix strategies directly from jobs previously parsed
    :param jobs: The parsed job from which to extract strategies.
    :return: A list of all the strategies from the jobs.
    """
    if not isinstance(jobs, dict):
        return []

    strategies_list = []

    for job_name, job in jobs.items():
        if isinstance(job, dict):
            strategy = job.get("strategy", [])
            if isinstance(strategy, dict):
                matrix = strategy.get("matrix", None)
                if isinstance(matrix, dict) and matrix:
                    strategies_list.append(matrix)

    return strategies_list

def parsed_global_permissions(yaml_data):
    """
    This method is used to parse the global permissions from a parsed workflow file.
    :param yaml_data: The parsed workflow file as a dictionary.
    :return: A list of all the global permissions from the workflow file.
    """
    if not isinstance(yaml_data, dict):
        return []

    permissions = yaml_data.get("permissions", [])
    if not isinstance(permissions, dict):
        return []

    return permissions

def parse_jobs_permissions(jobs):
    """
    This method is used to parse the jobs' permissions from a parsed workflow file.
    :param jobs: The list of parsed jobs from the workflow file.
    :return: A list of all the jobs' permissions from the workflow file.
    """
    if not isinstance(jobs, dict):
        return []

    permissions_list = []
    for job_name, job in jobs.items():
        if isinstance(job, dict):
            permissions = job.get("permissions", [])
            if isinstance(permissions, dict):
                permissions_list.append(permissions)

    return permissions_list


def count_workflows_per_year(df):
    """
    This method is used to count the number of workflows per year.
    :param df: The dataframe containing all the workflows
    """
    df['committed_year'] = pd.to_datetime(df['committed_date'], unit='s').dt.year
    committed_counts = df['committed_year'].value_counts().sort_index()

    print("Number of records per year: ")
    print(committed_counts)