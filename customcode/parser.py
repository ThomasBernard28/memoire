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

def extract_events(yaml_data):
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



def extract_steps(yaml_data):
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
    # First, get the jobs, they contain the steps
    jobs = yaml_data.get("jobs", {})
    if not isinstance(jobs, dict):
        return []

    for job_name, job in jobs.items():
        if isinstance(job, dict):
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
                    else:
                        steps_list.append({'job_name': job_name, 'bad_step_format': step})
            else:
                steps_list.append({'job_name': job_name, 'bad_steps_format': steps})
        else:
            steps_list.append({'job_name': job_name, 'bad_job_format': job})

    return steps_list

def count_workflows_per_year(df):
    df['committed_year'] = pd.to_datetime(df['committed_date'], unit='s').dt.year
    committed_counts = df['committed_year'].value_counts().sort_index()

    print("Number of records per year: ")
    print(committed_counts)