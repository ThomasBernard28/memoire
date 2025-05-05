import pandas as pd
import os
from ruamel.yaml import YAML

yaml = YAML(typ='safe')
source_folder = "../dataset/workflows"

def parse_snapshot(snapshot):
    parsed_workflows = []
    for _, workflow in snapshot.iterrows():
        parsed_workflow = parse_workflow(workflow['file_hash'])
        if parsed_workflow:
            parsed_workflows.append((workflow['repository'], workflow['file_hash'] ,parsed_workflow))

    return parsed_workflows

def parse_workflow(file_hash):
    file_path = f"{source_folder}/{file_hash}"
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
    if not isinstance(yaml_data, dict):
        return []

    event_data = yaml_data.get("on", [])

    if isinstance(event_data, dict):
        return list(event_data.keys())
    elif isinstance(event_data, list):
        return event_data
    elif isinstance(event_data, str):
        return [event_data]
    else:
        return []

def extract_steps(yaml_data):
    if not isinstance(yaml_data, dict):
        return []

    steps_list = []
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