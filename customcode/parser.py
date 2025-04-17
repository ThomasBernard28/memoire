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
            parsed_workflows.append((workflow['repository'], parsed_workflow))
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

'''
def parse_workflow(file_hash):
    file_path = f"{source_folder}/{file_hash}"
    if not os.path.isfile(file_path):
        print(f"File {file_path} not found.")
        return None

    try:
        with open(file_path, 'r', encoding="utf-8") as f:
            yaml_data = yaml.load(f)
            if yaml_data is None:
                return None
            if not isinstance(yaml_data, dict):
                print(f"Unexpected YAML structure in {file_hash}: {type(yaml_data)}")
                return None
    except Exception as e:
        print(f"Error while parsing YAML: {e}")
        return None

    #Extract workflow info
    workflow_info = {
        "file_path": file_path,
        "lines_count": sum(1 for _ in open(file_path, encoding="utf-8")),
        "events": [],
        "jobs_count": len(yaml_data.get("jobs", {})),
        "jobs": {}
    }

    event_data = yaml_data.get("on", [])
    # If triggers are defined as a dictionary, extract the keys
    if isinstance(event_data, dict):
        workflow_info["events"] = list(event_data.keys())
    # elif the triggers are defined as a list, use the list as is
    elif isinstance(event_data, list):
        workflow_info["events"] = event_data
    # else if there is only one trigger defined as a string, use it as is
    else:
        workflow_info["events"] = [event_data] if isinstance(event_data, str) else []

    # Code to analyze and extract information about jobs and events
    for job_name, job_data in yaml_data.get("jobs", {}).items():
        steps = job_data.get("steps", [])

        # Get step details
        step_details = []
        for step in steps:
            if isinstance(step, dict):
                step_details.append({
                    "name" : step.get("name", "Unkmown"),
                    "uses" : step.get("uses", None),
                    "run" : step.get("run", None)
                })
            else:
                step_details.append({
                    "name": "NonDictionaryStep",
                    "uses": None,
                    "run": str(step)
                })
        workflow_info["jobs"][job_name] = {
            "steps_count": len(steps),
            "uses_github_actions": any("uses" in step for step in steps),
            "uses_commands": any("run" in step for step in steps),
            "step_details": step_details
        }

    return workflow_info
'''
def count_workflows_per_year(df):
    df['committed_year'] = pd.to_datetime(df['committed_date'], unit='s').dt.year
    committed_counts = df['committed_year'].value_counts().sort_index()

    print("Number of records per year: ")
    print(committed_counts)