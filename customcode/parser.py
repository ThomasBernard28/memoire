import pandas as pd
import os
from ruamel.yaml import YAML

yaml = YAML(typ='safe')

def parse_workflow(file_path):
    if not os.path.isfile(file_path):
        print(f"File {file_path} not found.")
        return None

    try:
        with open(file_path, 'r', encoding="utf-8") as f:
            yaml_data = yaml.load(f)
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
            step_details.append({
                "name" : step.get("name", "Unkmown"),
                "uses" : step.get("uses", None),
                "run" : step.get("run", None)
            })

        workflow_info["jobs"][job_name] = {
            "steps_count": len(steps),
            "uses_github_actions": any("uses" in step for step in steps),
            "uses_commands": any("run" in step for step in steps),
            "step_details": step_details
        }

    return workflow_info

def count_workflows_per_year(df):
    df['committed_year'] = pd.to_datetime(df['committed_date'], unit='s').dt.year
    committed_counts = df['committed_year'].value_counts().sort_index()

    print("Number of records per year: ")
    print(committed_counts)