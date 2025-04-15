import os
import requests
import zipfile
import io
import re

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

WORKFLOW_RUNS_URL = "https://api.github.com/repos/cdisc-org/conformance-rules-editor/actions/runs"


headers = {"Authorization": f"token {GITHUB_TOKEN}"}
response = requests.get(WORKFLOW_RUNS_URL, headers=headers)
response.raise_for_status()
workflow_data = response.json()

first_run_id = workflow_data["workflow_runs"][0]["id"]
print(f"First workflow run ID: {first_run_id}")

logs_url = f"https://api.github.com/repos/cdisc-org/conformance-rules-editor/actions/runs/{first_run_id}/logs"


logs_response = requests.get(logs_url, headers=headers)
logs_response.raise_for_status()

with zipfile.ZipFile(io.BytesIO(logs_response.content)) as zip_file:
    zip_file.extractall("logs")


target_file_path = os.path.join(
    "logs",
    "Build and Deploy Preview",
    "5_Build and Deploy Preview.txt"
)

if not os.path.exists(target_file_path):
    raise FileNotFoundError(f"{target_file_path} not found")


with open(target_file_path, "r", encoding="utf-8", errors="ignore") as f:
    log_content = f.read()

# Regex for icyflower link (adjust if format changes)
match = re.search(r"https:\/\/[a-z0-9-]+\.centralus\.azurestaticapps\.net", log_content)
if match:
    preview_url = match.group(0)
    print("ICYFLOWER Deploy Link Found:")
    print(preview_url)
    # Save it to GitHub Actions environment
    with open(os.environ['GITHUB_ENV'], "a") as env_file:
        env_file.write(f"RULE_EDITOR_URL={preview_url}\n")
else:
    print("No ICYFLOWER deploy link found in the logs.")
