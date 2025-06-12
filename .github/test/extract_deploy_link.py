import os
import requests
import zipfile
import io
import re
import glob

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
if GITHUB_TOKEN is None:
    raise ValueError("GITHUB_TOKEN environment variable not set")

WORKFLOW_RUNS_URL = (
    "https://api.github.com/repos/cdisc-org/conformance-rules-editor/actions/runs"
)

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

print(os.listdir("logs"))
print(os.listdir(os.path.join("logs", "Build and Deploy Preview")))

log_dir = os.path.join("logs")
# List all .txt files in the directory
file_path = None
for filename in os.listdir(log_dir):
    if filename.endswith(".txt") and "Build and Deploy Preview" in filename:
        file_path = os.path.join(log_dir, filename)
        print(f"Found log file: {file_path}")

commit_files = glob.glob(
    os.path.join(
        log_dir, "Build and Deploy Preview", "[0-9]_Build and Deploy Preview.txt"
    )
)

if not file_path:
    print("No matching Build and Deploy Preview log file found")

if not commit_files:
    print("No matching SHA Commit log file found")

use_file_path = None
if file_path:
    use_file_path = file_path
elif commit_files:
    use_file_path = commit_files[0]
else:
    raise FileNotFoundError("No log file found")


if not os.path.exists(use_file_path):
    raise FileNotFoundError(f"{use_file_path} not found")


with open(use_file_path, "r", encoding="utf-8", errors="ignore") as f:
    log_content = f.read()

# Find preview URL
preview_match = re.search(
    r"https:\/\/[a-z0-9-]+\.centralus\.azurestaticapps\.net", log_content
)
if preview_match:
    preview_url = preview_match.group(0)
    print("ICYFLOWER Deploy Link Found:")
    print(preview_url)
    # Save it to GitHub Actions environment
    with open(os.environ["GITHUB_ENV"], "a") as env_file:
        env_file.write(f"RULE_EDITOR_URL={preview_url}\n")
else:
    print("No ICYFLOWER deploy link found in the logs.")
