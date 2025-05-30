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
deploy_files = glob.glob(os.path.join(log_dir, "[0-9]_Build and Deploy Preview.txt"))
commit_files = glob.glob(
    os.path.join(log_dir, "Build and Deploy Preview", "[0-9]_Print commit SHA.txt")
)

if not deploy_files:
    raise FileNotFoundError("No matching Build and Deploy Preview log file found")

if not commit_files:
    print("No matching SHA Commit log file found")

deploy_file_path = deploy_files[0]
if not os.path.exists(deploy_file_path):
    raise FileNotFoundError(f"{deploy_file_path} not found")

commit_file_path = commit_files[0]
if not os.path.exists(commit_file_path):
    raise FileNotFoundError(f"{commit_file_path} not found")

with open(deploy_file_path, "r", encoding="utf-8", errors="ignore") as f:
    log_content = f.read()

with open(commit_file_path, "r", encoding="utf-8", errors="ignore") as f:
    commit_content = f.read()

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

# Find and print commit SHA
commit_match = re.search(r"Commit SHA:\s*([a-f0-9]{40})", commit_content)
if commit_match:
    commit_sha = commit_match.group(1)
    print("Editor Commit SHA Found:")
    print(commit_sha)

else:
    print("No commit SHA found in the logs.")
