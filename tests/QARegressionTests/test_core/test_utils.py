import subprocess
from platform import system
from os import listdir, remove


def run_command(args, shell):
    try:
        completed_process = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True,
            encoding="utf8",
            # test_test_command and windows seem to be happy with shell=True
            # test_validate on linux needs shell=False
            shell=shell or system() == "Windows",
        )
        return (
            completed_process.returncode,
            completed_process.stdout.lower(),
            completed_process.stderr.lower(),
        )
    except subprocess.CalledProcessError as e:
        return e.returncode, e.stdout.lower(), e.stderr.lower()


def tearDown():
    for file_name in listdir("."):
        if file_name not in ("host.json", "local.settings.json") and (
            file_name.endswith(".xlsx") or file_name.endswith(".json")
        ):
            remove(file_name)
