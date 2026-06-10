import subprocess
from platform import system
from os import environ, listdir, remove


def run_command(args, shell):
    env = environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONUTF8"] = "1"

    try:
        completed_process = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True,
            encoding="utf8",
            shell=shell or system() == "Windows",
            env=env,
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
