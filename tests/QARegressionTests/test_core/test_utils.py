import subprocess
from platform import system


def run_command(args, shell):
    try:
        completed_process = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True,
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
