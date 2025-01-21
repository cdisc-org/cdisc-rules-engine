import subprocess


def run_command(command):
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
        text=True,
    )
    stdout, stderr = process.communicate()
    exit_code = process.returncode
    return exit_code, stdout.lower(), stderr.lower()
