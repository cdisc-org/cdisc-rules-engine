import subprocess


def run_command(args):
    try:
        completed_process = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True,
            # shell=True,
        )
        return (
            completed_process.returncode,
            completed_process.stdout.lower(),
            completed_process.stderr.lower(),
        )
    except subprocess.CalledProcessError as e:
        return e.returncode, e.stdout.lower(), e.stderr.lower()
