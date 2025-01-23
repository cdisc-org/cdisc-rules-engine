import subprocess


def run_command(args):
    try:
        process = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True,
            shell=True,
        )
        return (
            process.returncode,
            process.stdout.lower(),
            process.stderr.lower(),
        )
    except subprocess.CalledProcessError as e:
        return e.returncode, e.stdout.lower(), e.stderr.lower()
