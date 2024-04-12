import sys
import re

MAJOR = "MAJOR"
MINOR = "MINOR"
PATCH = "PATCH"


def increment_version(version_str, part):
    """Increment the specified part of the version string."""
    major, minor, patch = map(int, version_str.split("."))

    if part == MAJOR:
        major += 1
        minor = 0
        patch = 0
    elif part == MINOR:
        minor += 1
        patch = 0
    elif part == PATCH:
        patch += 1

    return f"{major}.{minor}.{patch}"


def update_version_file(version_str):
    """Update version.py with the new version string."""
    with open("version.py", "w") as f:
        f.write(f'__version__ = "{version_str}"\n')


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in [MAJOR, MINOR, PATCH]:
        print(f"Usage: python update_version.py <{MAJOR}|{MINOR}|{PATCH}>")
        sys.exit(1)

    part = sys.argv[1]

    # Read current version from version.py
    with open("version.py", "r") as f:
        version_line = f.readline()
        current_version = re.search(r"\d+\.\d+\.\d+", version_line).group()

    # Increment the appropriate part of the version
    new_version = increment_version(current_version, part)

    # Update version.py with the new version
    update_version_file(new_version)

    print(f"{current_version} -> {new_version}")
