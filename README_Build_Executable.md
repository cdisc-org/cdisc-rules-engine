# Building CDISC Rules Engine Executable

## Option 1: Using GitHub Actions (Recommended)

### Step 1: Fork and Setup

1. Fork the repository: https://github.com/cdisc-org/cdisc-rules-engine
2. The workflow file `.github/workflows/build-version.yml` is already included in the main repository. It is contained within our .gitignore so you can customize it as you see fit.

### Step 2: Run the Build

1. Go to the top bar of the fork, click Settings > Security > Secrets and Variables > Actions
2. Click **New Repository Secret** and set an action secret named CDISC_LIBRARY_API_KEY and secret as your API key
3. Go to **Actions** tab in your forked repository
4. Click "Build Custom Executable"
5. Click **Run workflow**
6. Download the artifact when complete

### Step 3: Automated Builds (Optional)

To run builds automatically, uncomment the schedule section in the workflow:

```yaml
schedule:
  - cron: "0 2 * * 0" # Weekly on Sunday at 2 AM UTC
  # OR
  - cron: "0 2 * * *" # Daily at 2 AM UTC
```

## Troubleshooting

### Architecture Issues

You can build executables for different operating systems using GitHub's hosted runners. This creates platform-specific executables that work on different environments. See:

- https://docs.github.com/en/actions/concepts/runners/github-hosted-runners
- https://github.com/actions/runner-images

The runner in our workflow currently builds for ubuntu-22.04 but this can be changed to your particular OS, as well as CPU architectures (This will be different for Apple M chips that use ARM architecture versus Intel chips)

## Option 2: Using Docker Locally

### Prerequisites

- Docker Desktop installed and running
- Git
- **Note**: There is no official support for a macOS docker runner; Windows also requires some additional setup

### Step 1: Clone Repository

#### Linux/macOS/WSL/Windows Command Prompt/Powershell:

```bash
git clone https://github.com/cdisc-org/cdisc-rules-engine.git
cd cdisc-rules-engine
```

### Step 1.5: Update cache and code

When you clone the repo initially, it will come with an updated cache and main branch. Before subsequent local docker builds, you will want to follow the README to install the compatible python version of engine, create the virtual environment, and then update the cache as well as pulling down changes from main in cdisc-rules-engine root directory.

#### Linux/macOS/WSL/Git Bash/Windows Command Prompt & PowerShell:

```bash
# Set up upstream remote (only done once)
git remote add upstream https://github.com/cdisc-org/cdisc-rules-engine.git

# Pull latest changes from upstream main branch
git pull upstream main
```

### Step 2: Build with Docker

#### Linux/macOS/WSL/Git Bash:

```bash
# Build the executable
docker build -f Dockerfile.build -t cdisc-builder .

# Extract the executable and remove container
CONTAINER_ID=$(docker create cdisc-builder)
docker cp $CONTAINER_ID:/app/dist/output/core-ubuntu-22.04/core ./build-output/
docker rm $CONTAINER_ID

# Make executable (Linux/macOS only)
chmod +x ./build-output/core

echo "Executable ready: ./build-output/core"
```

#### Windows Command Prompt:

```cmd
REM Build the executable
docker build -f Dockerfile.build -t cdisc-builder .

REM Create container and get ID
docker create cdisc-builder > temp_id.txt
set /p CONTAINER_ID=<temp_id.txt

REM Extract the executable
docker cp %CONTAINER_ID%:/app/dist/output/core-ubuntu-22.04/core ./build-output/

REM Clean up
docker rm %CONTAINER_ID%
del temp_id.txt

echo Executable ready: ./build-output/core
```

#### Windows PowerShell:

```powershell
# Build the executable
docker build -f Dockerfile.build -t cdisc-builder .

# Extract the executable and remove container
$CONTAINER_ID = docker create cdisc-builder
docker cp "${CONTAINER_ID}:/app/dist/output/core-ubuntu-22.04/core" ./build-output/
docker rm $CONTAINER_ID

# Note: chmod is not needed on Windows
Write-Host "Executable ready: ./build-output/core"
```

### Alternative: Create build-output directory first

If you encounter issues with the docker cp command, create the output directory first:

#### Linux/macOS/WSL/Git Bash:

```bash
mkdir -p ./build-output
```

#### Windows Command Prompt:

```cmd
if not exist "build-output" mkdir build-output
```

#### Windows PowerShell:

```powershell
New-Item -ItemType Directory -Force -Path ./build-output
```

## Customizing the Build for Your Environment

The default Dockerfile builds for Ubuntu 22.04 on AMD64 architecture. To customize for your specific environment, modify these sections in Dockerfile.build:

### Change Target Operating System

- https://docs.docker.com/reference/dockerfile/#from
- **Windows**: https://hub.docker.com/r/microsoft/windows
- **macOS**: https://hub.docker.com/search - you can explore DockerHub to find a macOS image to utilize

```dockerfile
# Change the base image (line 2)
FROM --platform=linux/amd64 ubuntu:22.04
# Example:
# FROM mcr.microsoft.com/windows/servercore:ltsc2022  # Windows
```

### Update PyInstaller Output Path

If you change the base OS, update the PyInstaller dist path to match:

```dockerfile
# Change the --dist path in the pyinstaller command (around line 20)
--dist ./dist/output/core-ubuntu-22.04  # Current
# Examples:
# --dist ./dist/output/core-windows
# --dist ./dist/output/core-mac
```

### Update Docker Copy Command

Remember to update the corresponding path in your Docker copy command:

**Linux/macOS/WSL/Git Bash:**

```bash
# Update this path to match your PyInstaller dist path
docker cp $CONTAINER_ID:/app/dist/output/core-ubuntu-22.04/core ./build-output/
# Example for Windows build:
# docker cp $CONTAINER_ID:/app/dist/output/core-windows/core ./build-output/
```

**Windows Command Prompt:**

```cmd
REM Update this path to match your PyInstaller dist path
docker cp %CONTAINER_ID%:/app/dist/output/core-ubuntu-22.04/core ./build-output/
REM Example for Windows build:
REM docker cp %CONTAINER_ID%:/app/dist/output/core-windows/core ./build-output/
```

**Windows PowerShell:**

```powershell
# Update this path to match your PyInstaller dist path
docker cp "${CONTAINER_ID}:/app/dist/output/core-ubuntu-22.04/core" ./build-output/
# Example for Windows build:
# docker cp "${CONTAINER_ID}:/app/dist/output/core-windows/core" ./build-output/
```

## Platform-Specific Notes

### Windows Users

- **Recommended**: Use WSL (Windows Subsystem for Linux) or Git Bash for the best experience with the bash commands
- The `chmod +x` command is not needed on Windows as executable permissions work differently
- If using Command Prompt, some syntax differs from bash (variable assignment, echo commands)

### Linux/macOS Users

- The bash commands should work directly in your terminal
- Make sure Docker Desktop is running before executing the commands
- The `chmod +x` step is required to make the executable runnable

### Cross-Platform Alternative

For the most consistent experience across all platforms, consider using the **GitHub Actions approach (Option 1)**, which handles platform differences automatically and doesn't require local Docker setup.
