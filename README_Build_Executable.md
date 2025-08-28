# CDISC Rules Engine Docker Build Guide

This guide walks you through building the CDISC Rules Engine executable from scratch.

## Prerequisites

This guide presumes you have 2 things:

- an installation of git
- an installation of docker/ docker desktop as well as an account
- a current compatible version of python installed (currently 3.12)

## Setup Steps

### 1. Clone the Repository

```bash
# Clone the CDISC Rules Engine repository
git clone https://github.com/cdisc-org/cdisc-rules-engine.git
cd cdisc-rules-engine
```

### 2. Set up Python Virtual Environment (Optional but Recommended)

```bash
# Create virtual environment
python3.12 -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On Mac/Linux:
source venv/bin/activate

# Install dependencies (for local testing)
pip install -r requirements.txt
```

### 3. Update CDISC Library Cache

The CDISC Rules Engine uses cached CDISC standards. Update the cache before building:

```bash
python core.py update-cache
```

### 4. Run the Build Script

```bash
# Execute the build script
./build_for_colab.sh
```

The build process will:

- Create a Docker container with Ubuntu 22.04 (x86_64 architecture)
- Install Python 3.12 and all dependencies
- Build the executable using PyInstaller
- Package everything into a tarball ready for Google Colab

### 6. Verify the Build

Check that the executable was built for the correct architecture:

```bash
# Extract and check the executable
cd build-output/core-ubuntu-22.04/core
file core
```

You should see output like:

```
core: ELF 64-bit LSB executable, x86-64, version 1 (SYSV)...
```

If you see "ARM aarch64" instead of "x86-64", the build used the wrong architecture.

## Troubleshooting

## Alternative: GitHub Actions Build

If Docker setup is problematic, you can use GitHub Actions instead:

1. Fork the CDISC Rules Engine repository
2. Add this workflow file as `.github/workflows/build-colab.yml`:

```yaml
name: Build for Colab
on: workflow_dispatch

jobs:
  build:
    runs-on: ubuntu-22.04
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Cache standards
        run: python core.py cache-standards
      - name: Build executable
        run: |
          pip install --upgrade pip setuptools wheel twine
          pip install --no-deps -r requirements.txt
          pip install -r requirements.txt --no-cache-dir
          pyinstaller --onedir --contents-directory "." core.py \
            --dist ./dist/output/core-ubuntu-22.04 \
            --collect-submodules pyreadstat \
            --add-data="resources/cache:resources/cache" \
            --add-data="resources/templates:resources/templates" \
            --add-data="resources/schema:resources/schema" \
            --add-data="tests/resources/datasets:tests/resources/datasets"
          cd dist/output && tar -czf cdisc-core-colab.tar.gz core-ubuntu-22.04/
      - uses: actions/upload-artifact@v4
        with:
          name: cdisc-core-colab
          path: dist/output/cdisc-core-colab.tar.gz
```

3. Go to Actions tab in your fork and manually trigger the workflow
4. Download the artifact when complete

This approach builds on GitHub's x86_64 runners and provides the same result.
