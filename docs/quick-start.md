# Quick Start

> **Need help?** See [FAQ & Troubleshooting](faq.md) or post in [GitHub Discussions](https://github.com/cdisc-org/cdisc-rules-engine/discussions/categories/q-a).

---

## Option 1: Pre-Built Executable

**Best for:** Users who want to run CORE without installing Python or managing dependencies.

### 1. Download

Download the latest executable for your operating system from the [Releases page](https://github.com/cdisc-org/cdisc-rules-engine/releases) and unzip the downloaded file.

### 2. Verify the Installation

Open a terminal in the unzipped directory and run:

**Windows (PowerShell):**

```powershell
.\core.exe --help
```

**Linux / Mac:**

```bash
# Make it executable (one-time setup)
chmod +x ./core

./core --help
```

> **Mac users:** If you see a security warning, remove the quarantine attribute first:
>
> ```bash
> xattr -rd com.apple.quarantine .
> ```

### 3. (Optional) Update the Cache

Executable releases ship with a pre-populated cache, so you can skip this step and go straight to validation. If you want the latest published rules, see [CLI Reference → update-cache](cli-reference.md#updating-the-cache-update-cache) for API key setup and options.

> **Note:** Rules published after a release may depend on engine features not present in that executable. When in doubt, wait for the next release rather than updating the cache manually.

### 4. Run a Validation

**Windows:**

```powershell
.\core.exe validate -s sdtmig -v 3-4 -d C:\path\to\datasets
```

**Linux / Mac:**

```bash
./core validate -s sdtmig -v 3-4 -d /path/to/datasets
```

### 5. (Optional) Run a Built-In Test

CORE ships with a self-test command to confirm the executable is working:

```bash
# Windows
.\core.exe test-validate json

# Linux/Mac
./core test-validate json
```

Test files are cleaned up automatically after completion.

---

## Option 2: From Source Code

**Best for:** Developers, contributors, or users who need the latest features.

### Prerequisites

- **Python 3.12** is required. Other versions are not supported.
  Check your version:

```bash
  python --version
```

Install Python 3.12 from [python.org](https://www.python.org/downloads/) if needed.

- **Git** is required to clone the repository.
  Check your version:

```bash
  git --version
```

Install Git from [git-scm.com](https://git-scm.com/downloads) if needed.

### 1. Clone the Repository

```bash
git clone https://github.com/cdisc-org/cdisc-rules-engine.git
cd cdisc-rules-engine
```

### 2. Create and Activate a Virtual Environment

**Linux / Mac:**

```bash
python -m venv venv
source venv/bin/activate
```

**Windows:**

```bash
python -m venv venv
.\venv\Scripts\Activate
```

> If you have multiple Python versions, use `python3.12 -m venv venv` explicitly.

### 3. Install Dependencies

```bash
python -m pip install -r requirements-dev.txt
```

### 4. Populate the Cache

```bash
python core.py update-cache
```

### 5. Run a Validation

```bash
python core.py validate -s sdtmig -v 3-4 -d /path/to/datasets
```

---

## Supported Dataset Formats

CORE supports the following input formats:

| Format     | Description                          |
| ---------- | ------------------------------------ |
| **XPT**    | SAS Transport Format (version 5)     |
| **JSON**   | Dataset-JSON ≥ v1.1 (CDISC standard) |
| **NDJSON** | Newline Delimited JSON               |
| **XLSX**   | Microsoft Excel                      |

> **Note:** Define-XML files must be provided via `--define-xml-path` (`-dxp`), not through the dataset directory.

---

## CLI Command Reference

All commands and flags are documented in the [CLI Reference](cli-reference.md).

Command summary:

| Command          | Purpose                                           |
| ---------------- | ------------------------------------------------- |
| `validate`       | Run conformance validation                        |
| `update-cache`   | Download/refresh rules, CT, and metadata          |
| `list-rules`     | List rules available in the cache                 |
| `list-rule-sets` | List standards and versions in the cache          |
| `list-ct`        | List controlled terminology packages in the cache |

> Throughout these docs, examples use `python core.py`. If you're using the executable, replace this with `.\core.exe` (Windows) or `./core` (Linux/Mac).
