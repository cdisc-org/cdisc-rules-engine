import subprocess
import os
import pytest


def test_bundled_pyreadstat():
    exe_path = r"C:\Users\SamJohnson\Documents\workspaces\release\v7.1\core.exe"
    # test_paths = [
    #     'dist/output/windows/core/core.exe',
    #     'dist/output/mac/core/core',
    #     'dist/output/ubuntu-latest/core/core',
    #     'dist/output/ubuntu-20-04/core/core'
    # ]
    # exe_path = next((path for path in test_paths if os.path.exists(path)), None)
    if not exe_path:
        pytest.skip("No executable found")
    if not exe_path.endswith(".exe"):
        os.chmod(exe_path, 0o755)
    help_cmd = [exe_path, "--help"]
    help_result = subprocess.run(help_cmd, capture_output=True, text=True)
    print(f"Available commands: {help_result.stdout}")
    cmd = [
        exe_path,
        "python",
        "-c",
        """
        import pyreadstat
        import tempfile
        import os
        temp_path = tempfile.mktemp(suffix=".sas7bdat")
        data = [[1, 2], [3, 4]]
        var_names = ["var1", "var2"]
        pyreadstat.write_sas7bdat(temp_path, data, var_names=var_names)
        df, meta = pyreadstat.read_sas7bdat(temp_path)
        os.unlink(temp_path)
        """,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"pyreadstat import failed: {result.stderr}"
