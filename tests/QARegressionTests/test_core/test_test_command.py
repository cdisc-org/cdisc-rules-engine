import os
import subprocess
import unittest


class TestTestCommand(unittest.TestCase):
    def setUp(self):
        self.error_keyword = "error"

    def run_command(self, command):
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

    def test_test_command_with_all_options(self):
        command = (
            f"python core.py test "
            f"-c {os.path.join('resources', 'cache')} "
            f"-dp {os.path.join('tests', 'resources', 'CoreIssue271', 'vlm-check-dataset.json')} "
            f"-r {os.path.join('tests', 'resources', 'CoreIssue271', 'vlm-check-variable-length.json')} "
            f"--whodrug {os.path.join('tests', 'resources', 'dictionaries', 'whodrug')} "
            f"--meddra {os.path.join('tests', 'resources', 'dictionaries', 'meddra')} "
            f"-s sdtmig "
            f"-v 3.4 "
            f"-dv 2.1 "
            f"-dxp {os.path.join('tests', 'resources', 'report_test_data', 'define.xml')}"
        )
        exit_code, stdout, stderr = self.run_command(command)
        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertEqual(stderr, "", f"Error while executing command:\n{stderr}")

    def test_test_command_without_dataset_path(self):
        command = (
            f"python core.py test "
            f"-c {os.path.join('resources', 'cache')} "
            f"-r {os.path.join('tests', 'resources', 'Rule-CG0027.json')}"
        )
        exit_code, stdout, stderr = self.run_command(command)
        self.assertNotEqual(exit_code, 0)
        self.assertNotEqual(
            stderr, "", f"Error not raised while executing invalid command:\n{stderr}"
        )

    def test_test_command_without_rule(self):
        command = (
            f"python core.py test "
            f"-c {os.path.join('resources', 'cache')} "
            f"-dp {os.path.join('tests', 'resources', 'CG0027-positive.json')}"
        )
        exit_code, stdout, stderr = self.run_command(command)
        self.assertNotEqual(exit_code, 0)
        self.assertNotEqual(
            stderr, "", f"Error not raised while executing invalid command:\n{stderr}"
        )

    def test_test_command_with_default_cache_path(self):
        command = (
            f"python core.py test "
            f"-s sdtmig "
            f"-v 3.4 "
            f"-dp {os.path.join('tests', 'resources', 'CG0027-positive.json')} "
            f"-r {os.path.join('tests', 'resources', 'Rule-CG0027.json')}"
        )
        exit_code, stdout, stderr = self.run_command(command)
        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertEqual(stderr, "", f"Error while executing command:\n{stderr}")

    def test_test_command_without_whodrug_and_meddra(self):
        command = (
            f"python core.py test "
            f"-s sdtmig "
            f"-v 3.4 "
            f"-c {os.path.join('resources', 'cache')} "
            f"-dp {os.path.join('tests', 'resources', 'CG0027-positive.json')} "
            f"-r {os.path.join('tests', 'resources', 'Rule-CG0027.json')}"
        )
        exit_code, stdout, stderr = self.run_command(command)
        self.assertEqual(exit_code, 0)
        self.assertFalse(self.error_keyword in stdout)
        self.assertEqual(stderr, "", f"Error while executing command:\n{stderr}")

    def test_test_command_with_invalid_whodrug_and_meddra(self):
        command = (
            f"python core.py test "
            f"-c {os.path.join('resources', 'cache')} "
            f"-dp {os.path.join('tests', 'resources', 'CG0027-positive.json')} "
            f"-r {os.path.join('tests', 'resources', 'Rule-CG0027.json')} "
            f"--whodrug invalid_path "
            f"--meddra invalid_path"
        )
        exit_code, stdout, stderr = self.run_command(command)
        self.assertNotEqual(exit_code, 0)
        self.assertNotEqual(stderr, "", f"Error while executing command:\n{stderr}")

    def tearDown(self):
        for file_name in os.listdir("."):
            if file_name != "host.json" and (
                file_name.endswith(".xlsx") or file_name.endswith(".json")
            ):
                os.remove(file_name)


if __name__ == "__main__":
    unittest.main()
