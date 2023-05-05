import json
import os
from core import validate
from core import list_rule_sets
from core import list_dataset_metadata
from core import test
from core import list_rules

import unittest
from click.testing import CliRunner


class TestCLI(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_validate_required_s_option_missing(self):
        result = self.runner.invoke(validate, ['-v', '3.4',
                                                '-dp', r"tests/resources/test_dataset.xpt"])
        self.assertEqual(result.exit_code, 2)
        self.assertIn("Error: Missing option '-s'", result.output)

    def test_validate_required_s_option_present(self):
        result = self.runner.invoke(validate, ['-s', 'sdtmig',
                                                '-v', '3.4', 
                                                '-dp', r"tests/resources/test_dataset.xpt"])
        print(result.output)
        self.assertEqual(result.exit_code, 0)

    def test_validate_required_v_option_missing(self):
        result = self.runner.invoke(validate, ['-s', 'sdtmig', 
                                                '-dp', r"tests/resources/test_dataset.xpt"])
        self.assertEqual(result.exit_code, 2)
        self.assertIn("Error: Missing option '-v'", result.output)

    def test_validate_required_v_option_present(self):
        result = self.runner.invoke(validate, ['-s', 'sdtmig',
                                                '-v', '3.4',
                                                '-dp', r"tests/resources/test_dataset.xpt"])
        self.assertEqual(result.exit_code, 0)
        
    def test_validate_both_d_and_data_options(self):
        result = self.runner.invoke(
            validate,
            [
                '-d', r"tests/resources/report_test_data",
                '--data', "tests/resources/report_test_data",
                '-s', 'sdtmig',
                '-v', '3.4',
            ]
        )
        self.assertNotEqual(result.exit_code, 0)
#         self.assertIn("Error: Invalid value", result.output)
#         self.assertIn("-d/--data", result.output)
#         self.assertIn("only one of the options can be provided", result.output)

    def test_validate_neither_d_nor_data_options(self):
        result = self.runner.invoke(
            validate,
            [
                '-s', 'sdtmig',
                '-v', '3.4',
            ]
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_with_all_required_options(self):
        result = self.runner.invoke(
            validate,
            [
                '-dp', r"tests/resources/test_dataset.xpt",
                '-s', 'sdtmig',
                '-v', '3.4',
            ]
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_without_all_required_options(self):
        result = self.runner.invoke(
            validate,
            [
                '-d', r"tests/resources/report_test_data",
            ]
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Error: Missing option", result.output)
        

    def test_validate_all_options(self):
        result = self.runner.invoke(
            validate,
            [
                "-ca",
                r"/resources/cache",
                "-ps",
                "20",
                "-d",
                r"tests/resources/report_test_data",
                "-dp",
                r"tests/resources/test_dataset.xpt",
                "-l",
                "debug",
                "-rt",
                r"resources/templates/report-template.xlsx",
                "-s",
                "sdtmig",
                "-v",
                "3.4",
                #"-ct",
                #"controlled_terminology",
                "-o",
                "result.json",
                "-of",
                "json",
                "-rr",
                "-dv",
                r"tests/resources/report_test_data/define.xml",
                "--whodrug",
                r"tests/resources/dictionaries/whodrug",
                "--meddra",
                r"tests/resources/dictionaries/meddra",
                "-r",
                r"tests/resources/rules.json",
                "-p",
                "bar",
            ],
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_minimum_options(self):
        result = self.runner.invoke(
            validate, ["-s", "sdtmig",
                        "-v", "3.4",
                        "-dp",r"tests/resources/test_dataset.xpt"])
        
        self.assertEqual(result.exit_code, 0)

    def test_validate_less_than_minimum_options(self):
        result = self.runner.invoke(validate, ["-s", "sdtmig"])
        self.assertEqual(result.exit_code, 2)
        self.assertIn("Error: Missing option", result.output)

    def test_validate_invalid_value_to_option_version(self):
        result = self.runner.invoke(
            validate, ["-s", "sdtmig",
                        "-v", "version",
                        "-dp",r"tests/resources/test_dataset.xpt", 
                        "-dv", "2.0"]
        )
        self.assertEqual(result.exit_code, 1)
        

    def test_validate_valid_options(self):
        result = self.runner.invoke(
            validate, ["-s", "sdtmig", 
                        "-v", "3.4",
                        "-dp",r"tests/resources/test_dataset.xpt",
                        "-dv", "/tests/resources/report_test_data/define.xml"])
        self.assertEqual(result.exit_code, 0)

    def test_validate_output_format_json(self):
        result = self.runner.invoke(
            validate, ["-s", "sdtmig", 
                        "-v", "3.4",
                        "-dp",r"tests/resources/test_dataset.xpt","-of", "json"])
        self.assertEqual(result.exit_code, 0)

    def test_validate_output_format_excel(self):
        result = self.runner.invoke(
            validate, ["-s", "sdtmig",
                        "-v", "3.4",
                        "-dp",r"tests/resources/test_dataset.xpt", 
                        "-of", "xlsx"])
        self.assertEqual(result.exit_code, 0)
        
    
    def test_validate_with_invalid_output_format(self):
        result = self.runner.invoke(validate, ["-s", "sdtmig",
        "-v", "3.4","-dp",r"tests/resources/test_dataset.xpt",
        "-o", "output.json", 
        "-of", "csv"])
        self.assertNotEqual(result.exit_code, 0)

    def test_validate_with_log_level_debug(self):
        result = self.runner.invoke(validate, ["-s", "sdtmig",
        "-v", "3.4",
        "-dp",r"tests/resources/test_dataset.xpt",
        "-l", "debug"])
        self.assertEqual(result.exit_code, 0)

    def test_validate_with_log_level_info(self):
        result = self.runner.invoke(validate, ["-s", "sdtmig",
        "-v", "3.4",
        "-dp",r"tests/resources/test_dataset.xpt",
        "-l", "info"])
        self.assertEqual(result.exit_code, 0)

    def test_validate_with_log_level_error(self):
        result = self.runner.invoke(validate, ["-s", "sdtmig",
        "-v", "3.4",
        "-dp",r"tests/resources/test_dataset.xpt",
        "-l", "error"])
        self.assertEqual(result.exit_code, 0)

    def test_validate_with_log_level_critical(self):
        result = self.runner.invoke(validate, ["-s", "sdtmig",
        "-v", "3.4",
        "-dp",r"tests/resources/test_dataset.xpt",
        "-l", "critical"])
        self.assertEqual(result.exit_code, 0)

    def test_validate_with_log_level_disabled(self):
        result = self.runner.invoke(validate, ["-s", "sdtmig",
        "-v", "3.4",
        "-dp",r"tests/resources/test_dataset.xpt",
        "-l", "disabled"])
        self.assertEqual(result.exit_code, 0)

    def test_validate_with_log_level_warn(self):
        result = self.runner.invoke(validate, ["-s", "sdtmig",
        "-v", "3.4",
        "-dp",r"tests/resources/test_dataset.xpt",
        "-l", "warn"])
        self.assertEqual(result.exit_code, 0)

    def test_validate_with_invalid_log_level(self):
        result = self.runner.invoke(validate, ["-s", "sdtmig",
        "-v", "3.4",
        "-dp",r"tests/resources/test_dataset.xpt",
        "-l", "invalid"])
        self.assertNotEqual(result.exit_code, 0)

    def test_validate_with_no_log_level(self):
        result = self.runner.invoke(validate, ["-s", "sdtmig", 
        "-v", "3.4",
        "-dp",r"tests/resources/test_dataset.xpt"])
        self.assertEqual(result.exit_code, 0)
        
    def test_validate_high_value_p(self):
        result=self.runner.invoke(validate,["-s","sdtmig",
        "-v","3.4","-dp",r"tests/resources/test_dataset.xpt",
        "-ps","10"])
        self.assertEqual(result.exit_code,0)
    
    def test_list_rule_sets_valid_cache_path(self):
        result = self.runner.invoke(list_rule_sets,["-c",r"resources/cache"])
        self.assertIn('SDTMIG, 3-2',result.output)
        self.assertIn('SDTMIG, 3-3',result.output)
        self.assertIn('SDTMIG, 3-4',result.output)
        self.assertIn('SENDIG, 3-1',result.output)
        

    def test_list_rule_sets_invalid_cache_path(self):
        result = self.runner.invoke(list_rule_sets,["--cache_path",
        r"resources"])
        self.assertNotEqual(result.exit_code,0)

    def test_list_rule_sets_no_cache_path(self):
        result = self.runner.invoke(list_rule_sets,[])
        self.assertIn('SDTMIG, 3-2',result.output)
        self.assertIn('SDTMIG, 3-3',result.output)
        self.assertIn('SDTMIG, 3-4',result.output)
        self.assertIn('SENDIG, 3-1',result.output)
        
    
    def test_list_dataset_metadata_with_valid_paths(self):
        result = self.runner.invoke(list_dataset_metadata, ["-dp",  
        r"tests/resources/test_dataset.xpt"])
        expected_output="""[
    {
        "domain": "EX",
        "filename": "test_dataset.xpt","""
        self.assertEqual(result.exit_code, 0)
        self.assertIn(expected_output,result.output)
        
    def test_list_dataset_metadata_with_invalid_paths(self):
        result = self.runner.invoke(list_dataset_metadata, ["-dp", 
        "invalid_path"])
        expected_output="""[
    {
        "domain": "EX",
        "filename": "tests//resources//test_dataset.xpt",
        "full_path": "tests//resources//test_dataset.xpt",
        "size": 823120,
        "label": "Exposure",
"""
        self.assertEqual(result.exit_code, 1)
        self.assertNotIn(expected_output,result.output)
        
    def test_list_dataset_metadata_with_no_paths(self):
        result = self.runner.invoke(list_dataset_metadata)
        expected_output="""Error: Missing option '-dp' / '--dataset-path'"""
        self.assertEqual(result.exit_code, 2)
        self.assertIn(expected_output,result.output)
        
    def test_test_command_with_all_options(self):
        result = self.runner.invoke(test, [
            "-c", r"resources/cache",
            "-dp", r"tests/resources/CG0027-positive.json",
            "-r", R"tests/resources/rules.json",
            "--whodrug",r"tests/resources/dictionaries/whodrug",
            "--meddra", r"tests/resources/dictionaries/meddra",
            "-s", "sdtmig",
            "-v", "3.4",
            #"-ct", "controlled_terminology_package",
            "-dv", r"tests/resources/report_test_data/define.xml"
        ])
        self.assertEqual(result.exit_code, 0)
        # Add more assertions here to check if the output is correct

    def test_test_command_without_dataset_path(self):
        result = self.runner.invoke(test, [
            "-c", r"/resources/cache",
            "-r", r"tests/resources/rules.json",
        ])
        self.assertNotEqual(result.exit_code, 0)

    def test_test_command_without_rule(self):
        result = self.runner.invoke(test, [
            "-c", r"/resources/cache",
            "-dp", r"tests/resources/CG0027-positive.json"
        ])
        self.assertNotEqual(result.exit_code, 0)

    def test_test_command_with_default_cache_path(self):
        result = self.runner.invoke(test, ["-s","sdtmig",
            "-v","3.4",
            "-dp", r"tests/resources/CG0027-positive.json",
            "-r", r"tests/resources/rules.json",
        ])
        self.assertEqual(result.exit_code, 0)
        # Add more assertions here to check if the output is correct

    def test_test_command_without_whodrug_and_meddra(self):
        result = self.runner.invoke(test, ["-s","sdtmig","-v","3.4",
            "-c", r"resources/cache",
            "-dp", r"tests/resources/CG0027-positive.json",
            "-r", r"tests/resources/rules.json",
        ])
        self.assertEqual(result.exit_code, 0)
        

    def test_test_command_with_invalid_whodrug_and_meddra(self):
        result = self.runner.invoke(test, [
           "-c", r"/resources/cache",
            "-dp", r"tests/resources/CG0027-positive.json",
            "-r", r"tests/resources/rules.json",
            "--whodrug", "invalid_path",
            "--meddra", "invalid_path",
        ])
        self.assertNotEqual(result.exit_code, 0)
        
    def test_list_rules_all_options_provided(self):
        result = self.runner.invoke(list_rules, ["-c", 
        r"resources/cache", "-s", "sdtmig", 
        "-v", "3.4"])
        self.assertEqual(result.exit_code, 0)

    def test_list_rules_standard_option_provided(self):
        result = self.runner.invoke(list_rules, ["-s", "sdtmig"])
        self.assertEqual(result.exit_code, 0)

    def test_list_rules_version_option_provided(self):
        result = self.runner.invoke(list_rules, ["-v", "3.4"])
        self.assertEqual(result.exit_code, 0)

    def test_list_rules_no_option_provided(self):
        result = self.runner.invoke(list_rules)
        self.assertEqual(result.exit_code, 0)

    def test_list_rules_required_options_provided(self):
        result = self.runner.invoke(list_rules, ["-s", "sdtmig", 
            "-v", "3.4"])
        self.assertEqual(result.exit_code, 0)

    def test_list_rules_output_format(self):
        result = self.runner.invoke(list_rules)
        output = json.loads(result.output)
        self.assertIsInstance(output, list)
        self.assertTrue(all(isinstance(rule, dict) for rule in output))

    def tearDown(self):
        # Delete any Excel or JSON files created during the test case, except for host.json
        for file_name in os.listdir('.'):
            if file_name != 'host.json' and (file_name.endswith('.xlsx') or file_name.endswith('.json')):
                os.remove(file_name)




if __name__ == '__main__':
    unittest.main()


