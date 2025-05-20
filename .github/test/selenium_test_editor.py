import os
import sys
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pyperclip
import json
 
# Get the Preview Deployment URL
RULE_EDITOR_URL = os.getenv("RULE_EDITOR_URL")
if not RULE_EDITOR_URL:
    print("RULE_EDITOR_URL is not set! Test failed.")
    sys.exit(1)

print(f"Running tests on: {RULE_EDITOR_URL}")

# Configure Chrome options
chrome_options = Options()
chrome_options.add_argument("--ignore-certificate-errors")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("--headless=new")  # Enable headless mode

# Initialize ChromeDriver
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)
wait = WebDriverWait(driver, 20)

try:
    print("Opening Rule Editor site...")
    driver.get(RULE_EDITOR_URL)

    print("Clicking the search field for rule search...")
    rule_search_field = wait.until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="mui-10"]'))
    )
    rule_search_field.click()
    rule_search_field.send_keys("CG0006")

    print("Waiting for search results...")
    search_result = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="rulesList"]/table/tbody/tr/td[1]')
        )
    )
    search_result.click()
    print("Successfully searched and clicked the rule")

    print("Checking test tab button...")
    test_tab_button = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="root"]/div/div[3]/div/div[1]/div/div/div/button[2]')
        )
    )
    test_tab_button.click()

    print("Checking upload dataset tab...")
    upload_dataset_tab = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="tabpanel-1"]/div[5]/div[1]/div[2]')
        )
    )
    upload_dataset_tab.click()

    print("Checking file upload input field...")
    file_input = wait.until(
        EC.presence_of_element_located(
            (
                By.XPATH,
                '//*[@id="tabpanel-1"]/div[5]/div[2]/div/div/div/div/label/input',
            )
        )
    )

    file_path = os.path.abspath(".github/test/unit-test-coreid-CG0006-negative 1.xlsx")
    print(f"Uploading file: {file_path}")
    file_input.send_keys(file_path)
    print("Error result shows up")
    error_result = wait.until(
        EC.visibility_of_element_located(
            (By.XPATH, '//*[@id="tabpanel-1"]/div[6]/div[1]/div[1]/span/div/span')
        )
    )
    error_result.click()
    # Click the copy-to-clipboard element
    copy_button = wait.until(
        EC.element_to_be_clickable((
            By.XPATH, '//*[@id="tabpanel-1"]/div[6]/div[2]/div/div/div/div/div[2]/div/div/div/span[1]/div/span[2]'
        ))
    )
    copy_button.click()

    # Give time for clipboard to update
    time.sleep(1)  # adjust if necessary

    # Get clipboard content
    copied_content = pyperclip.paste()

    # Expected content
    expected_json = {
    "DM": [
        {
        "executionStatus": "success",
        "dataset": "dm.xpt",
        "domain": "DM",
        "variables": [],
        "message": None,
        "errors": []
        }
    ],
    "FA": [
        {
        "executionStatus": "success",
        "dataset": "fa.xpt",
        "domain": "FA",
        "variables": [
            "$val_dy", "FADTC", "FADY", "RFSTDTC"
        ],
        "message": "FADY is not calculated correctly even though the date portion of FADTC is complete, the date portion of DM.RFSTDTC is a complete date, and FADY is not empty.",
        "errors": [
            {
            "value": {
                "FADTC": "2012-12-02", "$val_dy": 18, "FADY": 35, "RFSTDTC": "2012-11-15"
            },
            "dataset": "fa.xpt", "row": 1, "USUBJID": "CDISC002", "SEQ": 1
            },
            {
            "value": {
                "FADTC": "2013-10-12", "$val_dy": 5, "FADY": 3, "RFSTDTC": "2013-10-08"
            },
            "dataset": "fa.xpt", "row": 2, "USUBJID": "CDISC004", "SEQ": 2
            },
            {
            "value": {
                "FADTC": "2012-12-02", "$val_dy": -34, "FADY": -30, "RFSTDTC": "2013-01-05"
            },
            "dataset": "fa.xpt", "row": 4, "USUBJID": "CDISC007", "SEQ": 4
            },
            {
            "value": {
                "FADTC": "2014-12-02", "$val_dy": 206, "FADY": 230, "RFSTDTC": "2014-05-11"
            },
            "dataset": "fa.xpt", "row": 5, "USUBJID": "CDISC008", "SEQ": 5
            }
        ]
        }
    ],
    "IE": [
        {
        "executionStatus": "success",
        "dataset": "ie.xpt",
        "domain": "IE",
        "variables": [
            "$val_dy", "IEDTC", "IEDY", "RFSTDTC"
        ],
        "message": "IEDY is not calculated correctly even though the date portion of IEDTC is complete, the date portion of DM.RFSTDTC is a complete date, and IEDY is not empty.",
        "errors": [
            {
            "value": {
                "IEDTC": "2022-03-17", "IEDY": -4, "$val_dy": -3, "RFSTDTC": "2022-03-20"
            },
            "dataset": "ie.xpt", "row": 1, "USUBJID": "CDISC-TEST-001", "SEQ": 1
            }
        ]
        }
    ],
    "LB": [
        {
        "executionStatus": "success",
        "dataset": "lb.xpt",
        "domain": "LB",
        "variables": [
            "$val_dy", "LBDTC", "LBDY", "RFSTDTC"
        ],
        "message": "LBDY is not calculated correctly even though the date portion of LBDTC is complete, the date portion of DM.RFSTDTC is a complete date, and LBDY is not empty.",
        "errors": [
            {
            "value": {
                "LBDY": 2, "LBDTC": "2022-03-30", "$val_dy": 11, "RFSTDTC": "2022-03-20"
            },
            "dataset": "lb.xpt", "row": 1, "USUBJID": "CDISC-TEST-001", "SEQ": 1
            }
        ]
        }
    ]
    }

    # Compare contents
    try:
        copied_json = json.loads(copied_content)
        if copied_json == expected_json:
            print("Test Passed: Copied content matches expected JSON.")
        else:
            print("Test Failed: Copied content does NOT match expected JSON.")
    except json.JSONDecodeError:
        print("Test Failed: Copied content is not valid JSON.")

except Exception as e:
    print(f"Test Failed: {e}")
    sys.exit(1)

finally:
    driver.quit()
