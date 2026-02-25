import os
import sys
import json
import time

from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import brotli

# Get the Preview Deployment URL
RULE_EDITOR_URL = os.getenv("RULE_EDITOR_URL")
if not RULE_EDITOR_URL:
    print("RULE_EDITOR_URL is not set! Test failed.")
    sys.exit(1)

print(f"Running tests on: {RULE_EDITOR_URL}")

chrome_options = Options()
chrome_options.add_argument("--ignore-certificate-errors")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-software-rasterizer")
chrome_options.add_argument("--disable-extensions")
seleniumwire_options = {
    "disable_encoding": False,
    "suppress_connection_errors": True,
    "request_storage": "memory",
    "request_storage_max_size": 100,
}

# Initialize driver using selenium-wire
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(
    service=service, options=chrome_options, seleniumwire_options=seleniumwire_options
)
wait = WebDriverWait(driver, 30)

username = os.getenv("RULE_EDITOR_USERNAME")
password = os.getenv("RULE_EDITOR_PASSWORD")

if not username or not password:
    print("RULE_EDITOR_USERNAME or RULE_EDITOR_PASSWORD is not set! Test failed.")
    sys.exit(1)

try:
    print("Opening Rule Editor site...")
    driver.get(RULE_EDITOR_URL)

    wait.until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="signInName"]'))
    )  # wait for the page to load

    print("Waiting for username field to be clickable...")
    username_field = wait.until(
        EC.visibility_of_element_located((By.XPATH, '//*[@id="signInName"]'))
    )

    username_field = wait.until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="signInName"]'))
    )
    print("Username field is clickable.")
    username_field.send_keys(username)
    print("Username entered.")

    print("Waiting for password field to be clickable...")
    password_field = wait.until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="password"]'))
    )
    wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="password"]')))
    print("Password field is clickable.")
    password_field.send_keys(password)
    print("Password entered.")

    sign_in_button = wait.until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="next"]'))
    )
    sign_in_button.click()
    print("Sign in button clicked.")

    time.sleep(20)  # wait for the login to complete

    # Wait until the value attribute of the element is "QA Testing"
    name_clear_button = wait.until(
        EC.visibility_of_element_located(
            (By.XPATH, '//*[@id="rulesList"]/table/thead/tr/th[2]/div[2]/div/button')
        )
    )

    name_clear_button = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="rulesList"]/table/thead/tr/th[2]/div[2]/div/button')
        )
    )
    name_clear_button.click()
    print("Login successful and user is on the correct page.")

    print("Searching for rule CG0006...")
    rule_search_field = wait.until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="mui-10"]'))
    )
    rule_search_field.click()
    rule_search_field.send_keys("CG0006")

    time.sleep(5)  # wait for the search results to load.

    search_result = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="rulesList"]/table/tbody/tr/td[1]')
        )
    )
    search_result.click()

    print("Rule selected: ", search_result.text)

    print("Switching to test tab...")
    test_tab_button = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="root"]/div/div[3]/div/div[1]/div/div/div/button[2]')
        )
    )

    test_tab_button.click()
    time.sleep(4)  # wait for the schema validation to complete
    print("Opening upload dataset tab...")
    upload_dataset_tab = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="tabpanel-1"]/div[5]/div[1]/div[2]')
        )
    )
    upload_dataset_tab.click()

    print("Uploading dataset file...")
    file_input = wait.until(
        EC.presence_of_element_located(
            (
                By.XPATH,
                '//*[@id="tabpanel-1"]/div[5]/div[2]/div/div/div/div/label/input',
            )
        )
    )
    file_path = os.path.abspath(".github/test/unit-test-coreid-CG0006-negative 1.xlsx")
    file_input.send_keys(file_path)

    print("Waiting for error result to appear...")
    error_result = WebDriverWait(driver, 30).until(
        EC.visibility_of_element_located(
            (By.XPATH, '//*[@id="tabpanel-1"]/div[6]/div[1]/div[1]/span/div/span')
        )
    )
    print("Error result displayed.")

    # Give a few seconds for the POST request to complete
    time.sleep(5)

    screenshot_path = "login_screenshot.png"
    driver.save_screenshot(screenshot_path)
    print(f"Screenshot saved to {screenshot_path}")

    # Find the rule execution API call
    rule_exec_response = None
    for request in driver.requests:
        if "/api/rules/execute" in request.url:
            if request.response:
                try:
                    raw_body = request.response.body
                    decompressed = brotli.decompress(raw_body).decode("utf-8")
                    rule_exec_response = json.loads(decompressed)
                    print("Captured and decoded response from /api/rules/execute")
                    break
                except Exception as e:
                    print("Error decoding response body:", e)

    # Expected content
    expected_json = {
        "DM": [
            {
                "executionStatus": "success",
                "dataset": "dm.xpt",
                "domain": "DM",
                "variables": [],
                "message": None,
                "errors": [],
            }
        ],
        "FA": [
            {
                "executionStatus": "issue_reported",
                "dataset": "fa.xpt",
                "domain": "FA",
                "variables": [
                    "$val_dy",
                    "FADY",
                    "FADTC",
                    "RFSTDTC",
                ],
                "message": (
                    "FADY is not correctly calculated even though the date portion of "
                    "FADTC is complete, the date portion of RFSTDTC is complete, and "
                    "FADY is not empty."
                ),
                "errors": [
                    {
                        "value": {
                            "$val_dy": 18,
                            "FADY": 35,
                            "RFSTDTC": "2012-11-15",
                            "FADTC": "2012-12-02",
                        },
                        "dataset": "fa.xpt",
                        "row": 1,
                        "USUBJID": "CDISC002",
                        "SEQ": 1,
                    },
                    {
                        "value": {
                            "$val_dy": 5,
                            "FADY": 3,
                            "RFSTDTC": "2013-10-08",
                            "FADTC": "2013-10-12",
                        },
                        "dataset": "fa.xpt",
                        "row": 2,
                        "USUBJID": "CDISC004",
                        "SEQ": 2,
                    },
                    {
                        "value": {
                            "$val_dy": -34,
                            "FADY": -30,
                            "RFSTDTC": "2013-01-05",
                            "FADTC": "2012-12-02",
                        },
                        "dataset": "fa.xpt",
                        "row": 4,
                        "USUBJID": "CDISC007",
                        "SEQ": 4,
                    },
                    {
                        "value": {
                            "$val_dy": 206,
                            "FADY": 230,
                            "RFSTDTC": "2014-05-11",
                            "FADTC": "2014-12-02",
                        },
                        "dataset": "fa.xpt",
                        "row": 5,
                        "USUBJID": "CDISC008",
                        "SEQ": 5,
                    },
                ],
            }
        ],
        "IE": [
            {
                "executionStatus": "issue_reported",
                "dataset": "ie.xpt",
                "domain": "IE",
                "variables": [
                    "$val_dy",
                    "IEDY",
                    "IEDTC",
                    "RFSTDTC",
                ],
                "message": (
                    "IEDY is not correctly calculated even though the date portion of "
                    "IEDTC is complete, the date portion of RFSTDTC is complete, and "
                    "IEDY is not empty."
                ),
                "errors": [
                    {
                        "value": {
                            "$val_dy": -3,
                            "IEDY": -4,
                            "RFSTDTC": "2022-03-20",
                            "IEDTC": "2022-03-17",
                        },
                        "dataset": "ie.xpt",
                        "row": 1,
                        "USUBJID": "CDISC-TEST-001",
                        "SEQ": 1,
                    }
                ],
            }
        ],
        "LB": [
            {
                "executionStatus": "issue_reported",
                "dataset": "lb.xpt",
                "domain": "LB",
                "variables": [
                    "$val_dy",
                    "LBDY",
                    "LBDTC",
                    "RFSTDTC",
                ],
                "message": (
                    "LBDY is not correctly calculated even though the date portion of "
                    "LBDTC is complete, the date portion of RFSTDTC is complete, and "
                    "LBDY is not empty."
                ),
                "errors": [
                    {
                        "value": {
                            "$val_dy": 11,
                            "RFSTDTC": "2022-03-20",
                            "LBDTC": "2022-03-30",
                            "LBDY": 2,
                        },
                        "dataset": "lb.xpt",
                        "row": 1,
                        "USUBJID": "CDISC-TEST-001",
                        "SEQ": 1,
                    }
                ],
            }
        ],
    }
    # Compare result
    if rule_exec_response == expected_json:
        print("Test Passed: API response matches expected JSON.")
    else:
        print("Test Failed: API response does NOT match expected JSON.")
        print("Received:")
        print(json.dumps(rule_exec_response, indent=2))
        sys.exit(1)


except Exception as e:
    print(f"Test Failed due to exception: {e}")
    screenshot_path = "login_screenshot.png"
    driver.save_screenshot(screenshot_path)
    print(f"Screenshot saved to {screenshot_path}")
    sys.exit(1)

finally:
    driver.quit()
