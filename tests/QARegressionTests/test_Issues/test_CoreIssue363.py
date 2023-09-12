import unittest
import requests
import json
import os
import pytest


@pytest.mark.regression
class TestCoreIssue363(unittest.TestCase):
    def test_post_request(self):
        # URL to send the POST request to
        url = (
            "https://cdisc-library-conformance-rules-generator-dev.azurewebsites.net"
            "/api/TestRule?"
            "code=your-api-key"
        )

        # Load data from JSON file
        with open(
            os.path.join("tests", "resources", "CoreIssue363", "CG0022.json"), "r"
        ) as file:
            data = json.load(file)

        # Send the POST request
        response = requests.post(url, json=data)

        # Check the response status code
        self.assertEqual(
            response.status_code, 200, "POST request failed with status code"
        )

        # Retrieve the response data as a dictionary
        response_data = response.json()

        # Verify that the error is not present in the response
        self.assertNotIn(
            "error: An unknown exception has occurred",
            response_data,
            "Unexpected error in the response",
        )


if __name__ == "__main__":
    unittest.main()
