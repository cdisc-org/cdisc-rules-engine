from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from datetime import datetime, timedelta
import requests
import json

class RuleDataService:

    def __init__(self, config):
        self.client_id = config.getValue("RULES_API_CLIENT_ID")
        self.client_secret = config.getValue("RULES_API_CLIENT_SECRET")
        self.base_url = config.getValue("RULES_API_BASE_URL")
        self.token = ""
        self.expire_time = datetime.now()
        self.refresh_token = ""
        self.has_more_rules = True
        self.next_params = None
    
    def generate_token(self):
        client = BackendApplicationClient(client_id=self.client_id)
        oauth = OAuth2Session(client=client)
        token_data = oauth.fetch_token(token_url=f'{self.base_url}/oauth/token', client_id=self.client_id,
        client_secret=self.client_secret)
        self.token = token_data.get("access_token")
        expires_in = token_data.get("expires_in")
        self.expire_time = datetime.now() + timedelta(0, expires_in)

    def has_more_rules(self):
        return self.has_more_rules

    def get_rules(self, params = None):
        if not self.refresh_token or datetime.now() > self.expire_time:
            self.generate_token()
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}"
        }
        url = f'{self.base_url}/jsonapi/node/conformance_rule?filter[status]=1'
        if params:
            url = url + f"&{params}"
        data = requests.get(url, headers=headers)
        json_data = json.loads(data.text)
        rules = json_data["data"]
        next_link = json_data.get("links", {}).get("next", {}).get("href")
        if next_link:
            self.has_more_rules = True
            self.next_params = next_link.split("?")[-1]
        else:
            self.has_more_rules = False
        return rules

        
    def check_errors(self, data):
        if ("meta" in data) and "ommitted" in data.get("meta", {}):
            # Invalid token provided
            return True
        return False
    

