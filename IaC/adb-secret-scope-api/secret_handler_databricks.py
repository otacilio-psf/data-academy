import requests
import json


class DatabricksSecret():

    def __init__(self, databricks_url, databricks_token):
        self._end_point = f'{databricks_url}/api/2.0/'
        self._headers = {'Authorization': f'Bearer {databricks_token}'}

    def create_scope(self, scope_name):
        pay_load = json.dumps({
            "scope": scope_name,
            "initial_manage_principal": "users"
        })

        r = requests.post(self._end_point+'secrets/scopes/create', data=pay_load, headers=self._headers)
        response_json = r.json()
        print(json.dumps(response_json, indent=4, sort_keys=True))

    def delete_scope(self, scope_name):
        pay_load = json.dumps({
            "scope": scope_name
        })

        r = requests.post(self._end_point+'secrets/scopes/delete', data=pay_load, headers=self._headers)
        response_json = r.json()
        print(json.dumps(response_json, indent=4, sort_keys=True))

    def list_scope(self):
        r = requests.get(self._end_point+'secrets/scopes/list', headers=self._headers)
        response_json = r.json()
        print(json.dumps(response_json, indent=4, sort_keys=True))


    def create_secret(self, scope_name, secret_name, secret_value):
        pay_load = json.dumps({
            "scope": scope_name,
            "key": secret_name,
            "string_value": secret_value
        })

        r = requests.post(self._end_point+'secrets/put', data=pay_load, headers=self._headers)
        response_json = r.json()
        print(json.dumps(response_json, indent=4, sort_keys=True))

    def delete_secret(self, scope_name, secret_name):
        pay_load = json.dumps({
            "scope": scope_name,
            "key": secret_name
        })

        r = requests.post(self._end_point+'secrets/delete', data=pay_load, headers=self._headers)
        response_json = r.json()
        print(json.dumps(response_json, indent=4, sort_keys=True))

    def list_secret(self, scope_name):
        pay_load = json.dumps({
            "scope": scope_name
        })

        r = requests.get(self._end_point+'secrets/list', data=pay_load, headers=self._headers)
        response_json = r.json()
        print(json.dumps(response_json, indent=4, sort_keys=True))
