from hsfs import client


class QueryConstructorApi:
    def construct_query(self, query):
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "featurestores", "query"]
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "PUT", path_params, headers=headers, data=query.json()
        )
