class QueryConstructorApi:
    def __init__(self, client):
        self._client = client

    def construct_query(self, query):
        path_params = ["project", self._client._project_id, "featurestores", "query"]
        headers = {"content-type": "application/json"}
        return self._client._send_request(
            "PUT", path_params, headers=headers, data=query.json()
        )
