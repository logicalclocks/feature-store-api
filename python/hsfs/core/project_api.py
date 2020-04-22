from hsfs import client


class ProjectApi:
    def get_client(self):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "client",
        ]
        return _client._send_request("GET", path_params, stream=True)
