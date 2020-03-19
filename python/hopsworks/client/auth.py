import requests


class BearerAuth(requests.auth.AuthBase):
    """Class to encapsulate a Bearer token."""

    def __init__(self, token):
        self._token = token

    def __call__(self, r):
        r.headers["Authorization"] = "Bearer " + self._token
        return r


class ApiKeyAuth(requests.auth.AuthBase):
    """Class to encapsulate an API key."""

    def __init__(self, token):
        self._token = token

    def __call__(self, r):
        r.headers["Authorization"] = "ApiKey " + self._token
        return r
