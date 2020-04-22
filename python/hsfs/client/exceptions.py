class RestAPIError(Exception):
    """REST Exception encapsulating the response object and url."""

    def __init__(self, url, response):
        error_object = response.json()
        message = (
            "Metadata operation error: (url: {}). Server response: \n"
            "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user "
            "msg: {}".format(
                url,
                response.status_code,
                response.reason,
                error_object.get("errorCode", ""),
                error_object.get("errorMsg", ""),
                error_object.get("usrMsg", ""),
            )
        )
        super().__init__(message)
        self.url = url
        self.response = response


class UnknownSecretStorageError(Exception):
    """This exception will be raised if an unused secrets storage is passed as a parameter."""


class ExternalClientError(TypeError):
    """Raised when external client cannot be initialized due to missing arguments."""

    def __init__(self, missing_argument):
        message = (
            "{0} cannot be of type NoneType, {0} is a non-optional "
            "argument to connect to hopsworks from an external environment."
        ).format(missing_argument)
        super().__init__(message)
