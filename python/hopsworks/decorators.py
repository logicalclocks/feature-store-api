import functools


def not_connected(fn):
    @functools.wraps(fn)
    def if_not_connected(inst, *args, **kwargs):
        if inst._connected:
            raise HopsworksConnectionError
        return fn(inst, *args, **kwargs)

    return if_not_connected


def connected(fn):
    @functools.wraps(fn)
    def if_connected(inst, *args, **kwargs):
        if not inst._connected:
            raise NoHopsworksConnectionError
        return fn(inst, *args, **kwargs)

    return if_connected


class HopsworksConnectionError(Exception):
    """Thrown when attempted to change connection attributes while connected."""

    def __init__(self):
        super().__init__(
            "Connection is currently in use. Needs to be closed for modification."
        )


class NoHopsworksConnectionError(Exception):
    """Thrown when attempted to perform operation on connection while not connected."""

    def __init__(self):
        super().__init__(
            "Connection is not active. Needs to be connected for feature store operations."
        )
