#
#   Copyright 2020 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

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
