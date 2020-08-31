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

from hsfs.client import external, hopsworks

_client = None


def init(
    client_type,
    host=None,
    port=None,
    project=None,
    region_name=None,
    secrets_store=None,
    hostname_verification=None,
    trust_store_path=None,
    cert_folder=None,
    api_key_file=None,
    api_key_value=None,
):
    global _client
    if not _client:
        if client_type == "hopsworks":
            _client = hopsworks.Client()
        elif client_type == "external":
            _client = external.Client(
                host,
                port,
                project,
                region_name,
                secrets_store,
                hostname_verification,
                trust_store_path,
                cert_folder,
                api_key_file,
                api_key_value,
            )


def get_instance():
    global _client
    if _client:
        return _client
    raise Exception("Couldn't find client. Try reconnecting to Hopsworks.")


def stop():
    global _client
    _client._close()
    _client = None
