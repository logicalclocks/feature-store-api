from hsfs.client import aws, hopsworks

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
):
    global _client
    if not _client:
        if client_type == "hopsworks":
            _client = hopsworks.Client()
        elif client_type == "aws":
            _client = aws.Client(
                host,
                port,
                project,
                region_name,
                secrets_store,
                hostname_verification,
                trust_store_path,
                cert_folder,
                api_key_file,
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
