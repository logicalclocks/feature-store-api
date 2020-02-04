from hopsworks.engine import spark, hive

_engine = None


def init(engine_type, host=None, cert_folder=None, cert_key=None):
    global _engine
    if not _engine:
        if engine_type == "spark":
            _engine = spark.Engine()
        elif engine_type == "hive":
            _engine = hive.Engine(host, cert_folder, cert_key)


def get_instance():
    global _engine
    return _engine


def stop():
    global _engine
    _engine = None
