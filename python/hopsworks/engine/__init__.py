from hopsworks.engine import spark

_engine = None


def init(engine_type):
    global _engine
    if not _engine:
        if engine_type == "spark":
            _engine = spark.SparkEngine()


def get_instance():
    global _engine
    return _engine
