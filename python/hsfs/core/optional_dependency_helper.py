import importlib.util
import logging
import sys


_logger = logging.getLogger(__name__)


def is_package_installed_or_load(name: str) -> bool:
    if name in sys.modules:
        _logger.debug(f"{name!r} is already imported")
        return True
    elif (spec := importlib.util.find_spec(name)) is not None:
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        _logger.info(f"{name!r} has been imported via find_spec")
        return True
    else:
        _logger.debug(f"can't find {name!r} module, install it or add it to the path.")
        return False
