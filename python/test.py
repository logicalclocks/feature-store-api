from python.hsfs.engine.python import Engine as pythonEngine
from python.hsfs.engine.spark import Engine as sparkEngine

p = pythonEngine()
s = sparkEngine()

p.register_external_temporary_table(None, None)
print(1)