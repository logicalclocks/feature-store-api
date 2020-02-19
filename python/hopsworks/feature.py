class Feature:
    def __init__(self, name, type=None, description=None, primary=None, partition=None):
        self._name = name
        self._type = type
        self._description = description
        self._primary = primary
        self._partition = partition
