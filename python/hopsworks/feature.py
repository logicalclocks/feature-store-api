class Feature:
    def __init__(self, name, type=None, description=None, primary=None, partition=None):
        self._name = name
        self._type = type
        self._description = description
        self._primary = primary
        self._partition = partition

    def to_dict(self):
        return {
            "name": self._name,
            "type": self._type,
            "description": self._description,
        }

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type
