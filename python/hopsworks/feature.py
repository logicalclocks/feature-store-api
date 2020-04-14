import humps


class Feature:
    def __init__(
        self, name, description=None, primary=None, partition=None, online_type=None
    ):
        self._name = name
        self._description = description
        self._primary = primary
        self._partition = partition
        self._online_type = online_type

    def to_dict(self):
        return {
            "name": self._name,
            "description": self._description,
            "partition": self._partition,
            "primary": self._primary,
        }

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        return cls(**json_decamelized)

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type
