import humps


class Feature:
    def __init__(
        self,
        name,
        type=None,
        description=None,
        primary=None,
        partition=None,
        online_type=None,
    ):
        self._name = name
        self._type = type
        self._description = description
        # TODO: imo should be managed by the backend
        self._primary = primary or False
        self._partition = partition or False
        self._online_type = online_type

    def to_dict(self):
        return {
            "name": self._name,
            "type": self._type,
            "description": self._description,
            "partition": self._partition,
            "primary": self._primary,
            "onlineType": self._online_type,
        }

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type
