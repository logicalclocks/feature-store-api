from hopsworks import util


class Join:
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"
    LEFT_SEMI_JOIN = "LEFT_SEMI_JOIN"
    COMMA = "COMMA"

    def __init__(self, query, on, left_on, right_on, join_type):

        self._query = query
        self._on = util.parse_features(on)
        self._left_on = util.parse_features(left_on)
        self._right_on = util.parse_features(right_on)
        self._join_type = join_type or self.INNER
