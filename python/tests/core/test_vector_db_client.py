#
#   Copyright 2023 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
import pytest
from hsfs.core import vector_db_client
from hsfs.feature_group import FeatureGroup
from hsfs.feature import Feature


class TestVectorDbClient:
    fg = FeatureGroup("test_fg", 1, 99, id=1)
    f1 = Feature("f1", feature_group=fg)
    f2 = Feature("f2", feature_group=fg)
    fg.features = [f1, f2]

    @pytest.fixture(autouse=True)
    def setup_mocks(self, mocker):
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch("hsfs.core.opensearch.OpenSearchClientSingleton._setup_opensearch_client")

        self.query = self.fg.select_all()
        self.target = vector_db_client.VectorDbClient(self.query)

    @pytest.mark.parametrize(
        "filter_expression, expected_result",
        [
            (lambda f1: None, []),
            (lambda f1: f1 > 10, [{"range": {"f1": {"gt": 10}}}]),
            (lambda f1: f1 < 10, [{"range": {"f1": {"lt": 10}}}]),
            (lambda f1: f1 >= 10, [{"range": {"f1": {"gte": 10}}}]),
            (lambda f1: f1 <= 10, [{"range": {"f1": {"lte": 10}}}]),
            (lambda f1: f1 == 10, [{"term": {"f1": 10}}]),
            (lambda f1: f1 != 10, [{"bool": {"must_not": [{"term": {"f1": 10}}]}}]),
            (lambda f1: f1.isin([10, 20, 30]), [{"terms": {"f1": "[10, 20, 30]"}}]),
            (lambda f1: f1.like("abc"), [{"wildcard": {"f1": {"value": "*abc*"}}}]),
        ],
    )
    def test_get_query_filter(self, filter_expression, expected_result):
        filter = filter_expression(self.f1)
        assert self.target._get_query_filter(filter) == expected_result

    @pytest.mark.parametrize(
        "filter_expression_nested, expected_result",
        [
            (
                lambda f1, f2: (f1 > 10) & (f2 < 20),
                [
                    {
                        "bool": {
                            "must": [
                                {"range": {"f1": {"gt": 10}}},
                                {"range": {"f2": {"lt": 20}}},
                            ]
                        }
                    }
                ],
            ),
            (
                lambda f1, f2: (f1 < 10) | (f2 > 20),
                [
                    {
                        "bool": {
                            "minimum_should_match": 1,
                            "should": [
                                {"range": {"f1": {"lt": 10}}},
                                {"range": {"f2": {"gt": 20}}},
                            ],
                        }
                    }
                ],
            ),
            (
                lambda f1, f2: ((f1 < 10) | (f1 > 30)) & ((f2 > 20) | (f2 < 10)),
                [
                    {
                        "bool": {
                            "must": [
                                {
                                    "bool": {
                                        "should": [
                                            {"range": {"f1": {"lt": 10}}},
                                            {"range": {"f1": {"gt": 30}}},
                                        ],
                                        "minimum_should_match": 1,
                                    }
                                },
                                {
                                    "bool": {
                                        "should": [
                                            {"range": {"f2": {"gt": 20}}},
                                            {"range": {"f2": {"lt": 10}}},
                                        ],
                                        "minimum_should_match": 1,
                                    }
                                },
                            ]
                        }
                    }
                ],
            ),
            (
                lambda f1, f2: ((f1 > 10) & (f2 < 20)) | ((f1 > 10) & (f2 < 20)),
                [
                    {
                        "bool": {
                            "minimum_should_match": 1,
                            "should": [
                                {
                                    "bool": {
                                        "must": [
                                            {"range": {"f1": {"gt": 10}}},
                                            {"range": {"f2": {"lt": 20}}},
                                        ]
                                    }
                                },
                                {
                                    "bool": {
                                        "must": [
                                            {"range": {"f1": {"gt": 10}}},
                                            {"range": {"f2": {"lt": 20}}},
                                        ]
                                    }
                                },
                            ],
                        }
                    }
                ],
            ),
            (
                lambda f1, f2: ((f1 > 10) & ((f2 < 20) | ((f1 > 30) & (f2 < 40)))),
                [
                    {
                        "bool": {
                            "must": [
                                {"range": {"f1": {"gt": 10}}},
                                {
                                    "bool": {
                                        "should": [
                                            {"range": {"f2": {"lt": 20}}},
                                            {
                                                "bool": {
                                                    "must": [
                                                        {"range": {"f1": {"gt": 30}}},
                                                        {"range": {"f2": {"lt": 40}}},
                                                    ]
                                                }
                                            },
                                        ],
                                        "minimum_should_match": 1,
                                    }
                                },
                            ]
                        }
                    }
                ],
            ),
        ],
    )
    def test_get_query_filter_logic(self, filter_expression_nested, expected_result):
        filter = filter_expression_nested(self.f1, self.f2)
        assert self.target._get_query_filter(filter) == expected_result
