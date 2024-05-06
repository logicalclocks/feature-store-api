#
#   Copyright 2024 Hopsworks AB
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
from unittest.mock import MagicMock

import pytest
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import vector_db_client
from hsfs.embedding import EmbeddingIndex
from hsfs.feature import Feature
from hsfs.feature_group import FeatureGroup


class TestVectorDbClient:
    embedding_index = EmbeddingIndex("2249__embedding_default_embedding")
    embedding_index.add_embedding("f2", 3)
    embedding_index._col_prefix = ""
    fg = FeatureGroup("test_fg", 1, 99, id=1, embedding_index=embedding_index)
    f1 = Feature("f1", feature_group=fg, primary=True, type="int")
    f2 = Feature("f2", feature_group=fg, primary=True, type="int")
    f3 = Feature("f3", feature_group=fg, type="int")
    fg.features = [f1, f2, f3]
    fg2 = FeatureGroup("test_fg", 1, 99, id=2)
    fg2.features = [f1, f2]

    @pytest.fixture(autouse=True)
    def setup_mocks(self, mocker):
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch(
            "hsfs.core.opensearch.OpenSearchClientSingleton._setup_opensearch_client"
        )

        self.query = self.fg.select_all()
        self.target = vector_db_client.VectorDbClient(self.query)
        self.target._opensearch_client = MagicMock()
        self.target._opensearch_client.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_index": "2249__embedding_default_embedding",
                        "_type": "_doc",
                        "_id": "2389|4",
                        "_score": 1.0,
                        "_source": {"f1": 4, "f2": [9, 4, 4]},
                    }
                ]
            }
        }

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

    def test_check_filter_when_filter_is_None(self):
        self.target._check_filter(None, self.fg2)

    def test_check_filter_when_filter_is_logic_with_wrong_feature_group(self):
        with pytest.raises(FeatureStoreException):
            self.target._check_filter((self.fg.f1 > 10) & (self.fg.f1 < 30), self.fg2)

    def test_check_filter_when_filter_is_logic_with_correct_feature_group(self):
        self.target._check_filter((self.fg.f1 > 10) & (self.fg.f1 < 30), self.fg)

    def test_check_filter_when_filter_is_filter_with_wrong_feature_group(self):
        with pytest.raises(FeatureStoreException):
            self.target._check_filter((self.fg.f1 < 30), self.fg2)

    def test_check_filter_when_filter_is_filter_with_correct_feature_group(self):
        self.target._check_filter((self.fg.f1 < 30), self.fg)

    def test_check_filter_when_filter_is_not_logic_or_filter(self):
        with pytest.raises(FeatureStoreException):
            self.target._check_filter("f1 > 20", self.fg2)

    def test_read_with_keys(self):
        actual = self.target.read(self.fg.id, self.fg.features, keys={"f1": 10, "f2": 20})

        expected_query = {
            "query": {"bool": {"must": [{"match": {"f1": 10}}, {"match": {"f2": 20}}]}},
            "_source": ["f1", "f2", "f3"],
        }
        self.target._opensearch_client.search.assert_called_once_with(
            body=expected_query, index="2249__embedding_default_embedding"
        )
        expected = [{"f1": 4, "f2": [9, 4, 4]}]
        assert actual == expected

    def test_read_with_pk(self):
        actual = self.target.read(self.fg.id, self.fg.features, pk="f1")

        expected_query = {
            "query": {"bool": {"must": {"exists": {"field": "f1"}}}},
            "size": 10,
            "_source": ["f1", "f2", "f3"],
        }
        self.target._opensearch_client.search.assert_called_once_with(
            body=expected_query, index="2249__embedding_default_embedding"
        )
        expected = [{"f1": 4, "f2": [9, 4, 4]}]
        assert actual == expected

    def test_read_without_pk_or_keys(self):
        with pytest.raises(FeatureStoreException):
            self.target.read(self.fg.id, self.fg.features)
