#
#   Copyright 2022 Hopsworks AB
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

from io import BytesIO
import datetime
import json
import fastavro

from hsfs import feature_group
from hsfs.engine import python


class TestPythonWriter:
    def test_write_dataframe_kafka(self, mocker, dataframe_fixture_times):
        # Arrange
        mocker.patch("hsfs.engine.python.Engine._get_kafka_config", return_value={})
        avro_schema_mock = mocker.patch(
            "hsfs.feature_group.FeatureGroup._get_encoded_avro_schema"
        )
        avro_schema = (
            '{"type":"record","name":"test_fg","namespace":"test_featurestore.db","fields":'
            '[{"name":"primary_key","type":["null","long"]},{"name":"event_date","type":'
            '["null",{"type":"int","logicalType":"date"}]},{"name":"event_datetime_notz","type":'
            '["null",{"type":"long","logicalType":"timestamp-micros"}]},{"name":"event_datetime_utc",'
            '"type":["null",{"type":"long","logicalType":"timestamp-micros"}]},'
            '{"name":"event_datetime_utc_3","type":["null",{"type":"long","logicalType":'
            '"timestamp-micros"}]},{"name":"event_timestamp","type":["null",{"type":"long",'
            '"logicalType":"timestamp-micros"}]},{"name":"event_timestamp_pacific","type":'
            '["null",{"type":"long","logicalType":"timestamp-micros"}]},{"name":"state","type":'
            '["null","string"]},{"name":"measurement","type":["null","double"]}]}'
        )
        avro_schema_mock.side_effect = [avro_schema]
        mock_python_engine_kafka_produce = mocker.patch(
            "hsfs.engine.python.Engine._kafka_produce"
        )
        mocker.patch("hsfs.core.job_api.JobApi")  # get, launch
        mocker.patch("hsfs.engine.python.Engine.get_job_url")
        mocker.patch("hsfs.engine.python.Engine.wait_for_job")

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
        )

        # Act
        python_engine._write_dataframe_kafka(
            feature_group=fg,
            dataframe=dataframe_fixture_times,
            offline_write_options={"start_offline_backfill": True},
        )

        # Assert
        encoded_row = mock_python_engine_kafka_produce.call_args[0][3]
        print("Value" + str(encoded_row))
        parsed_schema = fastavro.parse_schema(json.loads(avro_schema))
        with BytesIO() as outf:
            outf.write(encoded_row)
            outf.seek(0)
            record = fastavro.schemaless_reader(outf, parsed_schema)

        reference_record = {
            "primary_key": 1,
            "event_date": datetime.date(2022, 7, 3),
            "event_datetime_notz": datetime.datetime(
                2022, 7, 3, 0, 0, tzinfo=datetime.timezone.utc
            ),
            "event_datetime_utc": datetime.datetime(
                2022, 7, 3, 0, 0, tzinfo=datetime.timezone.utc
            ),
            "event_datetime_utc_3": datetime.datetime(
                2022, 7, 2, 21, 0, tzinfo=datetime.timezone.utc
            ),
            "event_timestamp": datetime.datetime(
                2022, 7, 3, 0, 0, tzinfo=datetime.timezone.utc
            ),
            "event_timestamp_pacific": datetime.datetime(
                2022, 7, 3, 7, 0, tzinfo=datetime.timezone.utc
            ),
            "state": "nevada",
            "measurement": 12.4,
        }

        assert reference_record == record
