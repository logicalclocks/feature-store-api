#
#   Copyright 2020 Logical Clocks AB
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

from hsfs import engine, util
from hsfs import feature_group as fg
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import feature_group_base_engine


class ExternalFeatureGroupEngine(feature_group_base_engine.FeatureGroupBaseEngine):
    def save(self, feature_group):
        if feature_group.features is None or len(feature_group.features) == 0:
            # If the user didn't specify the schema, parse it from the query
            external_dataset = engine.get_instance().register_external_temporary_table(
                feature_group, "read_ondmd"
            )
            # if python engine user should pass features as we do not parse it in this case
            if external_dataset is None:
                raise FeatureStoreException(
                    "Features (schema) need to be set for creation of external feature groups with engine "
                    + engine.get_type()
                    + ". Alternatively use Spark kernel."
                )

            feature_group._features = engine.get_instance().parse_schema_feature_group(
                external_dataset
            )

        # set primary and partition key columns
        # we should move this to the backend
        util.verify_attribute_key_names(feature_group, True)
        for feat in feature_group.features:
            if feat.name in feature_group.primary_key:
                feat.primary = True

        self._feature_group_api.save(feature_group)

    def insert(
        self,
        feature_group,
        feature_dataframe,
        write_options: dict,
        validation_options: dict = {},
    ):
        if not feature_group.online_enabled:
            raise FeatureStoreException(
                "Online storage is not enabled for this feature group. External feature groups can only store data in"
                + " online storage. To create an offline only external feature group, use the `save` method."
            )

        schema = engine.get_instance().parse_schema_feature_group(feature_dataframe)

        if not feature_group._id:
            # only save metadata if feature group does not exist
            feature_group.features = schema
            self.save(feature_group)
        else:
            # else, just verify that feature group schema matches user-provided dataframe
            self._verify_schema_compatibility(feature_group.features, schema)

        # ge validation on python and non stream feature groups on spark
        ge_report = feature_group._great_expectation_engine.validate(
            feature_group=feature_group,
            dataframe=feature_dataframe,
            validation_options=validation_options,
            ingestion_result="INGESTED",
            ge_type=False,
        )

        if ge_report is not None and ge_report.ingestion_result == "REJECTED":
            return None, ge_report

        return (
            engine.get_instance().save_dataframe(
                feature_group=feature_group,
                dataframe=feature_dataframe,
                operation=None,
                online_enabled=feature_group.online_enabled,
                storage="online",
                offline_write_options=write_options,
                online_write_options=write_options,
            ),
            ge_report,
        )

    def _update_features_metadata(self, feature_group, features):
        # perform changes on copy in case the update fails, so we don't leave
        # the user object in corrupted state
        fg_dict = feature_group.to_dict()
        copy_feature_group = fg.ExternalFeatureGroup.from_response_json(fg_dict)
        copy_feature_group.features = features
        self._feature_group_api.update_metadata(
            feature_group, copy_feature_group, "updateMetadata"
        )

    def update_features(self, feature_group, updated_features):
        """Updates features safely."""
        self._update_features_metadata(
            feature_group, self.new_feature_list(feature_group, updated_features)
        )

    def append_features(self, feature_group, new_features):
        """Appends features to a feature group."""
        self._update_features_metadata(
            feature_group,
            feature_group.features + new_features,  # todo allows for duplicates
        )

    def update_description(self, feature_group, description):
        """Updates the description of a feature group."""
        fg_dict = feature_group.to_dict()
        copy_feature_group = fg.ExternalFeatureGroup.from_response_json(fg_dict)
        copy_feature_group.description = description
        self._feature_group_api.update_metadata(
            feature_group, copy_feature_group, "updateMetadata"
        )

    def update_deprecated(self, feature_group, deprecate):
        """Updates the deprecation status of a feature group."""
        fg_dict = feature_group.to_dict()
        copy_feature_group = fg.ExternalFeatureGroup.from_response_json(fg_dict)
        self._feature_group_api.update_metadata(
            feature_group, copy_feature_group, "deprecate", deprecate
        )
