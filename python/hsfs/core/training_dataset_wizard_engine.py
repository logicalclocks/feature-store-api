from hsfs.client.exceptions import FeatureStoreException
from hsfs.constructor.query import Query
from hsfs.core import training_dataset_wizard_api
from hsfs import engine


class TrainingDatasetWizardEngine:
    def __init__(self, feature_store_id):
        self._training_dataset_wizard_api = training_dataset_wizard_api.TrainingDatasetWizardApi(
            feature_store_id
        )

    def run_feature_selection(self, query: Query, num_features_to_select: int):
        df = query.read()
        # TODO: checkpoint data frame here (?)
        if engine.get_type() == "spark":
            selected_features =  engine.get_instance().feature_selection(df,
                                                                         self._label,
                                                                         num_features_to_select,
                                                                         True)

            # TODO: (optional) add a filter based on feature importance

            # create list of selected features and order by feature importance, descending
            selected_features_list = sorted(selected_features.keys(), key=lambda item: -1*selected_features[item])

            n_selected_features = self._select_from_query(query, selected_features_list)
            if n_selected_features != len(selected_features_list):
                raise FeatureStoreException(
                    f"Query contains only {n_selected_features} features, "
                    f"but {len(selected_features_list)} were expected."
                )
            return query
        else:
            # TODO: call feature selection in hopsworks backend
            #return self._training_dataset_wizard_api.feature_selection(self)
            raise Exception(
                f"`{engine.get_type()}` engine doesn't support this operation. "
                "Supported engine is `'spark'`."
            )

    def _select_from_query(self, query, selected_features_list, prefix=""):
        n_selected_features = 0
        selected_left_features = []
        for feature in query.left_features:
            if f"{prefix}{feature.name}" in selected_features_list:
                n_selected_features += 1
                selected_left_features.append(feature)
        query.left_features = selected_left_features

        selected_joins = []
        for join in query.joins[::-1]:
            n_selected_features_join = self._select_from_query(join.query, selected_features_list, join._prefix)
            # we discard joins without features, starting from the back, until we encounter the first non-empty join
            if n_selected_features_join > 0 or len(selected_joins) > 0:
                selected_joins.append(join)
                n_selected_features += n_selected_features_join
        query.joins = selected_joins

        return n_selected_features

