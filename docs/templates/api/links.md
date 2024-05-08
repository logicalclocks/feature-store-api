# Provenance Links

Provenance Links are objects returned by methods such as [get_feature_groups_provenance](../storage_connector_api/#get_feature_groups_provenance), [get_storage_connector_provenance](../feature_group_api/#get_storage_connector_provenance), [get_parent_feature_group](../feature_group_api/#get_parent_feature_groups), [get_generated_feature_groups](../feature_group_api/#get_generated_feature_groups), [get_generated_feature_views](../feature_group_api/#get_generated_feature_views) [get_models_provenance](../feature_view_api/#get_models_provenance) and represent sections of the provenance graph, depending on the method invoked.

## Properties

{{links_properties}}

# Artifact

Artifacts objects are part of the provenance graph and contain a minimal set of information regarding the entities (feature groups, feature views) they represent.
The provenance graph contains Artifact objects when the underlying entities have been deleted or they are corrupted or they are not accessible by the user.

{{artifact_properties}}
