# Tags

The feature store enables users to attach tags to artifacts such as feature groups or training datasets. Tags are aditional metadata attached to your artifacts and thus they can be used for an enhance full text search (by default Hopsworks makes all metadata searchable, users can opt out for particular featurestores if they want to keep them private). Adding tags to an artifact provides users with a more dynamic metadata content that can be used for both storage as well as enhancing artifact discoverability.

A tag is a {<b>key</b>: <b>value</b>} association, providing additional information about the data, such as for example geographic origin. This is useful in an organization as it adds more context to your data making it easier to share and discover data and artifacts.

<b>Note</b>: Tagging is only available in the enterprise version.

## Tag Schemas
The first step is to define the schemas of tags that can later be attached. These schemas follow the [https://json-schema.org](https://json-schema.org) as reference. The schemas define legal jsons and these can be primitives such as integer, float(number), boolean, string, json objects or arrays. The schemas themselves are also defined as jsons.

A schema of a <b>primitive</b> type looks like:
```
{ "type" : "string" }
```

A schema of an <b>object</b> type looks like:
```
{
  "type" : "object",
  "properties" :
  {
    "field1": { "type" : "string" },
    "field2": { "type" : "integer" }
  }
}
```

Type object is the default type for schemas, so you can ommit it if you want to keep the schema short. The properties field defines a dictionary of field names and their types.

A schema of a <b>array</b> of strings type looks like:
```
{
  "type" : "array",
  "items" : { "type" : "string" }
}
```

The array and object types can be arbitrarily nested of course.

## Creating schemas
Schemas are defined at a cluster level, so they are available to all projects. They can only be defined by a user with admin rights.

![Attach tags using the UI](../assets/images/create_schemas.gif)

## Attach tags using the UI
Tags can be attached using the feature store UI or programmatically using the API. This API will be described in the rest of this notebook.

![Attach tags using the UI](../assets/images/attach_tags.gif)

## Search with Tags
Once tags are attached, the feature groups are now searchable also by their tags, both keys and values.

![Attach tags using the UI](../assets/images/search_by_tags.gif)

## API
Both feature groups and training datasets contain the following add, get, get_all, delete operations for working with tags.

### Feature Groups

#### Attach tags
<b>Note</b>: You can only attach one tag value for a tag name. By calling the add operation on the same tag multiple times, you perform an update operation.
If you require attaching multiple values to a tag, like maybe a sequence, consider changing the tag type to an array of the type you just defined.

{{fg_tag_add}}

#### Get tag by key
{{fg_tag_get}}

#### Get all tags
{{fg_tag_get_all}}

#### Delete tag
{{fg_tag_delete}}

### Training Datasets

#### Attach tags
<b>Note</b>: You can only attach one tag value for a tag name. By calling the add operation on the same tag multiple times, you perform an update operation.
If you require attaching multiple values to a tag, like maybe a sequence, consider changing the tag type to an array of the type you just defined.

{{td_tag_add}}

#### Get tag by key
{{td_tag_get}}

#### Get all tags
{{td_tag_get_all}}

#### Delete tag
{{td_tag_delete}}
