# Tags

The feature store enables users to attach tags to artifacts, such as feature groups or training datasets. Tags are aditional metadata attached to your artifacts and thus they can be used for an enhanced full text search. Adding tags to an artifact provides users with a more dynamic metadata content that can be used for both storage as well as enhancing artifact discoverability.

<b>Note</b>: By default Hopsworks makes all metadata searchable, users can opt out for particular featurestores if they want to keep them private.

A tag is a {<b>key</b>: <b>value</b>} association, providing additional information about the data, such as for example geographic origin. This is useful in an organization as it adds more context to your data making it easier to share and discover data and artifacts.

<b>Note</b>: Tagging is only available in the enterprise version.

## Tag Schemas
The first step is to define the schemas of tags that can later be attached. These schemas follow the [https://json-schema.org](https://json-schema.org) as reference. The schemas define legal jsons and these can be primitives, objects or arrays. The schemas themselves are also defined as jsons.

Allowed primitive types are:

- string
- boolean
- integer
- number (float)

A tag of primitive type - string would look like:
```
{ "type" : "string" }
```
and this would allow a json value of:
```
string tag value
```

We can also define arbitrarily complex json schemas, such as:
```
{
  "type" : "object",
  "properties" :
  {
    "first_name" : { "type" : "string" },
    "last_name" : { "type" : "string" },
    "age" : { "type" : "integer" },
    "hobbies" : {
        "type" : "array",
        "items" : { "type" : "string" }
    }
  },
  "required" : ["first_name", "last_name", "age"],
  "additionalProperties": false
}
```
and a value that follows this schema would be:
```
{
  "first_name" : "John",
  "last_name" : "Doe",
  "age" : 27,
  "hobbies" : ["tennis", "reading"]
}
```

<b>Properties</b> section of a tag is a dictionary that defines field names and types.

Json schema are pretty lenient, all that the properties section tells us, is that if a field appears, it should be of the appropriate type. If the json object contains the field `first_name`, this field cannot be of type `boolean`, it has to be of type `string`. What we emphasize here, is that the properties section does not impose that fields declared are mandatory, or that the json object cannot contain other fields that were not defined in the schemas.

<b>Required</b> section enforces the mandatory fields. In our case above `first_name`, `last_name`, `age` are declared as mandatory, while `hobbies` is left as an optional field.

<b>Additional Properties</b> section enforces the strictness of the schema. If we set this to `false` the json objects of this schema can only use fields that are declared (mandatoriy or optional) by the schema. No undeclared fields will be allowed.

Type object is the default type for schemas, so you can ommit it if you want to keep the schema short.

### Advanced tag usage

We can use additional properties of schemas as defined by [https://json-schema.org](https://json-schema.org) to enhance our previous person schema:

- Add a `$schema` section to allow us to use more advanced features of the json schemas defined in later drafts. The default schema draft is 4 and we will use 7 here (latest).
- Add an `id` field that is of type string but has to follow a particular regex pattern. We will also make this field mandatory.
- Set some rules on `age`, for example age should be an Integer between 0 and 150.
- Add an `address` field that is itself an object.


```
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type" : "object",
  "properties" :
  {
    "id" : {
      "type" : "string",
      "pattern" : "^[A-Z]{2}[0-9]{4}$"
    },
    "first_name" : { "type" : "string" },
    "last_name" : { "type" : "string" },
    "age" : {
      "type" : "integer",
      "minimum" : 0,
      "maximum" : 150
    },
    "hobbies" : {
        "type" : "array",
        "items" : { "type" : "string" }
    },
    "address" : {
      "street" : { "type" : "string" },
      "city" : { "type" : "string" }
    }
  },
  "required" : ["id", "first_name", "last_name", "age"],
  "additionalProperties": false
}
```
and a valid value for this new schema would be:
```
{
  "id" : "AB1234",
  "first_name" : "John",
  "last_name" : "Doe",
  "age" : 27,
  "hobbies" : ["tennis", "reading"],
  "address" : {
    "street" : "Vasagatan nr. 12",
    "city" : "Stockholm"
  }
}
```

### Basic tag usage
Our new Feature Store UI is aimed to ease the general use of tags by users and we thus currently support only basic tags usage there. Basic tag schemas allow only one level depth fields. So types of fields are limited to primitives or array of primitives. Basic schemas also only allow the `required` and `additionalProperties` sections.

#### Creating schemas
Schemas are defined at a cluster level, so they are available to all projects. They can only be defined by a user with admin rights.

<p align="center">
  <img src="../../assets/images/create_schema_new.gif" width="600" alt="Create schema in new UI">
</p>

#### Attach tags using the UI
Tags can be attached using the feature store UI or programmatically using the API. This API will be described in the rest of this notebook.

<p align="center">
  <img src="../../assets/images/attach_tag_new.gif" width="600" alt="Attach tags in new UI">
</p>

#### Search with Tags
Once tags are attached, the feature groups are now searchable also by their tags, both keys and values.

<p align="center">
  <img src="../../assets/images/search_tag_new.gif" width="600" alt="Search tags in new UI">
</p>

### Advanced tag usage
Our old UI allows for the full capabilities of the json schemas as defined by [https://json-schema.org](https://json-schema.org). This includes allowing to define tags of primitive type as well as arbitrarily complex json objects.

#### Creating advanced schemas
If you want to create advanced schemas, you can do this in our old UI by providing the raw json value of the schema.

<p align="center">
  <img src="../../assets/images/create_schema.gif" width="600" alt="Create schema in old UI">
</p>

#### Attach advanced tag values using the UI
Our old UI also provides you with a way to attach tag values that follow these advanced semantics by providing their raw json values.

<p align="center">
  <img src="../../assets/images/attach_tag.gif" width="600" alt="Attach tags in old UI">
</p>

### Examples
You can try our tags example in notebooks populated by our `feature store tour` under notebooks `hsfs/tags`.

You can also check our example on [https://examples.hopsworks.ai/featurestore/hsfs/tags/feature_store_tags/](https://examples.hopsworks.ai/featurestore/hsfs/tags/feature_store_tags/).

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
