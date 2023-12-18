#
#  Copyright 2021. Logical Clocks AB
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import humps
import json
import inspect
import ast

from hsfs import util
from hsfs.core import transformation_function_engine


class TransformationFunction:
    def __init__(
        self,
        featurestore_id,
        transformation_fn=None,
        version=None,
        name=None,
        source_code_content=None,
        builtin_source_code=None,
        output_type=None,
        id=None,
        type=None,
        items=None,
        count=None,
        href=None,
        **kwargs,
    ):
        self._id = id
        self._featurestore_id = featurestore_id
        self._version = version
        self._name = name
        self._transformation_fn = transformation_fn
        self._source_code_content = source_code_content

        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(
                self._featurestore_id
            )
        )

        # set up depending on user initialized
        if self._transformation_fn is not None:
            # type -> user init coming from user
            self._transformer_code = None
            self._extract_source_code()
            self._output_type = self._transformation_function_engine.infer_spark_type(
                output_type
            )
        elif builtin_source_code is not None:
            # user triggered to register built-in transformation function
            self._output_type = self._transformation_function_engine.infer_spark_type(
                output_type
            )
            self._source_code_content = json.dumps(
                {
                    "module_imports": "",
                    "transformer_code": builtin_source_code,
                }
            )
        else:
            # load backend response
            # load original source code
            self._output_type = self._transformation_function_engine.infer_spark_type(
                output_type
            )
            self._load_source_code(self._source_code_content)

        self._feature_group_feature_name = None
        self._feature_group_id = None

    def save(self):
        """Persist transformation function in backend.

        !!! example
            ```python
            # define function
            def plus_one(value):
                return value + 1

            # create transformation function
            plus_one_meta = fs.create_transformation_function(
                    transformation_function=plus_one,
                    output_type=int,
                    version=1
                )

            # persist transformation function in backend
            plus_one_meta.save()
            ```
        """
        self._transformation_function_engine.save(self)

    def delete(self):
        """Delete transformation function from backend.

        !!! example
            ```python
            # define function
            def plus_one(value):
                return value + 1

            # create transformation function
            plus_one_meta = fs.create_transformation_function(
                    transformation_function=plus_one,
                    output_type=int,
                    version=1
                )
            # persist transformation function in backend
            plus_one_meta.save()

            # retrieve transformation function
            plus_one_fn = fs.get_transformation_function(name="plus_one")

            # delete transformation function from backend
            plus_one_fn.delete()
            ```
        """
        self._transformation_function_engine.delete(self)

    def _extract_source_code(self):
        if not callable(self._transformation_fn):
            raise ValueError("transformer must be callable")

        self._name = self._transformation_fn.__name__

        transformer_code = inspect.getsource(self._transformation_fn)

        module_imports = self._get_module_imports(
            self._get_module_path(self._transformation_fn.__module__)
        )

        self._transformer_code = "\n".join(module_imports) + "\n" + transformer_code

        # initialise source code dict
        # add all imports from module
        # add original source code that will be used during offline transformations
        self._source_code_content = json.dumps(
            {
                "module_imports": "\n".join(module_imports),
                "transformer_code": transformer_code,
            }
        )

    @staticmethod
    def _get_module_path(module_name):
        def _get_module_path(module):
            return module.__file__

        module_path = {}
        exec(
            """import %s\nmodule_path["path"] = _get_module_path(%s)"""
            % (module_name, module_name)
        )
        return module_path["path"]

    @staticmethod
    def _get_module_imports(path):
        imports = []
        with open(path) as fh:
            root = ast.parse(fh.read(), path)

        for node in ast.iter_child_nodes(root):
            if isinstance(node, ast.Import):
                imported_module = False
            elif isinstance(node, ast.ImportFrom):
                imported_module = node.module
            else:
                continue

            for n in node.names:
                if imported_module:
                    import_line = "from " + imported_module + " import " + n.name
                elif n.asname:
                    import_line = "import " + n.name + " as " + n.asname
                else:
                    import_line = "import " + n.name
                imports.append(import_line)
        return imports

    def _load_source_code(self, source_code_content):
        source_code_content = json.loads(source_code_content)
        module_imports = source_code_content["module_imports"]
        transformer_code = source_code_content["transformer_code"]
        self._transformer_code = module_imports + "\n" * 2 + transformer_code

        scope = __import__("__main__").__dict__
        exec(self._transformer_code, scope)
        self._transformation_fn = eval(self._name, scope)
        self._transformation_fn._code = self._transformer_code

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**tffn_dto) for tffn_dto in json_decamelized["items"]]
        else:
            return cls(**json_decamelized)

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**json_decamelized)
        return self

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "id": self._id,
            "name": self._name,
            "version": self._version,
            "sourceCodeContent": self._source_code_content,
            "outputType": self._output_type,
            "featurestoreId": self._featurestore_id,
        }

    @property
    def id(self):
        """Training dataset id."""
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @property
    def name(self):
        return self._name

    @property
    def version(self):
        return self._version

    @property
    def transformer_code(self):
        return self._transformer_code

    @property
    def transformation_fn(self):
        return self._transformation_fn

    @property
    def source_code_content(self):
        return self._source_code_content

    @property
    def output_type(self):
        return self._output_type

    @name.setter
    def name(self, name):
        self._name = name

    @version.setter
    def version(self, version):
        self._version = version

    @transformer_code.setter
    def transformer_code(self, transformer_code):
        self._transformer_code = transformer_code

    @transformation_fn.setter
    def transformation_fn(self, transformation_fn):
        self._transformation_fn = transformation_fn

    @source_code_content.setter
    def source_code_content(self, source_code_content):
        self._source_code_content = source_code_content

    @output_type.setter
    def output_type(self, output_type):
        self._output_type = output_type
