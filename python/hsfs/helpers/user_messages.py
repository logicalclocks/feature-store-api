#
#   Copyright 2024 HOPSWORKS AB
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
from __future__ import annotations

from hsfs import client
from hsfs import feature_store as fs_mod
from hsfs.helpers import constants, verbose
from rich import box
from rich.markdown import Markdown
from rich.panel import Panel


def print_connected_to_feature_store_message(fs_obj: fs_mod.FeatureStore):
    feature_groups = fs_obj._feature_group_api.get_all(feature_store_id=fs_obj.id)
    if len(feature_groups) == 0:
        get_started_message = Markdown(
            "- To learn how to get started with Hopsworks feature store, checkout our "
            "[guides and docs](https://docs.hopsworks.ai/latest/user_guides/fs/) "
            "or our [tutorials](https://github.com/logicalclocks/hopsworks-tutorials) on github.\n"
            "- Call `quicktour()` method to get an overview of the feature store API and capabilities.",
            justify="left",
            inline_code_lexer="python",
            inline_code_theme="one-dark",
        )
    else:
        get_started_message = Markdown(
            "- Call `show_feature_groups()` to show a list of existing Feature Groups to insert/upsert new data or "
            "set `with_features=True` to see which features you can select to build a new Feature View.\n"
            "- Call `show_feature_views()` to show a list of existing Feature Views, you can use them to read data "
            "and create Training Datasets. Feature Views composed of Features from online-enabled FeatureGroups can "
            "be used to serve feature value for real-time use cases. Checkout the ⚡ "
            "[benchmarks](https://www.hopsworks.ai/post/feature-store-benchmark-comparison-hopsworks-and-feast)\n"
            "- Call the `quicktour()` method to get an overview of the feature store API and capabilities.",
            justify="left",
            inline_code_lexer="python",
            inline_code_theme=constants.PYTHON_LEXER_THEME,
        )

    if verbose.is_hsfs_verbose() and verbose.is_rich_print_enabled():
        rich_console = verbose.get_rich_console()
        (
            rich_console.print(
                Panel.fit(
                    f"Connected to Project [bold red]{fs_obj.project_name}[/bold red] on [italic red]{client.get_instance()._host}[/italic red].",
                    title="Hopsworks Feature Store",
                    style="bold",
                    box=box.ASCII2,
                    padding=(1, 2),
                ),
                get_started_message,
                justify="center",
            ),
        )
    else:
        print(f"Connected to project {fs_obj.project_name} in Hopsworks Feature Store.")
