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

from typing import Any, Dict, List, Optional, Tuple

from hsfs import feature_view as feature_view_mod
from hsfs.helpers import verbose
from rich import box
from rich.table import Table


def make_list_fv_table(
    show_headers: bool = True,
    show_features: bool = False,
    show_description: bool = False,
) -> Table:
    table = Table(show_header=show_headers, header_style="bold", box=box.ASCII2)

    table.add_column("Name")
    table.add_column("Version")
    table.add_column("ID")
    table.add_column("Parent Feature Groups")
    if show_description and not show_features:
        table.add_column("Description")

    return table


def make_rich_text_row(
    fv_dict: Dict[str, Any],
    show_features: bool,
    show_description: bool,
) -> Tuple[List[str], Optional[str], Optional[Table]]:
    fg_names = set([sk["feature_group"]["name"] for sk in fv_dict["serving_keys"]])
    entries = [
        fv_dict["name"],
        f"v{fv_dict['version']}",
        f"{fv_dict['id']}",
        ", ".join(fg_names),
    ]
    description = None
    if show_description and fv_dict["description"] is not None:
        if show_features:
            description = "  [bold]Description :[/bold]\n    " + fv_dict["description"]
        else:
            description = fv_dict["description"]

    feature_table = None
    if show_features:
        feature_table = build_training_feature_bullets(fv_dict)

    return entries, description, feature_table


def build_training_feature_bullets(fv_dict: Dict[str, Any]) -> Table:
    feature_table = Table(box=None, show_lines=False)
    feature_table.add_column("  Features :")
    feature_table.add_column("")
    feature_table.add_column("")

    for sk in fv_dict["serving_keys"]:
        feature_table.add_row(
            f"    * {sk.get('prefix', '') + sk['feature_name']}",
            sk.get("join_on", ""),
            "serving key",
            sk["feature_group"]["name"],
        )

    sk_names = [sk["feature_name"] for sk in fv_dict["serving_keys"]]

    for feature in fv_dict["features"]:
        if feature["name"] not in sk_names:
            feature_table.add_row(
                f"    * {feature['name']}",
                feature["type"],
                "",
                feature["featuregroup"]["name"],
            )

    return feature_table


def show_rich_table_feature_views(
    fview_dict_list: List[Dict[str, Any]],
    show_features: bool = False,
    show_description: bool = False,
) -> None:
    row_entries_and_opt_features_and_description = []

    for fview_dict in fview_dict_list:
        row_entries_and_opt_features_and_description.append(
            make_rich_text_row(
                fview_dict,
                show_features,
                show_description,
            )
        )
    if show_features:
        tables = []

        for (
            entries,
            description,
            feature_table,
        ) in row_entries_and_opt_features_and_description:
            new_table = make_list_fv_table(
                show_headers=False,
                show_features=show_features,
                show_description=show_description,
            )
            new_table.add_row(*entries)
            tables.extend(
                [
                    tab
                    for tab in [new_table, description, feature_table]
                    if tab is not None
                ]
            )

        verbose.get_rich_console().print(*tables)
    else:
        the_table = make_list_fv_table(
            show_headers=True,
            show_description=show_description,
            show_features=show_features,
        )

        for entries, description, _ in row_entries_and_opt_features_and_description:
            if show_description:
                entries.append(description or "")
            the_table.add_row(*entries)

        verbose.get_rich_console().print(the_table)


def build_training_feature_table(fview_obj: feature_view_mod.FeatureView) -> Table:
    serving_key_table = Table(
        box=box.ASCII2, title="Serving Keys", title_justify="left"
    )

    serving_key_table.add_column("Name")
    serving_key_table.add_column("Required")
    serving_key_table.add_column("JoinOn")
    serving_key_table.add_column("Feature Group")

    for serving_key in fview_obj.serving_keys:
        serving_key_table.add_row(
            serving_key.required_serving_key,
            "required" if serving_key.required else "optional",
            serving_key.join_on if serving_key.join_on else "N/A",
            serving_key.feature_group.name,
        )

    feature_table = Table(box=box.ASCII2, title="Features", title_justify="left")
    has_transformation = (
        fview_obj.transformation_functions
        and len(fview_obj.transformation_functions) > 0
    )
    feature_table.add_column("Name")
    feature_table.add_column("Type")
    feature_table.add_column("Metadata")
    if has_transformation:
        feature_table.add_column("Transformation Function")
    feature_table.add_column("Feature Group")

    for feature in sorted(fview_obj.features, key=lambda x: x.feature_group.name):
        metadata = ""
        if feature.inference_helper_column:
            metadata += "inference helper,"
        if feature.training_helper_column:
            metadata += "training helper,"
        if feature.label:
            metadata += "label,"

        if has_transformation:
            tf_name = ""
            opt_tf_obj = fview_obj.transformation_functions.get(feature.name, None)
            if opt_tf_obj:
                tf_name = opt_tf_obj.name
            entries = [
                feature.name,
                feature.type,
                metadata[:-1],
                tf_name,
                feature.feature_group.name,
            ]
        else:
            entries = [
                feature.name,
                feature.type,
                metadata[:-1],
                feature.feature_group.name,
            ]

        feature_table.add_row(*entries)

    return serving_key_table, feature_table


def build_and_print_info_fv_table(
    fview_obj: feature_view_mod.FeatureView, show_features: bool = False
) -> None:
    tables = []
    description = ""
    if fview_obj.description and len(fview_obj.description) > 0:
        description = f"Description: {fview_obj.description}"
    online_enabled = all([fg.online_enabled for fg in fview_obj.query.featuregroups])
    required_serving_keys = [
        sk.required_serving_key for sk in fview_obj.serving_keys if sk.required
    ]
    table = Table(
        show_header=True,
        header_style="bold",
        box=box.ASCII2,
        expand=False,
        caption=description,
    )
    table.add_column("Feature View")
    table.add_column(fview_obj.name)
    table.add_row("Version", f"v{fview_obj.version}")
    table.add_row("ID", f"{fview_obj.id}")
    table.add_row("Serving", "🟢 Real-Time" if online_enabled else "🔴 Batch")
    table.add_row("Required Serving Keys", ", ".join(required_serving_keys) or "N/A")
    table.add_row(
        "Parent Feature Groups",
        ",\n".join([fg.name for fg in fview_obj.query.featuregroups]),
    )
    tables.append(table)
    if show_features:
        tables.extend(build_training_feature_table(fview_obj))

    verbose.get_rich_console().print(*tables)
