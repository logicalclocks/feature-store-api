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
    show_feature_list: bool,
    show_description: bool,
) -> Tuple[List[str], Optional[str], Optional[Table]]:
    fg_names = set([sk["feature_group"]["name"] for sk in fv_dict["serving_keys"]])
    entries = (
        fv_dict["name"],
        f"v{fv_dict['version']}",
        f"{fv_dict['id']}",
        ", ".join(fg_names),
    )
    description = None
    if show_description and fv_dict["description"] is not None:
        description = "  [bold]Description :[/bold]\n    " + fv_dict["description"]

    feature_table = None
    if show_feature_list:
        feature_table = build_training_feature_bullets(fv_dict)

    return entries, description, feature_table


def build_training_feature_bullets(fv_dict: Dict[str, Any]) -> Table:
    feature_table = Table(box=None, show_lines=False)
    feature_table.add_column("  Features :")
    feature_table.add_column("")
    feature_table.add_column("")

    for feature in fv_dict["serving_keys"]:
        feature_table.add_row(
            f"    * {feature.get('prefix', '') + feature['feature_name']}",
            feature["type"],
            "serving key",
            feature["feature_group"]["name"],
        )

    sk_names = [sk["feature_name"] for sk in fv_dict["serving_keys"]]

    for feature in fv_dict["features"]:
        if feature["name"] not in sk_names:
            feature_table.add_row(
                f"    * {feature['name']}",
                feature["type"],
                "",
                feature["feature_group"]["name"],
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
    serving_key_table.add_column("Type")
    serving_key_table.add_column("Required")
    serving_key_table.add_column("Feature Group")

    for serving_key in fview_obj.serving_keys:
        serving_key_table.add_row(
            serving_key.required_serving_key,
            serving_key.type,
            "required" if serving_key.required else "optional",
            serving_key.feature_group.name,
        )

    feature_table = Table(box=box.ASCII2, title="Features", title_justify="left")

    feature_table.add_column("Name")
    feature_table.add_column("Type")
    feature_table.add_column("Feature Group")
    feature_table.add_column("Join")
    feature_table.add_column("Extra")

    for feature in fview_obj.query.features:
        join = ""
        extra = ""
        feature_table.add_row(
            feature.name, feature.type, feature.feature_group.name, join, extra
        )

    return feature_table, serving_key_table


def build_and_print_info_fv_table(fview_obj: feature_view_mod.FeatureView) -> None:
    table = Table(show_header=True, box=box.ASCII2)
    table.add_column(fview_obj.name, justify="center", style="bold")
    table.add_row("Name", fview_obj.name)
    table.add_row("Version", f"v{fview_obj.version}")
    table.add_row("ID", f"{fview_obj.id}")
    table.add_row(
        "Parent Feature Groups",
        ", ".join([fg.name for fg in fview_obj.query.feature_groups]),
    )

    feature_table, serving_key_table = build_training_feature_table(fview_obj)

    verbose.get_rich_console().print(table, feature_table, serving_key_table)
