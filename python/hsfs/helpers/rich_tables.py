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

from typing import Any, Dict, List, Optional, Tuple, Union

from hsfs import feature as feature_mod
from hsfs import feature_group as fg_mod
from hsfs import util
from hsfs.helpers import constants
from rich import box
from rich.table import Table


def build_list_feature_table(
    fg_obj: Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup, fg_mod.SpineGroup],
):
    feature_table = Table(show_header=True, header_style="bold", box=box.ASCII2)
    feature_table.add_column("Feature Name")
    if fg_obj.online_enabled:
        feature_table.add_column("Online Type")
        feature_table.add_column("Offline Type")
    else:
        feature_table.add_column("Type", justify="center")
    feature_table.add_column("Metadata", overflow="ellipsis", justify="center")
    feature_table.add_column("Description", overflow="ellipsis")

    for feature in fg_obj.features:
        entries = [feature.name]
        if fg_obj.online_enabled:
            entries.append(feature.online_type)
            entries.append(feature.type)
        else:
            entries.append(feature.type)

        if feature.primary:
            entries.append("Primary Key")
        elif feature.name == fg_obj.event_time:
            entries.append("Event Time")
        else:
            entries.append("")

        entries.append(feature.description)

        feature_table.add_row(*entries)

    return feature_table


def build_info_feature_group_table(
    fg_obj: Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup, fg_mod.SpineGroup],
    show_features: bool = True,
) -> Table:
    renderables = []
    table = Table(show_header=True, header_style="bold", box=box.ASCII2)

    if isinstance(fg_obj, fg_mod.ExternalFeatureGroup):
        table.add_column("External Feature Group")
    elif isinstance(fg_obj, fg_mod.SpineGroup):
        table.add_column("Spine Group")
    else:
        table.add_column("Feature Group")

    table.add_column(fg_obj.name, overflow="ellipsis")

    table.add_row("Version", f"v{fg_obj.version}")
    if fg_obj.description and fg_obj.description != "":
        table.add_row("Description", f"{fg_obj.description}")
    table.add_row("ID", f"{fg_obj.id}")
    table.add_row(
        "Serving",
        f"{'Online (Real-Time) 🟢' if fg_obj.online_enabled else 'Offline (Batch) 🔴'}",
    )
    table.add_row("Primary Key", "".join(fg_obj.primary_key))
    table.add_row(
        "Event-Time Column",
        fg_obj.event_time if fg_obj.event_time else "N/A",
    )
    if fg_obj.partition_key is not None and len(fg_obj.partition_key) > 0:
        table.add_row("Partition Key", "".join(fg_obj.partition_key))

    if fg_obj.expectation_suite:
        table.add_row(
            "Expectation Suite",
            f"{'🟢' if fg_obj.expectation_suite.run_validation else '🔴'}",
        )
        table.add_row("Ingestion", fg_obj.expectation_suite.validation_ingestion_policy)

    table.add_row(
        "Statistics",
        f"{'🟢 Enabled' if fg_obj.statistics_config.enabled else '🔴 Disabled'}",
    )
    table.add_row(
        "Table Format",
        fg_obj.time_travel_format if fg_obj.time_travel_format else "PARQUET",
    )
    renderables.append(table)
    if len(fg_obj.features) > 0 and show_features:
        renderables.append("\n[underline]Features :[underline]")
        renderables.append(build_list_feature_table(fg_obj))
    if fg_obj.id is None:
        renderables.append(
            "Start writing data to the `FeatureStore` with the `insert()` method to register your `FeatureGroup`."
        )
    else:
        renderables.append(
            f"You can also check out your [link={util.get_feature_group_url(feature_store_id=fg_obj._feature_store_id, feature_group_id=fg_obj.id)}]Feature Group page in the Hopsworks UI[/link] for more information."
        )

    return renderables


def make_table_feature_groups(show_header: bool = True) -> Table:
    table = Table(show_header=show_header, header_style="bold")

    table.add_column("Name")
    table.add_column("Version")
    table.add_column("ID")
    table.add_column("Type")
    table.add_column("Online")

    return table


def make_rich_text_fg(
    table: Table,
    fgroup: Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup, fg_mod.SpineGroup],
    show_feature_list: bool,
):
    fg_type = ""
    if isinstance(fgroup, fg_mod.SpineGroup):
        fg_type = constants.SHOW_FG_TYPE_MAPPING["spine"]
    elif isinstance(fgroup, fg_mod.ExternalFeatureGroup):
        fg_type = constants.SHOW_FG_TYPE_MAPPING["external"]
    else:
        fg_type = constants.SHOW_FG_TYPE_MAPPING["stream"]
    online_status = "🟢 Real-Time" if fgroup.online_enabled else "🔴 Batch"
    entry_list = [
        fgroup.name,
        f"v{fgroup.version}",
        f"{fgroup.id}",
        fg_type,
        online_status,
    ]

    table.add_row(
        *entry_list,
    )

    if show_feature_list:
        nested_entry = ["" for _ in range(len(entry_list))]
        nested_entry[0] = build_nested_feature_list_table(fgroup.features)
        table.add_row(*nested_entry)


def build_nested_feature_list_table(features: List[feature_mod.Feature]) -> Table:
    feature_table = Table(show_header=True, header_style="bold", box=box.ASCII2)
    feature_table.add_column("Features")
    feature_table.add_column("Type")

    for feature in features:
        feature_table.add_row(feature.name, feature.type)

    return feature_table


def make_rich_text_fg_alt(
    fgroup: Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup, fg_mod.SpineGroup],
    show_description: bool,
    show_feature_list: bool,
) -> Tuple[List[str], Optional[str], Optional[Table]]:
    if isinstance(fgroup, fg_mod.SpineGroup):
        fg_type = constants.SHOW_FG_TYPE_MAPPING["spine"]
    elif isinstance(fgroup, fg_mod.ExternalFeatureGroup):
        fg_type = constants.SHOW_FG_TYPE_MAPPING["external"]
    else:
        fg_type = constants.SHOW_FG_TYPE_MAPPING["stream"]
    online_status = "🟢 Real-Time" if fgroup.online_enabled else "🔴 Batch"
    entries = [
        fgroup.name,
        f"v{fgroup.version}",
        f"{fgroup.id}",
        fg_type,
        online_status,
    ]

    description = None
    if all(
        [show_description, fgroup.description is not None, len(fgroup.description) > 0]
    ):
        description = "  [bold]Description :[/bold]\n    " + fgroup.description

    feature_table = None
    if show_feature_list:
        feature_table = build_feature_bullets(fgroup)

    return entries, description, feature_table


def build_feature_bullets(
    fgroup: Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup, fg_mod.SpineGroup],
) -> Table:
    feature_table = Table(box=None, show_lines=False)
    feature_table.add_column("  Features :")
    feature_table.add_column("")
    feature_table.add_column("")
    for feature in fgroup.features:
        extra = ""
        if feature.primary and feature.partition:
            extra = "  (primary & partition key)"
        elif feature.primary:
            extra = "  (primary key)"
        elif feature.partition:
            extra = "  (partition key)"
        if fgroup.event_time and fgroup.event_time == feature.name:
            extra = "  (event-time)"
        feature_table.add_row(f"    * {feature.name}", feature.type, extra)

    return feature_table


def make_table_feature_views() -> Table:
    table = Table(show_header=True, header_style="bold")

    table.add_column("Name")
    table.add_column("Version")
    table.add_column("ID")
    table.add_column("Parent Feature Groups")

    return table


def make_rich_text_feature_view_row(
    fv_dict: Dict[str, Any],
    show_feature_list: bool,
) -> Tuple[List[str], Optional[Table]]:
    fg_names = [sk["feature_group"]["name"] for sk in fv_dict["serving_keys"]]
    entries = (
        fv_dict["name"],
        f"v{fv_dict['version']}",
        f"{fv_dict['id']}",
        ", ".join(fg_names),
    )

    feature_table = None
    if show_feature_list:
        feature_table = build_training_feature_bullets(fv_dict)

    return entries, feature_table


def build_training_feature_bullets(fv_dict: Dict[str, Any]) -> Table:
    feature_table = Table(box=None, show_lines=False)
    feature_table.add_column("  Features :")
    feature_table.add_column("")
    feature_table.add_column("")

    for feature in fv_dict["serving_keys"]:
        feature_table.add_row(
            f"    * {feature['prefix'] + feature['name']}",
            feature["type"],
            "serving key",
            feature["feature_group"]["name"],
        )

    sk_names = [sk["name"] for sk in fv_dict["serving_keys"]]

    for feature in fv_dict["features"]:
        if feature["name"] not in sk_names:
            feature_table.add_row(
                f"    * {feature['name']}",
                feature["type"],
                "",
                feature["feature_group"]["name"],
            )

    return feature_table
