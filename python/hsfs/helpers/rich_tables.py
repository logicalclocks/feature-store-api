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

from typing import Union

from hsfs import feature_group as fg_mod
from hsfs import feature_view as fv_mod
from hsfs import util
from hsfs.helpers import constants, verbose
from rich import box
from rich.table import Table


def make_feature_group_feature_table(
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


def make_base_info_feature_group_table(
    fg_obj: Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup, fg_mod.SpineGroup],
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

    if len(fg_obj.features) > 0:
        renderables.append(make_feature_group_feature_table(fg_obj))
    if fg_obj.id is None:
        renderables.append(
            "Start writing data to the `FeatureStore` with the `insert()` method to register your `FeatureGroup`."
        )
    else:
        renderables.append(
            f"You can also check out your [link={util.get_feature_group_url(feature_store_id=fg_obj._feature_store_id, feature_group_id=fg_obj.id)}]Feature Group page in the Hopsworks UI[/link] for more information."
        )

    rich_console = verbose.get_rich_console()
    rich_console.print(*renderables)


def make_table_feature_groups() -> Table:
    table = Table(show_header=True, header_style="bold")

    table.add_column("Name")
    table.add_column("Version")
    table.add_column("ID")
    table.add_column("Type")
    table.add_column("Real-Time")

    return table


def make_rich_text_feature_group_row(
    table: Table,
    fgroup: Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup, fg_mod.SpineGroup],
    show_feature_list: bool,
) -> str:
    fg_type = ""
    if isinstance(fgroup, fg_mod.SpineGroup):
        fg_type = constants.SHOW_FG_TYPE_MAPPING["spine"]
    elif isinstance(fgroup, fg_mod.ExternalFeatureGroup):
        fg_type = constants.SHOW_FG_TYPE_MAPPING["external"]
    else:
        fg_type = constants.SHOW_FG_TYPE_MAPPING["stream"]
    online_status = "🟢 Online" if fgroup.online_enabled else "🔴 Offline"
    entry_list = [
        f"{fgroup.name}",
        f"v{fgroup.version}",
        f"{fgroup.id}",
        fg_type,
        online_status,
    ]

    table.add_row(
        *entry_list,
    )

    if show_feature_list:
        table.add_row("", "- Columns:", "", "", "", "")
        for feature in fgroup.features:
            table.add_row("", "*", f"{feature.name} :", f"{feature.type}")

    return table


def make_table_feature_views():
    table = Table(show_header=True, header_style="bold")

    table.add_column("Name")
    table.add_column("Version")
    table.add_column("ID")
    table.add_column("Real-Time")

    return table


def make_rich_text_feature_view_row(
    table: Table,
    fv_obj: fv_mod.FeatureView,
    show_feature_list: bool,
) -> str:
    online_status = (
        "🟢 Online"
        if all(fv_obj.query.featuregroups.map(lambda x: x.online_enabled))
        else "🔴 Offline"
    )
    entry_list = [
        f"{fv_obj.name}",
        f"v{fv_obj.version}",
        f"{fv_obj.id}",
        online_status,
    ]

    table.add_row(
        *entry_list,
    )

    if show_feature_list:
        table.add_row("", "- Columns:", "", "")
        for feature in fv_obj.features:
            table.add_row("", "*", f"{feature.name} :", f"{feature.type}")
