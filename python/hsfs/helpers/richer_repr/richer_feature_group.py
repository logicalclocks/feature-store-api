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

from hsfs import feature_group as fg_mod
from hsfs import util
from hsfs.helpers import constants, verbose
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


def build_and_print_info_fg_table(
    fg_obj: Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup, fg_mod.SpineGroup],
    show_features: bool = True,
) -> None:
    renderables = []
    description = None
    if fg_obj.description and fg_obj.description != "":
        description = f"[bold]Description :[/bold] {fg_obj.description}"
    table = Table(
        show_header=True, header_style="bold", box=box.ASCII2, caption=description
    )

    if isinstance(fg_obj, fg_mod.ExternalFeatureGroup):
        table.add_column("External Feature Group")
    elif isinstance(fg_obj, fg_mod.SpineGroup):
        table.add_column("Spine Group")
    else:
        table.add_column("Feature Group")

    table.add_column(fg_obj.name, overflow="ellipsis")

    table.add_row("Version", f"v{fg_obj.version}")
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
    verbose.get_rich_console().print(*renderables)


def make_table_fg_list(
    show_header: bool = True,
    show_features: bool = False,
    show_description: bool = False,
) -> Table:
    table = Table(show_header=show_header, header_style="bold", box=box.ASCII2)
    table.add_column("Name")
    table.add_column("Version")
    table.add_column("ID")
    table.add_column("Type")
    table.add_column("Online")
    if show_description and not show_features:
        table.add_column("Description")

    return table


def make_rich_text_row(
    fgroup: Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup, fg_mod.SpineGroup],
    show_description: bool,
    show_features: bool,
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
        if show_features:
            description = "  [bold]Description :[/bold]\n    " + fgroup.description
        else:
            description = fgroup.description

    feature_table = None
    if show_features:
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


def show_rich_table_feature_groups(
    fgroup_list: List[Dict[str, Any]],
    show_features: bool = False,
    show_description: bool = False,
) -> None:
    rich_console = verbose.get_rich_console()
    row_entries_and_opt_features_and_description = [
        make_rich_text_row(
            fgroup_obj,
            show_description,
            show_features,
        )
        for fgroup_obj in fgroup_list
    ]

    if show_features:
        tables = []
        for (
            entries,
            description,
            feature_table,
        ) in row_entries_and_opt_features_and_description:
            new_table = make_table_fg_list(
                show_header=False,
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

        rich_console.print(*tables)
    else:
        the_table = make_table_fg_list(
            show_header=True,
            show_description=show_description,
            show_features=show_features,
        )
        for entries, description, _ in row_entries_and_opt_features_and_description:
            if show_description:
                entries.append(description or "")
            the_table.add_row(*entries)
        rich_console.print(the_table)
