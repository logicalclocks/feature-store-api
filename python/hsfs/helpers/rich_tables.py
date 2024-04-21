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
from hsfs.helpers import constants
from rich.table import Table


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
