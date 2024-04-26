#
#   Copyright 2024 HOPSWOKRS AB
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

from pathlib import Path
from typing import Literal

from hsfs.helpers import constants, verbose
from rich.markdown import Markdown


def load_class_quicktour(class_name: Literal["feature_store"]) -> str:
    path = Path(__file__).parent / f"{class_name}.md"
    try:
        with open(path, "r") as f:
            return f.read()
    except FileNotFoundError:
        return f"Quicktour for {class_name} not found"


def rich_print_quicktour(class_name: Literal["feature_store"]) -> None:
    if verbose.is_rich_print_enabled():
        markdown = Markdown(
            load_class_quicktour(class_name),
            justify="left",
            code_theme=verbose.get_python_lexer_theme(),
            inline_code_theme=verbose.get_python_lexer_theme(),
            inline_code_lexer="python",
        )
        verbose.get_rich_console().print(markdown)
    else:
        print(constants.GET_OR_ENABLE_RICH_CONSOLE_ERROR_MESSAGE)
