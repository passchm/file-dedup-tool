#!/usr/bin/python3

# Copyright 2024 Pascal Schmid
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import argparse
import datetime
import sqlite3
from collections import defaultdict
from pathlib import Path

from .scan import Entry, EntryKind
from .xhtml5builder import XHT


def convert_row_to_entry(row: tuple) -> Entry:
    return Entry(
        EntryKind(row[0]),
        Path(row[1]),
        row[2],
        datetime.datetime.fromtimestamp(row[3], tz=datetime.timezone.utc),
        row[4],
    )


def build_tree(paths):
    tree = lambda: defaultdict(tree)
    root = tree()

    for path in paths:
        current_level = root
        for part in path.parts:
            current_level = current_level[part]

    return root


def print_tree(tree, level=0):
    for key, value in tree.items():
        print("    " * level + str(key))
        if isinstance(value, dict):
            print_tree(value, level=level + 1)


def dump_tree(tree, paths_to_entries: dict[Path, Entry], current_path: Path, level=0):
    for key, value in tree.items():
        line_text = str(key)
        if (current_path / key) in paths_to_entries:
            entry = paths_to_entries[current_path / key]
            line_text += " " + str(entry.kind)
        print("    " * level + line_text)
        if isinstance(value, dict):
            dump_tree(value, paths_to_entries, current_path / key, level=level + 1)


def render_tree(tree, paths_to_entries: dict[Path, Entry], current_path: Path):
    list_items = []
    for key, value in tree.items():
        current_item_content = []

        if (current_path / key) in paths_to_entries:
            entry = paths_to_entries[current_path / key]
            current_item_content.append(XHT("u", {}, str(entry.kind)))
            current_item_content.append(" " + str(key))
        else:
            current_item_content.append(str(key))

        if isinstance(value, dict):
            current_item_content.append(
                render_tree(value, paths_to_entries, current_path / key)
            )

        list_items.append(XHT("li", {}, *current_item_content))
    return XHT("ul", {}, *list_items)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--database", default="./dedup.sqlite3", help="the database file"
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.database)

    raw_paths = frozenset(
        sorted(
            map(
                lambda row: Path(row[0]),
                conn.execute("SELECT DISTINCT path FROM files ORDER BY path"),
            )
        )
    )
    bare_tree = build_tree(raw_paths)
    # print_tree(bare_tree)

    paths_to_entries = dict()
    for row in conn.execute("SELECT * FROM files ORDER BY path"):
        entry = convert_row_to_entry(row)
        paths_to_entries[entry.path] = entry

    conn.close()

    # dump_tree(bare_tree, paths_to_entries, Path())

    html_tree = XHT.page([], [render_tree(bare_tree, paths_to_entries, Path())])
    html_text = html_tree.xhtml5()
    Path("./index.xhtml").write_text(html_text)


if __name__ == "__main__":
    main()
