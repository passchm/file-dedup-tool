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


def convert_row_to_entry(row: tuple[int, str, int, float, str | None]) -> Entry:
    return Entry(
        EntryKind(row[0]),
        Path(row[1]),
        row[2],
        datetime.datetime.fromtimestamp(row[3], tz=datetime.timezone.utc),
        row[4],
        [],
    )


PathsTree = dict[str, "PathsTree"]


def build_paths_tree(paths: frozenset[Path]) -> PathsTree:
    root: PathsTree = {}
    for path in paths:
        current_level = root
        for part in path.parts:
            if part not in current_level:
                current_level[part] = {}
            current_level = current_level[part]
    return root


def render_tree(
    tree: PathsTree,
    paths_to_entries: dict[Path, Entry],
    checksums_to_entries: dict[str, list[Entry]],
    parent_path: Path,
) -> XHT:
    list_items = []
    for key, value in tree.items():
        current_path = parent_path / key
        current_item_content = []
        current_item_class_list = []

        if current_path in paths_to_entries:
            entry = paths_to_entries[current_path]

            current_item_content.append(XHT("span", {}, str(key)))

            # Duplicates
            if (
                entry.size > 0
                and entry.checksum
                and len(checksums_to_entries[entry.checksum]) > 1
            ):
                dupes_list = []
                for dupe in checksums_to_entries[entry.checksum]:
                    if dupe != entry:
                        assert dupe.size == entry.size
                        dupe_class_list = []
                        if dupe.path.name == entry.path.name:
                            dupe_class_list.append("same-name")
                        dupes_list.append(
                            XHT(
                                "li",
                                {"class": " ".join(dupe_class_list)},
                                str(dupe.path),
                            )
                        )
                current_item_class_list.append("has-duplicates")
                current_item_content.append(
                    XHT("ul", {"class": "duplicates"}, *dupes_list)
                )
        else:
            current_item_content.append(XHT("span", {}, str(key)))

        if isinstance(value, dict):
            current_item_content.append(
                render_tree(value, paths_to_entries, checksums_to_entries, current_path)
            )

        list_items.append(
            XHT(
                "li",
                (
                    {"class": " ".join(current_item_class_list)}
                    if len(current_item_class_list) > 0
                    else {}
                ),
                *current_item_content
            )
        )
    return XHT("ul", {}, *list_items)


def main() -> None:
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
    bare_tree = build_paths_tree(raw_paths)

    paths_to_entries = dict()
    checksums_to_entries = dict()
    for row in conn.execute("SELECT * FROM files ORDER BY path"):
        entry = convert_row_to_entry(row)
        paths_to_entries[entry.path] = entry
        if entry.checksum:
            if entry.checksum not in checksums_to_entries:
                checksums_to_entries[entry.checksum] = [entry]
            else:
                checksums_to_entries[entry.checksum].append(entry)

    conn.close()

    html_tree = XHT.page(
        [XHT("style", {}, (Path(__file__).parent / "style.css").read_text())],
        [render_tree(bare_tree, paths_to_entries, checksums_to_entries, Path())],
    )
    html_text = html_tree.xhtml5()
    Path("./index.xhtml").write_text(html_text)


if __name__ == "__main__":
    main()
