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
from pathlib import Path

from .scan import Entry, EntryKind
from .xhtml5builder import XHT


def load_tree(conn: sqlite3.Connection, entry_id: int) -> Entry:
    children = []
    for child_id in conn.execute(
        "SELECT id FROM files WHERE parent_id = ?", (entry_id,)
    ):
        children.append(load_tree(conn, child_id[0]))

    row = conn.execute(
        "SELECT kind, path, size, timestamp, checksum FROM files WHERE id = ?",
        (entry_id,),
    ).fetchone()
    return Entry(
        EntryKind(row[0]),
        Path(row[1]),
        row[2],
        datetime.datetime.fromtimestamp(row[3], tz=datetime.timezone.utc),
        row[4],
        children,
    )


def render_tree(entry: Entry, grouped_entries: dict[str, list[Entry]]) -> XHT:
    entry_content = []

    if entry.kind in [EntryKind.FILE, EntryKind.DIRECTORY, EntryKind.SYMLINK]:
        entry_content.append(XHT("p", {}, entry.path.name))
    else:
        entry_content.append(XHT("p", {}, str(entry.path)))

    has_duplicates = False
    if entry.size > 0 and entry.checksum and len(grouped_entries[entry.checksum]) > 1:
        has_duplicates = True
        duplicates = []
        for dupe in grouped_entries[entry.checksum]:
            assert dupe.size == entry.size
            if dupe != entry:
                if dupe.path.name == entry.path.name:
                    duplicates.append(XHT("li", {"class": "same-name"}, str(dupe.path)))
                else:
                    duplicates.append(XHT("li", {}, str(dupe.path)))
        entry_content.append(XHT("ul", {"class": "duplicates"}, *duplicates))

    if len(entry.children) > 0:
        entry_content.append(
            XHT(
                "ul",
                {},
                *map(lambda e: render_tree(e, grouped_entries), entry.children)
            )
        )

    if has_duplicates:
        return XHT("li", {"class": "has-duplicates"}, *entry_content)
    else:
        return XHT("li", {}, *entry_content)


def flatten_entries(entry: Entry) -> list[Entry]:
    entries = [entry]
    for child in entry.children:
        entries.extend(flatten_entries(child))
    return entries


def group_by_checksums(flattened_entries: list[Entry]) -> dict[str, list[Entry]]:
    checksums_to_entries = dict()
    for entry in flattened_entries:
        if entry.checksum:
            if entry.checksum not in checksums_to_entries:
                checksums_to_entries[entry.checksum] = [entry]
            else:
                checksums_to_entries[entry.checksum].append(entry)
    return checksums_to_entries


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--database", default="./dedup.sqlite3", help="the database file"
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.database)

    root_entries = []
    for root_id in conn.execute("SELECT id FROM files WHERE parent_id = 0"):
        root_entries.append(load_tree(conn, root_id[0]))

    conn.close()

    flattened_entries = []
    for root_entry in root_entries:
        flattened_entries.extend(flatten_entries(root_entry))
    grouped_entries = group_by_checksums(flattened_entries)

    html_tree = XHT.page(
        [XHT("style", {}, (Path(__file__).parent / "style.css").read_text())],
        [XHT("ul", {}, *map(lambda e: render_tree(e, grouped_entries), root_entries))],
    )
    html_text = html_tree.xhtml5()
    Path("./index.xhtml").write_text(html_text)


if __name__ == "__main__":
    main()
