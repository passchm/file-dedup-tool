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


def fetch_entry_by_id(conn: sqlite3.Connection, entry_id: int) -> Entry:
    row = conn.execute(
        (
            "SELECT kind, path, size, timestamp, checksum, parent_id"
            " FROM files WHERE id = ?"
        ),
        (entry_id,),
    ).fetchone()
    return Entry(
        EntryKind(row[0]),
        Path(row[1]),
        row[2],
        datetime.datetime.fromtimestamp(row[3], tz=datetime.timezone.utc),
        row[4],
        row[5],
    )


def render_entry(conn: sqlite3.Connection, entry_id: int) -> XHT:
    entry_content = []

    entry = fetch_entry_by_id(conn, entry_id)

    if entry.kind in [EntryKind.FILE, EntryKind.DIRECTORY, EntryKind.SYMLINK]:
        entry_content.append(XHT("p", {}, entry.path.name))
    else:
        entry_content.append(XHT("p", {}, str(entry.path)))

    has_duplicates = False
    if entry.size > 0 and entry.checksum:
        rendered_duplicates = []

        for dupe_row in conn.execute(
            (
                "SELECT id FROM files"
                " WHERE size = ? AND checksum = ? AND id != ?"
                " ORDER BY path"
            ),
            (entry.size, entry.checksum, entry_id),
        ):
            dupe_id = int(dupe_row[0])
            dupe = fetch_entry_by_id(conn, dupe_id)
            rendered_duplicates.append(
                XHT(
                    "li",
                    {"class": "same-name"} if dupe.path.name == entry.path.name else {},
                    XHT("a", {"href": "#node-" + str(dupe_id)}, str(dupe.path)),
                )
            )

        if len(rendered_duplicates) > 0:
            has_duplicates = True

        entry_content.append(XHT("ul", {"class": "duplicates"}, *rendered_duplicates))

    rendered_children = []

    children = conn.execute(
        "SELECT id FROM files WHERE parent_id = ? ORDER BY path", (entry_id,)
    ).fetchall()
    for child_id in map(lambda row: int(row[0]), children):
        rendered_children.append(render_entry(conn, child_id))

    if len(rendered_children) > 0:
        entry_content.append(XHT("ul", {"class": "children"}, *rendered_children))

    entry_attributes = {}

    entry_attributes["id"] = "node-" + str(entry_id)

    if has_duplicates:
        entry_attributes["class"] = "has-duplicates"

    return XHT("li", entry_attributes, *entry_content)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--database", default="./dedup.sqlite3", help="the database file"
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.database)

    rendered_root_entries = []
    for root_id in conn.execute("SELECT id FROM files WHERE parent_id = 0"):
        rendered_root_entries.append(render_entry(conn, root_id[0]))

    conn.close()

    html_tree = XHT.page(
        [XHT("style", {}, (Path(__file__).parent / "style.css").read_text())],
        [XHT("ul", {}, *rendered_root_entries)],
    )
    html_text = html_tree.xhtml5()
    Path("./index.xhtml").write_text(html_text)


if __name__ == "__main__":
    main()
