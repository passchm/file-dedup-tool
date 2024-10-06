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


def render_tree(entry: Entry) -> XHT:
    entry_content = []

    if entry.kind in [EntryKind.ZIP_MEMBER_FILE, EntryKind.ZIP_MEMBER_DIRECTORY]:
        entry_content.append(XHT("p", {}, str(entry.path)))
    else:
        entry_content.append(XHT("p", {}, entry.path.name))

    if len(entry.children) > 0:
        entry_content.append(XHT("ul", {}, *map(render_tree, entry.children)))

    return XHT("li", {}, *entry_content)


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

    html_tree = XHT.page(
        [XHT("style", {}, (Path(__file__).parent / "style.css").read_text())],
        [XHT("ul", {}, *map(render_tree, root_entries))],
    )
    html_text = html_tree.xhtml5()
    Path("./index.xhtml").write_text(html_text)


if __name__ == "__main__":
    main()
