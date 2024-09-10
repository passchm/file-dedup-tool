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


def print_tree(d, level=0):
    for key, value in d.items():
        print("    " * level + str(key))
        if isinstance(value, dict):
            print_tree(value, level=level + 1)


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

    for row in conn.execute("SELECT * FROM files ORDER BY path"):
        entry = convert_row_to_entry(row)

    conn.close()

    bare_tree = build_tree(raw_paths)
    print_tree(bare_tree)


if __name__ == "__main__":
    main()
