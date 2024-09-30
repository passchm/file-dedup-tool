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
import hashlib
import sqlite3
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

SQL_INIT = """\
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    kind INTEGER,
    path TEXT,
    size INTEGER,
    timestamp REAL,
    checksum TEXT,
    parent_id INTEGER,
    FOREIGN KEY (parent_id) REFERENCES files(id)
);
"""


class EntryKind(Enum):
    FILE = 1
    DIRECTORY = 2
    SYMLINK = 3


@dataclass(frozen=True)
class Entry:
    kind: EntryKind
    path: Path
    size: int
    timestamp: datetime.datetime
    checksum: str | None
    children: list["Entry"]


class UnknownEntryKindError(NotImplementedError):
    pass


def checksum_file(file_path: Path) -> str:
    with open(file_path, "rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def scan_path(path: Path) -> Entry:
    if path.is_symlink():
        return Entry(
            EntryKind.SYMLINK,
            path,
            path.lstat().st_size,
            datetime.datetime.fromtimestamp(
                path.lstat().st_mtime,
                tz=datetime.timezone.utc,
            ),
            None,
            [],
        )
    elif path.is_dir():
        assert not path.is_symlink()
        return Entry(
            EntryKind.DIRECTORY,
            path,
            path.lstat().st_size,
            datetime.datetime.fromtimestamp(
                path.lstat().st_mtime,
                tz=datetime.timezone.utc,
            ),
            None,
            list(map(scan_path, sorted(path.iterdir()))),
        )
    elif path.is_file():
        assert not path.is_symlink()
        return Entry(
            EntryKind.FILE,
            path,
            path.lstat().st_size,
            datetime.datetime.fromtimestamp(
                path.lstat().st_mtime,
                tz=datetime.timezone.utc,
            ),
            checksum_file(path),
            [],
        )
    else:
        raise UnknownEntryKindError("unknown kind of path " + repr(path))


def insert_entries_recursively(
    entry: Entry,
    conn: sqlite3.Connection,
    parent_id: int,
) -> None:
    entry_path = bytes(entry.path).decode("utf-8", errors="replace")
    entry_id = conn.execute(
        "INSERT INTO files (kind, path, size, timestamp, checksum, parent_id)"
        + " VALUES (?, ?, ?, ?, ?, ?)"
        + " RETURNING id",
        (
            entry.kind.value,
            entry_path,
            entry.size,
            entry.timestamp.timestamp(),
            entry.checksum,
            parent_id,
        ),
    ).fetchone()[0]

    for child in entry.children:
        insert_entries_recursively(child, conn, entry_id)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--database", default="./dedup.sqlite3", help="the database file"
    )
    parser.add_argument(
        "target_paths",
        metavar="target-path",
        nargs="+",
        help="the target paths which should be scanned",
    )

    args = parser.parse_args()

    conn = sqlite3.connect(args.database)

    conn.execute(SQL_INIT)
    conn.commit()

    for target_path_str in args.target_paths:
        target_path = Path(target_path_str)
        assert target_path.exists()
        print(target_path)

        root_entry = scan_path(target_path)

        with conn:
            insert_entries_recursively(root_entry, conn, 0)

    conn.close()


if __name__ == "__main__":
    main()
