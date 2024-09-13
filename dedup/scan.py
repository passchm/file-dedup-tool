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
    kind INTEGER,
    path TEXT,
    size INTEGER,
    timestamp INTEGER,
    checksum TEXT
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
    checksum: str


class UnknownEntryKindError(NotImplementedError):
    pass


def checksum_file(file_path: Path) -> str:
    with open(file_path, "rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def scan_path(path: Path) -> list[Entry]:
    items = []
    if path.is_symlink():
        items.append(
            Entry(
                EntryKind.SYMLINK,
                path,
                path.lstat().st_size,
                datetime.datetime.fromtimestamp(
                    path.lstat().st_mtime,
                    tz=datetime.timezone.utc,
                ),
                None,
            )
        )
    elif path.is_dir():
        assert not path.is_symlink()
        items.append(
            Entry(
                EntryKind.DIRECTORY,
                path,
                path.lstat().st_size,
                datetime.datetime.fromtimestamp(
                    path.lstat().st_mtime,
                    tz=datetime.timezone.utc,
                ),
                None,
            )
        )
        for p in sorted(path.iterdir()):
            items.extend(scan_path(p))
    elif path.is_file():
        assert not path.is_symlink()
        items.append(
            Entry(
                EntryKind.FILE,
                path,
                path.lstat().st_size,
                datetime.datetime.fromtimestamp(
                    path.lstat().st_mtime,
                    tz=datetime.timezone.utc,
                ),
                checksum_file(path),
            )
        )

        if path.suffix.lower() == ".zip":
            # TODO: Scan the ZIP file
            pass

        if ".tar" in map(lambda s: s.lower(), path.suffixes):
            # TODO: Scan the TAR file
            pass
    else:
        raise UnknownEntryKindError("unknown kind of path " + repr(path))
    return items


def main():
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

        entries = scan_path(target_path)
        print(len(entries))

        mapped_entries = map(
            lambda e: (
                e.kind.value,
                str(e.path),
                e.size,
                e.timestamp.timestamp(),
                e.checksum,
            ),
            entries,
        )
        with conn:
            conn.executemany(
                "INSERT INTO files (kind, path, size, timestamp, checksum) "
                + "VALUES (?, ?, ?, ?, ?)",
                mapped_entries,
            )

    conn.close()


if __name__ == "__main__":
    main()
