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

import datetime
import hashlib
import sqlite3
import sys
import tarfile
import zipfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

SQL_INIT = """
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
    ZIP_MEMBER_FILE = 101
    ZIP_MEMBER_DIRECTORY = 102
    TAR_MEMBER_FILE = 201
    TAR_MEMBER_DIRECTORY = 202
    TAR_MEMBER_SYMLINK = 203


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


def scan_tar_file(tar_path: Path) -> list[Entry]:
    items = []
    assert tarfile.is_tarfile(tar_path)
    with tarfile.open(tar_path) as tf:
        for info in tf:
            timestamp = datetime.datetime.fromtimestamp(
                info.mtime,
                tz=datetime.timezone.utc,
            )
            if info.issym():
                items.append(
                    Entry(
                        EntryKind.TAR_MEMBER_SYMLINK,
                        tar_path / info.name,
                        info.size,
                        timestamp,
                        None,
                    )
                )
            elif info.isdir():
                items.append(
                    Entry(
                        EntryKind.TAR_MEMBER_DIRECTORY,
                        tar_path / info.name,
                        info.size,
                        timestamp,
                        None,
                    )
                )
            elif info.isfile():
                f = tf.extractfile(info)
                file_checksum = hashlib.file_digest(f, "sha256").hexdigest()
                items.append(
                    Entry(
                        EntryKind.TAR_MEMBER_FILE,
                        tar_path / info.name,
                        info.size,
                        timestamp,
                        file_checksum,
                    )
                )
    return list(sorted(items, key=lambda item: item.path))


def scan_zip_file(zip_path: Path) -> list[Entry]:
    items = []
    assert zipfile.is_zipfile(zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            timestamp = datetime.datetime(
                *info.date_time,
                tzinfo=datetime.timezone.utc,
            )
            if info.is_dir():
                assert info.file_size == 0
                items.append(
                    Entry(
                        EntryKind.ZIP_MEMBER_DIRECTORY,
                        zip_path / info.filename,
                        info.file_size,
                        timestamp,
                        None,
                    )
                )
            else:
                with zf.open(info.filename, "r") as f:
                    file_checksum = hashlib.file_digest(f, "sha256").hexdigest()
                items.append(
                    Entry(
                        EntryKind.ZIP_MEMBER_FILE,
                        zip_path / info.filename,
                        info.file_size,
                        timestamp,
                        file_checksum,
                    )
                )
    return list(sorted(items, key=lambda item: item.path))


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
            assert zipfile.is_zipfile(path)
            items.extend(scan_zip_file(path))

        if ".tar" in map(lambda s: s.lower(), path.suffixes):
            assert tarfile.is_tarfile(path)
            items.extend(scan_tar_file(path))
    else:
        raise UnknownEntryKindError("unknown kind of path " + repr(path))
    return items


def main():
    target_path = Path(sys.argv[1])
    print(target_path)
    assert target_path.exists()

    conn = sqlite3.connect("dedup.sqlite3")

    conn.execute(SQL_INIT)
    conn.commit()

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
    conn.executemany(
        "INSERT INTO files (kind, path, size, timestamp, checksum)"
        + "VALUES (?, ?, ?, ?, ?)",
        mapped_entries,
    )
    conn.commit()

    conn.close()


if __name__ == "__main__":
    main()
