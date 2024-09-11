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
import sys
import tarfile
import typing
import zipfile
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

# Issues with nested archives:
# - If a ZIP file is inside of a TAR file, the files inside of the ZIP file will
#   have EntryKind.ZIP_MEMBER_FILE.
# - If a TAR file is inside of a ZIP file, the files inside of the TAR file will
#   have EntryKind.TAR_MEMBER_FILE.
# - The root directory inside of the archive has the same path
#   as the archive file itself.
# Therefore, this functionality is currently disabled.
SCAN_NESTED_ARCHIVES = False


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


def scan_tar_fileobj(tar_path: Path, tar_fileobj: typing.BinaryIO) -> list[Entry]:
    items = []
    assert tarfile.is_tarfile(tar_fileobj)
    with tarfile.open(fileobj=tar_fileobj, mode="r") as tf:
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

                if SCAN_NESTED_ARCHIVES:
                    if Path(info.name).suffix.lower() == ".zip":
                        print("ZIP in TAR:", tar_path / info.name)
                        f = tf.extractfile(info)
                        items.extend(scan_zip_fileobj(tar_path / info.name, f))

                    if ".tar" in map(lambda s: s.lower(), Path(info.name).suffixes):
                        print("TAR in TAR:", tar_path / info.name)
                        f = tf.extractfile(info)
                        items.extend(scan_tar_fileobj(tar_path / info.name, f))
    return list(sorted(items, key=lambda item: item.path))


def scan_tar_file(tar_path: Path) -> list[Entry]:
    with open(tar_path, "rb") as tar_fileobj:
        return scan_tar_fileobj(tar_path, tar_fileobj)


def scan_zip_fileobj(zip_path: Path, zip_fileobj: typing.BinaryIO) -> list[Entry]:
    items = []
    assert zipfile.is_zipfile(zip_fileobj)
    with zipfile.ZipFile(zip_fileobj) as zf:
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

                if SCAN_NESTED_ARCHIVES:
                    if Path(info.filename).suffix.lower() == ".zip":
                        print("ZIP in ZIP:", zip_path / info.filename)
                        with zf.open(info.filename, "r") as f:
                            items.extend(scan_zip_fileobj(zip_path / info.filename, f))

                    if ".tar" in map(lambda s: s.lower(), Path(info.filename).suffixes):
                        print("TAR in ZIP:", zip_path / info.filename)
                        with zf.open(info.filename, "r") as f:
                            items.extend(scan_tar_fileobj(zip_path / info.filename, f))
    return list(sorted(items, key=lambda item: item.path))


def scan_zip_file(zip_path: Path) -> list[Entry]:
    with open(zip_path, "rb") as zip_fileobj:
        return scan_zip_fileobj(zip_path, zip_fileobj)


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
