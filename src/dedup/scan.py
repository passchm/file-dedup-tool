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
import tarfile
import typing
import zipfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from .tar_file_tree import TarTree, build_tar_tree
from .zip_file_tree import ZipTree, build_zip_tree

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

    ZIP_MEMBER_FILE = 11
    ZIP_MEMBER_DIRECTORY = 12
    ZIP_MEMBER_COMPONENT = 14

    TAR_MEMBER_FILE = 21
    TAR_MEMBER_DIRECTORY = 22
    TAR_MEMBER_SYMLINK = 23
    TAR_MEMBER_COMPONENT = 24


@dataclass(frozen=True)
class Entry:
    kind: EntryKind
    path: Path
    size: int
    timestamp: datetime.datetime
    checksum: str | None
    parent_id: int


class UnknownEntryKindError(NotImplementedError):
    pass


def insert_entry(entry: Entry, conn: sqlite3.Connection) -> int:
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
            entry.parent_id,
        ),
    ).fetchone()[0]
    return int(entry_id)


def checksum_file(file_path: Path) -> str:
    with open(file_path, "rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def scan_zip_tree(
    zip_handle: zipfile.ZipFile,
    root_timestamp: datetime.datetime,
    tree: ZipTree,
    conn: sqlite3.Connection,
    parent_id: int,
) -> None:
    if not tree.info:
        entry = Entry(
            EntryKind.ZIP_MEMBER_COMPONENT,
            tree.path,
            0,
            root_timestamp,
            None,
            parent_id,
        )
        entry_id = insert_entry(entry, conn)
        for child in tree.children:
            scan_zip_tree(zip_handle, root_timestamp, child, conn, entry_id)
        return

    info = tree.info
    timestamp = datetime.datetime(
        *info.date_time,
        tzinfo=datetime.timezone.utc,
    )
    if info.is_dir():
        assert info.file_size == 0
        entry = Entry(
            EntryKind.ZIP_MEMBER_DIRECTORY,
            Path(info.filename),
            info.file_size,
            timestamp,
            None,
            parent_id,
        )
        entry_id = insert_entry(entry, conn)
        for child in tree.children:
            scan_zip_tree(zip_handle, root_timestamp, child, conn, entry_id)
    else:
        file_checksum = None

        is_encrypted = info.flag_bits & 0x1
        if is_encrypted:
            print(
                "ZIP warning:",
                parent_id,
                "contains an encrypted file called",
                info.filename,
            )
        else:
            try:
                with zip_handle.open(info.filename, "r") as f:
                    file_checksum = hashlib.file_digest(f, "sha256").hexdigest()  # type: ignore[arg-type]
            except zipfile.BadZipFile as ex:
                print("ZIP warning:", parent_id, ex)
            except NotImplementedError as ex:
                print("ZIP warning:", parent_id, ex)

        entry = Entry(
            EntryKind.ZIP_MEMBER_FILE,
            Path(info.filename),
            info.file_size,
            timestamp,
            file_checksum,
            parent_id,
        )
        entry_id = insert_entry(entry, conn)

        if Path(info.filename).suffix.lower() == ".zip":
            with zip_handle.open(info.filename, "r") as f:
                scan_zip_fileobj(
                    f,  # type: ignore[arg-type]
                    conn,
                    entry_id,
                )
        elif ".tar" in map(lambda s: s.lower(), Path(info.filename).suffixes):
            with zip_handle.open(info.filename, "r") as f:
                scan_tar_fileobj(
                    f,  # type: ignore[arg-type]
                    conn,
                    entry_id,
                )


def scan_zip_fileobj(
    zip_fileobj: typing.BinaryIO, conn: sqlite3.Connection, parent_id: int
) -> None:
    if not zipfile.is_zipfile(zip_fileobj):
        print("ZIP warning:", parent_id, "not a ZIP archive")
        return

    root_timestamp = datetime.datetime.fromtimestamp(
        conn.execute(
            "SELECT timestamp FROM files WHERE id = ?", (parent_id,)
        ).fetchone()[0],
        tz=datetime.timezone.utc,
    )

    try:
        with zipfile.ZipFile(zip_fileobj) as zf:
            root_node = build_zip_tree(list(zf.infolist()))
            for child in root_node.children:
                scan_zip_tree(zf, root_timestamp, child, conn, parent_id)
    except UnicodeDecodeError as ex:
        print("ZIP warning:", parent_id, ex)


def scan_tar_tree(
    tar_handle: tarfile.TarFile,
    root_timestamp: datetime.datetime,
    tree: TarTree,
    conn: sqlite3.Connection,
    parent_id: int,
) -> None:
    if not tree.info:
        entry = Entry(
            EntryKind.TAR_MEMBER_COMPONENT,
            tree.path,
            0,
            root_timestamp,
            None,
            parent_id,
        )
        entry_id = insert_entry(entry, conn)
        for child in tree.children:
            scan_tar_tree(tar_handle, root_timestamp, child, conn, entry_id)
        return

    info = tree.info
    timestamp = datetime.datetime.fromtimestamp(
        info.mtime,
        tz=datetime.timezone.utc,
    )
    if info.issym():
        entry = Entry(
            EntryKind.TAR_MEMBER_SYMLINK,
            Path(info.name),
            info.size,
            timestamp,
            None,
            parent_id,
        )
        insert_entry(entry, conn)
    elif info.isdir():
        entry = Entry(
            EntryKind.TAR_MEMBER_DIRECTORY,
            Path(info.name),
            info.size,
            timestamp,
            None,
            parent_id,
        )
        entry_id = insert_entry(entry, conn)
        for child in tree.children:
            scan_tar_tree(tar_handle, root_timestamp, child, conn, entry_id)
    elif info.isfile():
        f = tar_handle.extractfile(info)
        file_checksum = hashlib.file_digest(f, "sha256").hexdigest()  # type: ignore[arg-type]

        entry = Entry(
            EntryKind.TAR_MEMBER_FILE,
            Path(info.name),
            info.size,
            timestamp,
            file_checksum,
            parent_id,
        )
        entry_id = insert_entry(entry, conn)

        if Path(info.name).suffix.lower() == ".zip":
            f = tar_handle.extractfile(info)
            scan_zip_fileobj(
                f,  # type: ignore[arg-type]
                conn,
                entry_id,
            )
        elif ".tar" in map(lambda s: s.lower(), Path(info.name).suffixes):
            f = tar_handle.extractfile(info)
            scan_tar_fileobj(
                f,  # type: ignore[arg-type]
                conn,
                entry_id,
            )
    else:
        raise UnknownEntryKindError("unknown kind of archive member")


def scan_tar_fileobj(
    tar_fileobj: typing.BinaryIO, conn: sqlite3.Connection, parent_id: int
) -> None:
    assert tarfile.is_tarfile(tar_fileobj)

    root_timestamp = datetime.datetime.fromtimestamp(
        conn.execute(
            "SELECT timestamp FROM files WHERE id = ?", (parent_id,)
        ).fetchone()[0],
        tz=datetime.timezone.utc,
    )

    with tarfile.open(fileobj=tar_fileobj, mode="r") as tf:
        root_node = build_tar_tree(tf.getmembers())
        for child in root_node.children:
            scan_tar_tree(tf, root_timestamp, child, conn, parent_id)


def scan_path(path: Path, conn: sqlite3.Connection, parent_id: int) -> None:
    if path.is_symlink():
        entry = Entry(
            EntryKind.SYMLINK,
            path,
            path.lstat().st_size,
            datetime.datetime.fromtimestamp(
                path.lstat().st_mtime,
                tz=datetime.timezone.utc,
            ),
            None,
            parent_id,
        )
        insert_entry(entry, conn)
    elif path.is_dir():
        assert not path.is_symlink()

        entry = Entry(
            EntryKind.DIRECTORY,
            path,
            path.lstat().st_size,
            datetime.datetime.fromtimestamp(
                path.lstat().st_mtime,
                tz=datetime.timezone.utc,
            ),
            None,
            parent_id,
        )
        entry_id = insert_entry(entry, conn)
        for child_path in sorted(path.iterdir()):
            scan_path(child_path, conn, entry_id)
    elif path.is_file():
        assert not path.is_symlink()

        entry = Entry(
            EntryKind.FILE,
            path,
            path.lstat().st_size,
            datetime.datetime.fromtimestamp(
                path.lstat().st_mtime,
                tz=datetime.timezone.utc,
            ),
            checksum_file(path),
            parent_id,
        )
        entry_id = insert_entry(entry, conn)

        if path.suffix.lower() == ".zip":
            with path.open("rb") as f:
                scan_zip_fileobj(f, conn, entry_id)
        elif ".tar" in map(lambda s: s.lower(), path.suffixes):
            with path.open("rb") as f:
                scan_tar_fileobj(f, conn, entry_id)
    else:
        raise UnknownEntryKindError("unknown kind of path " + repr(path))


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

        with conn:
            scan_path(target_path, conn, 0)

    conn.close()


if __name__ == "__main__":
    main()
