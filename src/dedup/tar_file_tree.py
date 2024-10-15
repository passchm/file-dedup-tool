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

import tarfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path


@dataclass
class TarTree:
    info: tarfile.TarInfo | None
    path: Path
    children: list["TarTree"]


PathsTree = dict[str, "PathsTree"]


def build_paths_tree(paths: list[Path]) -> PathsTree:
    root: PathsTree = {}
    for path in paths:
        current_level = root
        for part in path.parts:
            if part not in current_level:
                current_level[part] = {}
            current_level = current_level[part]
    return root


def build_bare_tree(parent_node: TarTree, tree: PathsTree) -> None:
    for key, value in tree.items():
        child_node = TarTree(None, parent_node.path / key, [])
        if isinstance(value, dict):
            build_bare_tree(child_node, value)
        parent_node.children.append(child_node)


def match_info_to_node(node: TarTree, infos: list[tarfile.TarInfo]) -> None:
    assert node.info is None
    for info in infos:
        current_path = Path(info.name)
        if current_path == node.path:
            node.info = info
    for child_node in node.children:
        match_info_to_node(child_node, infos)


def flatten_tar_tree(node: TarTree) -> list[tarfile.TarInfo]:
    nodes = []
    if node.info:
        nodes.append(node.info)
    for subtree in node.children:
        nodes.extend(flatten_tar_tree(subtree))
    return nodes


def build_tar_tree(infos: list[tarfile.TarInfo]) -> TarTree:
    all_paths = list(map(lambda info: Path(info.name), infos))

    # TODO: TAR files can contain paths that are not unique!
    # Make sure that all paths are unique
    assert len(set(all_paths)) == len(all_paths)

    root_node = TarTree(None, Path(), [])

    paths_tree = build_paths_tree(all_paths)
    build_bare_tree(root_node, paths_tree)
    match_info_to_node(root_node, infos)

    # Check that all infos have been assigned to a node
    flattened_tree = frozenset(flatten_tar_tree(root_node))
    assert all(map(lambda info: info in flattened_tree, infos))

    return root_node


def print_tar_tree(tree: TarTree, level: int = 0) -> None:
    if tree.info:
        print("    " * level + "! " + str(tree.path) + " " + tree.info.name)
    else:
        print("    " * level + "? " + str(tree.path))
    for child in tree.children:
        print_tar_tree(child, level=level + 1)


def main() -> None:
    with BytesIO() as tar_memory:
        with tarfile.open(fileobj=tar_memory, mode="w") as tar_file:
            tar_member_data = b"hello world"
            with BytesIO(tar_member_data) as tar_member_obj:
                tar_member_info = tarfile.TarInfo()
                tar_member_info.name = "/usr/share/hello/world.txt"
                tar_member_info.size = len(tar_member_data)
                tar_file.addfile(tar_member_info, tar_member_obj)

            tar_member_data = b"hi moon"
            with BytesIO(tar_member_data) as tar_member_obj:
                tar_member_info = tarfile.TarInfo()
                tar_member_info.name = "/usr/share/hi/moon.txt"
                tar_member_info.size = len(tar_member_data)
                tar_file.addfile(tar_member_info, tar_member_obj)

        tar_data = tar_memory.getvalue()

    with BytesIO(tar_data) as tar_memory:
        with tarfile.open(fileobj=tar_memory, mode="r") as tf:
            tree = build_tar_tree(tf.getmembers())
            print_tar_tree(tree)


if __name__ == "__main__":
    main()
