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

import zipfile
from collections import defaultdict
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path


@dataclass
class ZipTree:
    info: zipfile.ZipInfo
    path: Path
    children: list["ZipTree"]


def build_paths_tree(paths: list[Path]):
    tree = lambda: defaultdict(tree)
    root = tree()

    for path in paths:
        current_level = root
        for part in path.parts:
            current_level = current_level[part]

    return root


def build_bare_tree(parent_node: ZipTree, tree: dict) -> None:
    for key, value in tree.items():
        child_node = ZipTree(None, parent_node.path / key, [])
        if isinstance(value, dict):
            build_bare_tree(child_node, value)
        parent_node.children.append(child_node)


def match_info_to_node(node: ZipTree, infos: list[zipfile.ZipInfo]) -> None:
    assert node.info is None
    for info in infos:
        current_path = Path(info.filename)
        if current_path == node.path:
            node.info = info
    for child_node in node.children:
        match_info_to_node(child_node, infos)


def flatten_zip_tree(node: ZipTree) -> list[zipfile.ZipInfo]:
    nodes = []
    if node.info:
        nodes.append(node.info)
    for subtree in node.children:
        nodes.extend(flatten_zip_tree(subtree))
    return nodes


def build_zip_tree(infos: list[zipfile.ZipInfo]) -> ZipTree:
    all_paths = list(map(lambda info: Path(info.filename), infos))
    # Make sure that all paths are unique
    assert len(set(all_paths)) == len(all_paths)

    root_node = ZipTree(None, Path(), [])

    paths_tree = build_paths_tree(all_paths)
    build_bare_tree(root_node, paths_tree)
    match_info_to_node(root_node, infos)

    # Check that all infos have been assigned to a node
    flattened_tree = flatten_zip_tree(root_node)
    assert all(map(lambda info: info in flattened_tree, infos))

    return root_node


def print_zip_tree(tree: ZipTree, level=0):
    if tree.info:
        print("    " * level + "! " + str(tree.path) + " " + tree.info.filename)
    else:
        print("    " * level + "? " + str(tree.path))
    for child in tree.children:
        print_zip_tree(child, level=level + 1)


def main():
    zip_memory = BytesIO()

    with zipfile.ZipFile(zip_memory, "w") as zf:
        zf.writestr("a/aa1.txt", "AA1")
        zf.writestr("a/aa2.txt", "AA2")
        zf.writestr("a/aa/aaa.txt", "AAA")
        zf.writestr("b/bb/bbb.txt", "BBB")
        zf.writestr("c.txt", "C")

    with zipfile.ZipFile(zip_memory, "r") as zf:
        infos = list(zf.infolist())

        tree = build_zip_tree(infos)
        print_zip_tree(tree)


if __name__ == "__main__":
    main()
