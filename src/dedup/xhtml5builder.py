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

import xml.etree.ElementTree as ET
from io import BytesIO
from typing import Union


class XHT:
    def __init__(
        self, tag: str, attributes: dict[str, str], *content: Union[str, "XHT"]
    ):
        self.tag = tag
        self.attributes = attributes
        self.content = content

    def element(self) -> ET.Element:
        if len(self.attributes) > 0:
            e = ET.Element(self.tag, self.attributes)
        else:
            e = ET.Element(self.tag)

        if len(self.content) > 0:
            current_element = None
            for i in range(len(self.content)):
                content_part = self.content[i]
                if isinstance(content_part, XHT):
                    current_element = content_part.element()
                    e.append(current_element)
                else:
                    assert isinstance(content_part, str)
                    if current_element is None:
                        assert e.text is None
                        e.text = content_part
                    else:
                        assert current_element.tail is None
                        current_element.tail = content_part

        return e

    def xhtml5(self) -> str:
        tree = ET.ElementTree(self.element())
        with BytesIO() as bio:
            if tree.getroot().tag == "html":
                bio.write(b"<!DOCTYPE html>\n")
            tree.write(bio, encoding="utf-8")
            if tree.getroot().tag == "html":
                bio.write(b"\n")
            return bio.getvalue().decode("utf-8")

    @classmethod
    def page(
        cls,
        head: list[Union[str, "XHT"]],
        body: list[Union[str, "XHT"]],
    ) -> "XHT":
        return cls(
            "html",
            {
                "xmlns": "http://www.w3.org/1999/xhtml",
                "xml:lang": "en-US",
                "lang": "en-US",
            },
            cls(
                "head",
                {},
                cls("meta", {"charset": "utf-8"}),
                cls(
                    "meta",
                    {
                        "name": "viewport",
                        "content": "width=device-width, initial-scale=1",
                    },
                ),
                *head,
            ),
            cls("body", {}, *body),
        )


def main() -> None:
    print(XHT.page([XHT("title", {}, "Hello")], [XHT("p", {}, "World")]).xhtml5())
    print(XHT("p", {}, "Hello ", XHT("b", {}, "World<br />")).xhtml5())


if __name__ == "__main__":
    main()
