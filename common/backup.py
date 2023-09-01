import dataclasses
import datetime
import pathlib
import re
from typing import Any


@dataclasses.dataclass(frozen=True)
class TagUnique:
    title: str
    category: str


@dataclasses.dataclass(frozen=True)
class ImageHash:
    aspect_ratio: float
    value_hash: str
    hue_hash: int
    saturation_hash: int
    alternate_version: bool


@dataclasses.dataclass(frozen=True)
class ContentRepresentationElement:
    format: str
    compatibility_level: int
    file_path: pathlib.Path

    def json_serializable(self) -> dict[str, Any]:
        serializable = dataclasses.asdict(self)
        serializable["file_path"] = str(self.file_path)
        return serializable


@dataclasses.dataclass(frozen=True)
class AlbumOrder:
    set_tag: TagUnique
    artist_tag: TagUnique
    order: int


@dataclasses.dataclass
class AlternateSource:
    origin_name: str
    origin_content_id: str


@dataclasses.dataclass
class ContentDocument:
    content_id: int
    file_path: pathlib.Path
    title: str
    content_type: str
    description: str
    addition_date: datetime.datetime
    origin: str
    origin_content_id: str
    is_hidden: bool
    tags: set[TagUnique]
    alternate_sources: list[AlternateSource]
    imagehash: ImageHash | None = None
    representations: list[ContentRepresentationElement] | None = None
    albums: list[AlbumOrder] | None = None

    def json_serializable(self) -> dict[str, Any]:
        serializable = dataclasses.asdict(self)
        serializable["file_path"] = str(self.file_path)
        serializable["addition_date"] = self.addition_date.isoformat()
        serializable["tags"] = [dataclasses.asdict(tag) for tag in self.tags]
        if self.representations is not None:
            serializable["representations"] = [
                repr.json_serializable() for repr in self.representations
            ]
        return serializable


@dataclasses.dataclass
class TagDocument:
    title: str
    category: str
    aliases: set[str]
    parent: TagUnique | None

    def json_serializable(self) -> dict[str, Any]:
        serializable = dataclasses.asdict(self)
        serializable["aliases"] = list(self.aliases)
        if self.parent is not None:
            serializable["parent"] = dataclasses.asdict(self.parent)
        return serializable


file_template_regex = re.compile("\$[\da-zA-Z\-%]+\$")
