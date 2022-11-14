import dataclasses
import datetime
import io
import pathlib
import json
from typing import Any
import argparse
import tarfile
import crcmod

import common


@dataclasses.dataclass(frozen=True)
class TagUnique:
    title: str
    category: str


@dataclasses.dataclass
class ContentDocument:
    file_path: pathlib.Path
    title: str
    content_type: str
    description: str
    addition_date: datetime.datetime
    origin: str
    origin_content_id: str
    is_hidden: bool
    tags: set[TagUnique]

    def json_serializable(self) -> dict[str, Any]:
        serializable = dataclasses.asdict(self)
        serializable["file_path"] = str(self.file_path)
        serializable["addition_date"] = self.addition_date.isoformat()
        serializable["tags"] = [dataclasses.asdict(tag) for tag in self.tags]
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


tags_uniq: set[TagUnique] = set()
crc64 = crcmod.predefined.mkCrcFun("crc-64")
checksums: list[tuple[int, pathlib.PurePath]] = []


def create_tar_file(tar_dump: tarfile.TarFile, buffer: io.StringIO, path: pathlib.PurePath, write_crc=True):
    global checksums
    encoded_data = buffer.getvalue().encode("utf-8")
    if write_crc:
        data_crc64 = crc64(encoded_data)
        checksums.append((data_crc64, path))
    data_buffer = io.BytesIO(encoded_data)
    tar_tag_info = tarfile.TarInfo(str(path))
    tar_tag_info.size = len(encoded_data)
    tar_dump.addfile(tar_tag_info, data_buffer)


def main():
    global checksums

    tar_dump = tarfile.TarFile("medialib-dump.tar", "w")
    tag_uniq_id: dict[TagUnique, int] = dict()

    sql_get_content = "SELECT * FROM content order by RANDOM() LIMIT 5"
    sql_get_tag_ids = "SELECT tag_id FROM content_tags_list WHERE content_id=%s"
    sql_get_tag = "SELECT * FROM tag WHERE ID=%s"
    sql_get_tag_aliases = "SELECT title FROM tag_alias WHERE tag_id=%s"

    connection = common.make_connection()
    cursor = connection.cursor()

    cursor.execute(sql_get_content, tuple())
    raw_content_data = cursor.fetchall()

    def tags_processing(tag_id, cursor) -> TagUnique:
        global tags_uniq
        global tag_documents

        cursor.execute(sql_get_tag, (tag_id,))
        tag_raw_data = cursor.fetchone()
        tag_uniq = TagUnique(tag_raw_data[1], tag_raw_data[2])
        if tag_uniq not in tags_uniq:
            parent_tag_uniq = None
            if tag_raw_data[3] is not None:
                parent_tag_uniq = tags_processing(tag_raw_data[3], cursor)
            cursor.execute(sql_get_tag_aliases, (tag_raw_data[0],))
            tag_aliases = set([raw_alias[0] for raw_alias in cursor.fetchall()])
            tag_document = TagDocument(
                title=tag_raw_data[1],
                category=tag_raw_data[2],
                aliases=tag_aliases,
                parent=parent_tag_uniq
            )
            tag_uniq_id[tag_uniq] = tag_id
            tag_document_io = io.StringIO()
            json.dump(tag_document.json_serializable(), tag_document_io)
            tag_document_path = pathlib.PurePath("tags/{}.json".format(tag_id))
            create_tar_file(tar_dump, tag_document_io, tag_document_path)
            tags_uniq.add(tag_uniq)
        return tag_uniq

    for content in raw_content_data:
        tags: set[TagUnique] = set()
        content_id = content[0]
        cursor.execute(sql_get_tag_ids, (content_id,))
        tag_ids = cursor.fetchall()
        for tag_id_wrapped in tag_ids:
            tags.add(tags_processing(tag_id_wrapped[0], cursor))
        content_document = ContentDocument(
            file_path=pathlib.Path(content[1]),
            title=content[2],
            content_type=content[3],
            description=content[4],
            addition_date=content[5],
            origin=content[6],
            origin_content_id=content[7],
            is_hidden=content[8],
            tags=tags
        )

        content_document_io = io.StringIO()
        json.dump(content_document.json_serializable(), content_document_io)
        content_document_path = pathlib.PurePath("content-metadata/{}.json".format(content_id))
        create_tar_file(tar_dump, content_document_io, content_document_path)
    tag_uniq_id_io = io.StringIO()
    tag_uniq_id_serialisable = []
    for elem in tag_uniq_id:
        _tag_uniq = dataclasses.astuple(elem)
        tag_uniq_id_serialisable.append((_tag_uniq[0], _tag_uniq[1], tag_uniq_id[elem]))
    json.dump(tag_uniq_id_serialisable, tag_uniq_id_io)
    tag_uniq_id_filepath = pathlib.PurePath("tag_uniq_id.json")
    create_tar_file(tar_dump, tag_uniq_id_io, tag_uniq_id_filepath)

    checksums_io = io.StringIO()
    for checksum in checksums:
        checksums_io.write("{} {}\n".format(hex(checksum[0]).replace("0x", ""), checksum[1]))
    checksums_filepath = pathlib.PurePath("checksums-crc64")
    create_tar_file(tar_dump, checksums_io, checksums_filepath, write_crc=False)

    tar_dump.close()
    connection.close()


if __name__ == "__main__":
    main()
