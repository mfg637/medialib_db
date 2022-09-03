import dataclasses
import datetime
import pathlib
import json
from typing import Any

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
tag_documents = []


def main():
    global tag_documents

    content_documents = []

    sql_get_content = "SELECT * FROM content order by RAND() LIMIT 5"
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
            tag_documents.append(
                TagDocument(
                    title=tag_raw_data[1],
                    category=tag_raw_data[2],
                    aliases=tag_aliases,
                    parent=parent_tag_uniq
                )
            )
            tags_uniq.add(tag_uniq)
        return tag_uniq

    for content in raw_content_data:
        tags: set[TagUnique] = set()
        content_id = content[0]
        cursor.execute(sql_get_tag_ids, (content_id,))
        tag_ids = cursor.fetchall()
        for tag_id_wrapped in tag_ids:
            tags.add(tags_processing(tag_id_wrapped[0], cursor))
        content_documents.append(ContentDocument(
            file_path=pathlib.Path(content[1]),
            title=content[2],
            content_type=content[3],
            description=content[4],
            addition_date=content[5],
            origin=content[6],
            origin_content_id=content[7],
            is_hidden=content[8],
            tags=tags
        ))
    content_documents_serializable = [
        content_document.json_serializable() for content_document in content_documents
    ]
    tag_documents_serializable = [
        tag_document.json_serializable() for tag_document in tag_documents
    ]
    print(json.dumps(content_documents_serializable))
    print()
    print(json.dumps(tag_documents_serializable))

    connection.close()


if __name__ == "__main__":
    main()
