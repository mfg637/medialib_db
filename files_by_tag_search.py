import pathlib
import enum
import random
import typing

try:
    from . import config
except ImportError:
    import config

try:
    from . import common
except ImportError:
    import common


class ORDERING_BY(enum.Enum):
    DATE_DECREASING = enum.auto()
    DATE_INCREASING = enum.auto()
    NONE = enum.auto()
    RANDOM = enum.auto()


ordering_constants = {
    ORDERING_BY.DATE_DECREASING: "addition_date DESC",
    ORDERING_BY.DATE_INCREASING: "addition_date",
    ORDERING_BY.RANDOM: "RANDOM()"
}


class HIDDEN_FILTERING(enum.Enum):
    FILTER = enum.auto()
    SHOW = enum.auto()
    ONLY_HIDDEN = enum.auto()


hidden_filtering_constants = {
    HIDDEN_FILTERING.SHOW: "",
    HIDDEN_FILTERING.FILTER: " AND hidden=FALSE",
    HIDDEN_FILTERING.ONLY_HIDDEN: " AND hidden=TRUE"
}


def _requests_fabric(
        *tags_groups: dict[str, typing.Any],
        limit: int = None,
        offset: int = None,
        order_by: ORDERING_BY = ORDERING_BY.NONE,
        filter_hidden: HIDDEN_FILTERING = HIDDEN_FILTERING.FILTER,
        base_sql_block,
        cursor
        ):
    get_image_id_by_tag_code_block = "id in (SELECT content_id from content_tags_list where tag_id in ({}))"
    get_image_id_by_not_tag_code_block = "id not in (SELECT content_id from content_tags_list where tag_id in ({}))"
    result_sql_block = base_sql_block

    tag_ids = list()
    tags_count = list()

    sql_get_tag_ids = "SELECT * FROM get_tags_ids(%s)"
    sql_get_parent_ids = "SELECT * FROM get_parent_tag_ids(%s)"
    for tags_group in tags_groups:
        raw_tag_ids = []
        for tag in tags_group["tags"]:
            if type(tag) is str:
                cursor.execute(sql_get_tag_ids, (tag,))
            elif type(tag) is int:
                cursor.execute(sql_get_parent_ids, (tag,))
            data = cursor.fetchall()
            raw_tag_ids.extend(data)
        tags_count.append(len(raw_tag_ids))
        _tag_ids = set()
        for raw_id in raw_tag_ids:
            _tag_ids.add(raw_id[0])
        tag_ids.extend(_tag_ids)

    tags_set_lists = list()
    for i, val in enumerate(tags_count):
        if tags_groups[i]["not"]: # not tag
            result_sql_block += get_image_id_by_not_tag_code_block
        else:
            result_sql_block += get_image_id_by_tag_code_block
        tag_set_list = "%s"
        for j in range(1, val):
            tag_set_list += ", %s"
        tags_set_lists.append(tag_set_list)
        if i + 1 < len(tags_count):
            result_sql_block += " AND "

    result_sql_block = result_sql_block.format(*tags_set_lists)

    if filter_hidden != HIDDEN_FILTERING.SHOW:
        result_sql_block += hidden_filtering_constants[filter_hidden]

    if order_by != ORDERING_BY.NONE:
        result_sql_block += " ORDER BY {}".format(ordering_constants[order_by])
    if limit is not None:
        result_sql_block += " LIMIT {}".format(limit)
        if offset is not None:
            result_sql_block += " OFFSET {}".format(offset)
    print(result_sql_block, tag_ids, type(tag_ids[0]))
    cursor.execute(result_sql_block, tag_ids)


def get_media_by_tags(
        *tags: dict[str, typing.Any],
        limit: int = None,
        offset: int = None,
        order_by: ORDERING_BY = ORDERING_BY.NONE,
        filter_hidden: HIDDEN_FILTERING = HIDDEN_FILTERING.FILTER,
    ):
    connection = common.make_connection()
    cursor = connection.cursor()
    base_sql_code_block = "SELECT ID, file_path, content_type, title from content where "
    _requests_fabric(
        *tags,
        limit=limit,
        offset=offset,
        order_by=order_by,
        base_sql_block=base_sql_code_block,
        cursor=cursor,
        filter_hidden=filter_hidden
    )

    list_files = list()
    file_path = cursor.fetchone()
    while file_path is not None:
        list_files.append(file_path)
        file_path = cursor.fetchone()
    if order_by == ORDERING_BY.RANDOM:
        random.shuffle(list_files)
    connection.close()
    return list_files


def count_files_with_every_tag(*tags: dict[str, typing.Any], filter_hidden: HIDDEN_FILTERING = HIDDEN_FILTERING.FILTER):
    base_sql_code_block = "SELECT COUNT(*) from content where "
    connection = common.make_connection()
    cursor = connection.cursor()
    get_image_id_by_tag_code_block = ("id in (SELECT content_id from content_tags_list where tag_id = "
                                      "(SELECT tag_id from tag_alias where title=%s))")

    _requests_fabric(*tags, base_sql_block=base_sql_code_block, cursor=cursor, filter_hidden=filter_hidden)

    result = cursor.fetchone()[0]
    connection.close()
    return result
