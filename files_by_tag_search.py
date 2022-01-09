import pathlib
import enum
import random

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
    ORDERING_BY.RANDOM: "id = ceil(rand() * (select count(*) from content))"
}


def get_files_with_every_tag(*tags: str, limit: int = None, offset: int = None, order_by: ORDERING_BY = ORDERING_BY.NONE):
    get_image_id_by_tag_code_block = "id in (SELECT content_id from content_tags_list where tag_id = %s)"
    base_sql_code_block = "SELECT file_path from content where "
    result_sql_block = base_sql_code_block
    
    tag_ids = list()

    common.open_connection_if_not_opened()
    cursor = common.connection.cursor()
    for tag in tags:
        cursor.callproc('get_tags_ids', (tag,))
        # implied that stored only one result
        for result in cursor.stored_results():
            tag_ids.extend([i[0] for i in result.fetchall()])

    for i in range(len(tag_ids)):
        result_sql_block += get_image_id_by_tag_code_block
        if i + 1 < len(tags):
            result_sql_block += " AND "
    if order_by != ORDERING_BY.NONE:
        result_sql_block += " ORDER BY {}".format(ordering_constants[order_by])
    if limit is not None:
        result_sql_block += " LIMIT {}".format(limit)
        if offset is not None:
            result_sql_block += " OFFSET {}".format(offset)
    print(result_sql_block, tag_ids, type(tag_ids[0]))
    cursor.execute(result_sql_block, tag_ids)
    list_files = list()
    file_path = cursor.fetchone()
    while file_path is not None:
        list_files.append(pathlib.Path(file_path[0]))
        file_path = cursor.fetchone()
    if order_by == ORDERING_BY.RANDOM:
        random.shuffle(list_files)
    common.close_connection_if_not_closed()
    return list_files

def count_files_with_every_tag(*tags: str):
    get_image_id_by_tag_code_block = ("id in (SELECT content_id from content_tags_list where tag_id = "
                                      "(SELECT tag_id from tag_alias where title=%s))")
    base_sql_code_block = "SELECT COUNT(*) from content where "
    result_sql_block = base_sql_code_block
    for i in range(len(tags)):
        result_sql_block += get_image_id_by_tag_code_block
        if i + 1 < len(tags):
            result_sql_block += " AND "
    common.open_connection_if_not_opened()
    cursor = common.connection.cursor()
    cursor.execute(result_sql_block, tags)
    result = cursor.fetchone()[0]
    common.close_connection_if_not_closed()
    return result
