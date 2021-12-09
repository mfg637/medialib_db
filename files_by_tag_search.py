import pathlib
import enum

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
    ORDERING_BY.RANDOM: "RAND()"
}


def get_files_with_every_tag(*tags: str, limit: int = None, offset: int = None, order_by: ORDERING_BY = ORDERING_BY.NONE):
    get_image_id_by_tag_code_block = ("id in (SELECT content_id from content_tags_list where tag_id = "
                                      "(SELECT tag_id from tag_alias where title=%s))")
    base_sql_code_block = "SELECT file_path from content where "
    result_sql_block = base_sql_code_block
    for i in range(len(tags)):
        result_sql_block += get_image_id_by_tag_code_block
        if i + 1 < len(tags):
            result_sql_block += " AND "
    if order_by != ORDERING_BY.NONE:
        result_sql_block += " ORDER BY {}".format(ordering_constants[order_by])
    if limit is not None:
        result_sql_block += " LIMIT {}".format(limit)
        if offset is not None:
            result_sql_block += " OFFSET {}".format(offset)
    common.open_connection_if_not_opened()
    cursor = common.connection.cursor()
    cursor.execute(result_sql_block, tags)
    list_files = list()
    file_path = cursor.fetchone()
    while file_path is not None:
        list_files.append(pathlib.Path(file_path[0]))
        file_path = cursor.fetchone()
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
