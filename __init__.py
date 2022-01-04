from . import files_by_tag_search
from . import common
from . import tags_indexer

def get_tag_name_by_alias(alias):
    common.open_connection_if_not_opened()
    cursor = common.connection.cursor()
    sql_template = "SELECT title FROM tag WHERE ID = (SELECT tag_id FROM tag_alias WHERE title=%s)"
    cursor.execute(sql_template, (alias,))
    result = cursor.fetchone()[0]
    common.close_connection_if_not_closed()
    return result
