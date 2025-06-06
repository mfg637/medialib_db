from psycopg2.extensions import connection as psycopg2_connection
from psycopg2.extensions import cursor as psycopg2_cursor

import logging
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

logger = logging.getLogger(__name__)


class ORDERING_BY(enum.Enum):
    DATE_DECREASING = enum.auto()
    DATE_INCREASING = enum.auto()
    NONE = enum.auto()
    RANDOM = enum.auto()


ordering_constants = {
    ORDERING_BY.DATE_DECREASING: "addition_date DESC",
    ORDERING_BY.DATE_INCREASING: "addition_date",
    ORDERING_BY.RANDOM: "RANDOM()",
}


class HIDDEN_FILTERING(enum.Enum):
    FILTER = enum.auto()
    SHOW = enum.auto()
    ONLY_HIDDEN = enum.auto()


hidden_filtering_constants = {
    HIDDEN_FILTERING.SHOW: "",
    HIDDEN_FILTERING.FILTER: "hidden=FALSE",
    HIDDEN_FILTERING.ONLY_HIDDEN: "hidden=TRUE",
}


def _requests_fabric(
    *tags_groups: dict[str, typing.Any] | None,
    limit: int | None = None,
    offset: int | None = None,
    order_by: ORDERING_BY = ORDERING_BY.NONE,
    filter_hidden: HIDDEN_FILTERING = HIDDEN_FILTERING.FILTER,
    base_sql_block: str,
    cursor: psycopg2_cursor
):
    """
    Constructs and executes a dynamic SQL query
    to search for media files by tag groups,
    with support for tag inclusion/exclusion,
    ordering, pagination, and hidden filtering.

    Args:
        *tags_groups (dict[str, typing.Any] | None):
            Variable number of tag group dictionaries.
            Each dictionary should contain a "tags" key
            (list of tag names or IDs)
            and a "not" key (bool) indicating exclusion.
        limit (int | None, optional):
            Maximum number of results to return. Defaults to None (no limit).
        offset (int | None, optional):
            Number of results to skip (for pagination). Defaults to None.
        order_by (ORDERING_BY, optional):
            Enum value specifying the ordering of results.
            Defaults to ORDERING_BY.NONE.
        filter_hidden (HIDDEN_FILTERING, optional):
            Enum value specifying how to filter hidden items.
            Defaults to HIDDEN_FILTERING.FILTER.
        base_sql_block (str): The base SQL query string to build upon.
        cursor: Database cursor object used to execute SQL queries.

    Raises:
        Exception: If any tag group or current group is None.

    Side Effects:
        Executes the constructed SQL query using the provided cursor.

    Logs:
        Logs the resulting SQL query and tag IDs at debug level.
    """
    get_image_id_by_tag_code_block = (
        "id in (SELECT content_id "
        "from content_tags_list where tag_id in ({}))"
    )
    get_image_id_by_not_tag_code_block = (
        "id not in "
        "(SELECT content_id from content_tags_list "
        "where tag_id in ({}))"
    )
    result_sql_block = base_sql_block
    tag_ids = list()

    len_of_groups = len(tags_groups)
    if len_of_groups:
        tags_count = list()

        sql_get_tag_ids = "SELECT * FROM get_tags_ids(%s)"
        sql_get_parent_ids = "SELECT * FROM get_parent_tag_ids(%s)"
        for tags_group in tags_groups:
            if tags_group is None:
                raise Exception(
                    "Unexpected None: tags_group should not be None"
                )
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
            current_groups = tags_groups[i]
            if current_groups is not None:
                if current_groups["not"]:  # not tag
                    result_sql_block += get_image_id_by_not_tag_code_block
                else:
                    result_sql_block += get_image_id_by_tag_code_block
                tag_set_list = "%s"
                for j in range(1, val):
                    tag_set_list += ", %s"
                tags_set_lists.append(tag_set_list)
                if i + 1 < len(tags_count):
                    result_sql_block += " AND "
            else:
                raise Exception(
                    "Unexpected None: current_groups should not be None"
                )

        result_sql_block = result_sql_block.format(*tags_set_lists)

    if filter_hidden != HIDDEN_FILTERING.SHOW:
        if len_of_groups:
            result_sql_block += " AND "
        result_sql_block += hidden_filtering_constants[filter_hidden]

    if order_by != ORDERING_BY.NONE:
        result_sql_block += " ORDER BY {}".format(ordering_constants[order_by])
    if limit is not None:
        result_sql_block += " LIMIT {}".format(limit)
        if offset is not None:
            result_sql_block += " OFFSET {}".format(offset)
    logger.debug("Resulting SQL: %s, tag_ids = %s", result_sql_block, tag_ids)
    cursor.execute(result_sql_block, tag_ids)


def get_media_by_tags(
    connection: psycopg2_connection,
    *tags: dict[str, typing.Any],
    limit: int | None = None,
    offset: int | None = None,
    order_by: ORDERING_BY = ORDERING_BY.NONE,
    filter_hidden: HIDDEN_FILTERING = HIDDEN_FILTERING.FILTER
):
    """
    Retrieve media entries from the database
    that match the specified tags and filters.

    Args:
        connection: A database connection object.
        *tags (dict[str, typing.Any]):
            One or more dictionaries specifying tag filters to apply.
        limit (int | None, optional):
            Maximum number of results to return. Defaults to None (no limit).
        offset (int | None, optional):
            Number of results to skip before returning results.
            Defaults to None.
        order_by (ORDERING_BY, optional):
            Specifies the ordering of the results.
            Defaults to ORDERING_BY.NONE.
        filter_hidden (HIDDEN_FILTERING, optional):
            Determines whether to filter out hidden entries.
            Defaults to HIDDEN_FILTERING.FILTER.

    Returns:
        list: A list of tuples,
            each containing (ID, file_path, content_type, title)
            for matching media entries.
    """
    cursor = connection.cursor()
    base_sql_code_block = (
        "SELECT ID, file_path, content_type, title from content where "
    )
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
    cursor.close()

    return list_files


def get_all_media(
    connection: psycopg2_connection,
    limit: int | None = None,
    offset: int | None = None,
    order_by: ORDERING_BY = ORDERING_BY.NONE,
    filter_hidden: HIDDEN_FILTERING = HIDDEN_FILTERING.FILTER,
) -> list[tuple[int, str, str, str]]:
    """
    Retrieve a list of media files from the database with optional filtering,
    ordering, and pagination.

    Args:
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.
        limit (int | None, optional):
            Maximum number of records to retrieve. Defaults to None (no limit).
        offset (int | None, optional):
            Number of records to skip before starting to return records.
            Used for page navigation.
            Defaults to None.
        order_by (ORDERING_BY, optional):
            Specifies the ordering of the results.
            Defaults to ORDERING_BY.NONE.
        filter_hidden (HIDDEN_FILTERING, optional):
            Determines whether to filter out hidden files.
            Defaults to HIDDEN_FILTERING.FILTER.

    Returns:
        list: A list of tuples, each containing
        (ID, file_path, content_type, title) for each media file.

    Notes:
        - If order_by is ORDERING_BY.RANDOM,
            the resulting list is shuffled randomly after retrieval.
        - The function relies on the `_requests_fabric` helper
            to execute the appropriate SQL query.
    """
    cursor = connection.cursor()
    base_sql_code_block = (
        "SELECT ID, file_path, content_type, title from content where "
    )
    _requests_fabric(
        limit=limit,
        offset=offset,
        order_by=order_by,
        base_sql_block=base_sql_code_block,
        cursor=cursor,
        filter_hidden=filter_hidden,
    )

    list_files = list()
    file_path = cursor.fetchone()
    while file_path is not None:
        list_files.append(file_path)
        file_path = cursor.fetchone()
    if order_by == ORDERING_BY.RANDOM:
        random.shuffle(list_files)
    cursor.close()

    return list_files


def count_media_by_tags(
    connection: psycopg2_connection,
    *tags: dict[str, typing.Any],
    filter_hidden: HIDDEN_FILTERING = HIDDEN_FILTERING.FILTER
):
    """
    Counts the number of media entries in the database
    that match the given tags.

    Args:
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.
        *tags (dict[str, typing.Any]):
            One or more dictionaries specifying tag filters
            to apply to the query.
        filter_hidden (HIDDEN_FILTERING, optional):
            Determines whether to filter out hidden media entries.
            Defaults to HIDDEN_FILTERING.FILTER.

    Returns:
        int: The count of media entries
            matching the specified tags and filter criteria.

    Raises:
        Exception: If the query does not return any result.
    """
    base_sql_code_block = "SELECT COUNT(*) from content where "
    cursor = connection.cursor()

    _requests_fabric(
        *tags,
        base_sql_block=base_sql_code_block,
        cursor=cursor,
        filter_hidden=filter_hidden
    )

    result = cursor.fetchone()
    cursor.close()
    if result is not None:
        return result[0]
    else:
        raise Exception(
            (
                "No result returned from count_media_by_tags query "
                "(result is None)"
            )
        )


def get_total_count(
    connection: psycopg2_connection,
    filter_hidden: HIDDEN_FILTERING = HIDDEN_FILTERING.FILTER,
):
    cursor = connection.cursor()
    base_sql_code_block = "SELECT count(*) from content where "
    _requests_fabric(
        base_sql_block=base_sql_code_block,
        cursor=cursor,
        filter_hidden=filter_hidden,
    )
    item_count = cursor.fetchone()
    cursor.close()
    if item_count is not None:
        return item_count[0]
    else:
        raise Exception(
            (
                "No result returned from get_total_count query "
                "(result is None)"
            )
        )
