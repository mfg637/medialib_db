from psycopg2.extensions import connection as psycopg2_connection
from psycopg2.extensions import cursor as psycopg2_cursor
from collections.abc import Callable

import logging
import psycopg2.errors

import psycopg2

try:
    from . import common
except ImportError:
    import common

logger = logging.getLogger(__name__)


def _request(request_body: Callable, *args, connection: psycopg2_connection):
    """
    Executes a given request function using a database cursor
    from the provided psycopg2 connection.

    Args:
        request_body (Callable):
            A function that takes a psycopg2 cursor and additional arguments,
            and performs a database operation.
        *args: Additional arguments to pass to the request_body function.
        connection (psycopg2_connection):
            An active psycopg2 database connection
            from which a cursor will be created.

    Returns:
        Any: The result returned by the request_body function.

    Raises:
        Any exception raised by the request_body function
        or database operations.
    """
    cursor: psycopg2_cursor = connection.cursor()
    return request_body(cursor, *args)


def _check_tag_exists(
    cursor: psycopg2_cursor, tag_name: str, tag_category: str
) -> tuple[int,] | None:
    """
    Checks if a tag with the given name and category exists in the database.

    This function first searches for a tag in the 'tag' table matching
    the provided tag name (with underscores replaced by spaces) and category.
    If not found, it attempts to find the tag by looking up an alias
    in the 'tag_alias' table.

    Args:
        cursor (psycopg2_cursor): The database cursor to execute queries.
        tag_name (str): The name of the tag to check.
        tag_category (str): The category of the tag.

    Returns:
        tuple[int,] | None: The ID of the tag as a tuple if found,
            otherwise None.
    """
    sql_select_tag_query = "SELECT ID FROM tag WHERE title=%s and category=%s"
    _tag_name = tag_name.replace("_", " ")
    logger.debug(
        'query="{}" title="{}" category="{}"'.format(
            sql_select_tag_query, _tag_name, tag_category
        )
    )
    cursor.execute(sql_select_tag_query, (_tag_name, tag_category))
    id: tuple[int,] | None = cursor.fetchone()
    if id is None:
        sql_get_id_of_alias = (
            "SELECT ID FROM tag WHERE id = "
            "(SELECT tag_id FROM tag_alias WHERE tag_alias.title = %s) "
            "and category=%s"
        )
        cursor.execute(sql_get_id_of_alias, (_tag_name, tag_category))
        id = cursor.fetchone()
    return id


def check_tag_exists(
    tag_name: str, tag_category: str, connection: psycopg2_connection
) -> tuple[int,] | None:
    """
    Checks if a tag with the specified name exists in the given category
    within the database.

    Args:
        tag_name (str): The name of the tag to check.
        tag_category (str): The category of the tag.
        connection (psycopg2_connection): The database connection object.

    Returns:
        tuple[int,] | None: A tuple containing the tag ID if the tag exists,
            or None if it does not.

    Raises:
        Any exceptions raised by the underlying database request function.
    """
    return _request(
        _check_tag_exists, tag_name, tag_category, connection=connection
    )


def insert_new_tag(
    tag_name: str,
    tag_category: str,
    tag_alias: str | None,
    connection: psycopg2_connection,
) -> int:
    """
    Inserts a new tag into the database and returns its ID.

    If the tag already exists (based on a unique constraint), retrieves and
    returns the existing tag's ID. Also inserts an alias for the tag. Handles
    special formatting for character and artist categories. If an alias
    insertion fails due to integrity constraints, attempts to resolve conflicts
    by updating or removing duplicate tags.

    Args:
        tag_name (str): The name of the tag to insert.
        tag_category (str): The category of the tag
            (e.g., "character", "artist", "content").
        tag_alias (str | None): An optional alias for the tag.
            If None, a default alias is generated.
        connection (psycopg2_connection):
            An active psycopg2 database connection.

    Returns:
        int: The ID of the inserted or existing tag.

    Raises:
        Exception:
            Propagates database exceptions if conflicts cannot be resolved.
    """
    cursor = connection.cursor()
    if tag_alias is None:
        tag_alias = tag_name
        if tag_category == "character":
            tag_alias = "character:{}".format(tag_name)
        elif tag_category == "artist":
            tag_alias = "artist:{}".format(tag_name)
    _tag_name = tag_name.replace("_", " ")
    sql_insert_tag_query = (
        "INSERT INTO tag (id, title, category) "
        "VALUES (DEFAULT, %s, %s) RETURNING id"
    )
    try:
        cursor.execute(sql_insert_tag_query, (_tag_name, tag_category))
    except psycopg2.errors.UniqueViolation as e:
        connection.rollback()
        return common.get_value_or_fail(
            _check_tag_exists(cursor, _tag_name, tag_category), e
        )
    tag_id: int = common.get_value_or_fail(
        cursor.fetchone(),
        f"Tag ID not found for ({tag_name}, {tag_category}, {tag_alias})",
    )
    logger.debug("_insert_new_tag last row id={}".format(tag_id))
    sql_insert_alias_query = (
        "INSERT INTO tag_alias (tag_id, title) VALUES (%s, %s)"
    )
    logger.debug(
        "tag {} ({}) alias insert: {} ".format(
            _tag_name, tag_category, tag_alias
        )
    )
    try:
        cursor.execute(sql_insert_alias_query, (tag_id, tag_alias))
    except psycopg2.IntegrityError as e:
        cursor.connection.rollback()
        get_tag_info = (
            "SELECT tag.category, ID from tag "
            "where id=(SELECT tag_id from tag_alias where tag_alias.title=%s)"
        )
        cursor.execute(get_tag_info, (_tag_name,))
        response = cursor.fetchone()
        remove_duplicate = "DELETE FROM tag where id = %s"
        cursor.execute(remove_duplicate, (tag_id,))
        _category = common.get_value_or_fail(response, e, 0)
        tag_id = common.get_value_or_fail(response, e, 1)
        if _category == "content":
            update_category = "UPDATE tag set category = %s where id = %s"
            cursor.execute(update_category, (tag_category, tag_id))
            return tag_id
        elif tag_category == "content":
            return tag_id
        else:
            raise e
    if " " in tag_alias:
        cursor.execute(
            sql_insert_alias_query, (tag_id, tag_alias.replace(" ", "_"))
        )
    elif "_" in tag_alias:
        cursor.execute(
            sql_insert_alias_query, (tag_id, tag_alias.replace("_", " "))
        )
    connection.commit()
    return tag_id


def _get_category_of_tag(
    cursor: psycopg2_cursor, tag_name: str
) -> set[str] | None:
    """
    Retrieve the category or categories associated with a given tag name
    from the database.

    This function first attempts to find the category of the tag by its title
    in the 'tag' table. If not found, it attempts to find the category by
    looking up the tag's alias in the 'tag_alias' table. The function returns
    a set of category names if found, or None if no category is associated
    with the tag.

    Args:
        cursor (psycopg2_cursor): A database cursor for executing SQL queries.
        tag_name (str): The name of the tag to look up. Underscores in the
            tag name are replaced with spaces.

    Returns:
        set[str] | None: A set of category names if found, otherwise None.
    """
    get_tag_category_query = "SELECT category FROM tag WHERE title = %s;"
    cursor.execute(get_tag_category_query, (tag_name.replace("_", " "),))
    raw_categories = cursor.fetchall()
    if len(raw_categories) == 0 or raw_categories is None:
        get_tag_category_by_alias = (
            "SELECT category FROM tag where id = "
            "(SELECT tag_id FROM tag_alias where title = %s)"
        )
        cursor.execute(
            get_tag_category_by_alias, (tag_name.replace("_", " "),)
        )
        raw_categories = cursor.fetchall()
    if raw_categories is not None:
        categories_list = set()
        for raw_category in raw_categories:
            categories_list.add(raw_category[0])
        if len(categories_list):
            return categories_list
    return None


def get_category_of_tag(tag_name: str, connection: psycopg2_connection) -> set[str] | None:
    """
    Retrieve the category associated with a given tag name from the database.

    Args:
        tag_name (str): The name of the tag whose category is to be retrieved.
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.

    Returns:
        The category associated with the specified tag name.

    Raises:
        Any exceptions raised
        by the underlying _request or _get_category_of_tag functions.
    """
    return _request(_get_category_of_tag, tag_name, connection=connection)


def wildcard_tag_search(
    wildcard_string: str, connection: psycopg2_connection
) -> list[tuple[int, str]]:
    """
    Performs a wildcard search on the 'title' column of the 'tag_alias' table.

    Args:
        wildcard_string (str):
            The search pattern, where '*' is treated as a wildcard character.
        connection (psycopg2_connection):
            An active psycopg2 database connection.

    Returns:
        list: A list of tuples containing the rows from 'tag_alias'
            that match the wildcard pattern:
        [(id: int, alias:  str),…]

    Notes:
        - The function replaces '*' in the input string with '%'
            to conform to SQL LIKE syntax.
        - The search is case-sensitive
            unless the database collation specifies otherwise.
        - The returned tuples correspond to the columns
            in the 'tag_alias' table.
    """
    sql_tag_alias_search = "SELECT * FROM tag_alias WHERE title like %s"
    cursor = connection.cursor()
    sql_wildcard_string = wildcard_string.replace("*", "%")
    cursor.execute(sql_tag_alias_search, (sql_wildcard_string,))
    result = cursor.fetchall()
    cursor.close()
    return result


def get_tag_info_by_tag_id(
    tag_id: int, connection: psycopg2_connection
) -> tuple[int, str, str, int | None] | None:
    """
    Retrieve tag information from the database by tag ID.
    Args:
        tag_id (int): The unique identifier of the tag to retrieve.
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.
    Returns:
        tuple or None: A tuple containing the tag's information
        (id, title, category, parent) if found,
        or None if no tag with the specified ID exists.
    """
    sql_get_tag_id = "SELECT * FROM tag where ID=%s"
    cursor = connection.cursor()
    cursor.execute(sql_get_tag_id, (tag_id,))
    result = cursor.fetchone()
    cursor.close()
    return result


def get_tag_aliases(tag_id: int, connection: psycopg2_connection) -> list[str]:
    """
    Retrieve all alias titles associated with a given tag ID from the database.

    Args:
        tag_id (int): The ID of the tag for which to fetch aliases.
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.

    Returns:
        list[str]: A list of alias titles (strings)
            associated with the specified tag ID.
    """
    sql_get_tag_id = "SELECT title FROM tag_alias where tag_id=%s"
    cursor = connection.cursor()
    cursor.execute(sql_get_tag_id, (tag_id,))
    raw_results = cursor.fetchall()
    result = []
    for raw_result in raw_results:
        result.append(raw_result[0])
    cursor.close()
    return result


def set_tag_properties(
    tag_id: int,
    tag_name: str,
    tag_category: str,
    connection: psycopg2_connection,
):
    """
    Updates the properties of a tag in the database.

    Args:
        tag_id (int): The unique identifier of the tag to update.
        tag_name (str): The new name/title to assign to the tag.
        tag_category (str): The new category to assign to the tag.
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.

    Raises:
        psycopg2.DatabaseError:
            If a database error occurs during the update operation.

    Note:
        The function commits the transaction after updating the tag.
    """
    sql_set_properties = (
        "UPDATE tag SET title = %s, category = %s where id = %s"
    )
    cursor = connection.cursor()
    cursor.execute(sql_set_properties, (tag_name, tag_category, tag_id))
    cursor.close()
    connection.commit()


def add_alias(tag_id: int, alias_name: str, connection: psycopg2_connection):
    """
    Adds an alias for a given tag in the database.

    Inserts a new record into the 'tag_alias' table
    with the specified tag ID and alias name.

    Args:
        tag_id (int): The ID of the tag to which the alias will be added.
        alias_name (str): The alias name to associate with the tag.
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.

    Raises:
        psycopg2.DatabaseError:
            If a database error occurs during the operation.
    """
    sql_set_properties = (
        "INSERT INTO tag_alias (tag_id, title) VALUES (%s, %s)"
    )
    cursor = connection.cursor()
    cursor.execute(sql_set_properties, (tag_id, alias_name))
    cursor.close()
    connection.commit()


def delete_alias(
    tag_id: int, alias_name: str, connection: psycopg2_connection
):
    """
    Deletes an alias for a specific tag
    from the 'tag_alias' table in the database.

    Args:
        tag_id (int): The ID of the tag whose alias is to be deleted.
        alias_name (str): The name of the alias to be deleted.
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.

    Raises:
        psycopg2.DatabaseError:
            If a database error occurs during the operation.

    Note:
        The function commits the transaction after deleting the alias.
    """
    sql_set_properties = (
        "DELETE FROM tag_alias WHERE tag_id = %s AND title = %s"
    )
    cursor = connection.cursor()
    cursor.execute(sql_set_properties, (tag_id, alias_name))
    cursor.close()
    connection.commit()


def get_content_ids_by_tag_id(
    tag_id: int, connection: psycopg2_connection
) -> list[int]:
    """
    Retrieve a list of content IDs
    associated with a given tag ID from the database.

    Args:
        tag_id (int): The ID of the tag to search for.
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.

    Returns:
        list[int]: A list of content IDs linked to the specified tag ID.
    """
    sql_get_content_ids = (
        "SELECT content_id FROM content_tags_list where tag_id = %s"
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_content_ids, (tag_id,))
    raw_results = cursor.fetchall()
    result = []
    for raw_result in raw_results:
        result.append(raw_result[0])
    cursor.close()
    return result


def merge_tags(
    first_tag_id: int, second_tag_id: int, connection: psycopg2_connection
):
    """
    Merges two tags in the database by transferring all content associations,
    aliases, and parent relationships from the first tag to the second tag,
    and then deletes the first tag.

    Args:
        first_tag_id (int): The ID of the tag to be merged and removed.
        second_tag_id (int): The ID of the tag to merge into and retain.
        connection (psycopg2_connection):
            An active psycopg2 database connection.

    Behavior:
        - Moves all content associations from the first tag to the second tag,
            avoiding duplicates.
        - Removes duplicate content-to-tag associations.
        - Updates all tag aliases pointing to the first tag to point
            to the second tag.
        - Checks and resets parent relationships to avoid circular references
            between the two tags.
        - Deletes the first tag from the database.
        - Commits all changes to the database.

    Raises:
        Any exceptions raised by the underlying database operations.
    """
    first_content_list = set(
        get_content_ids_by_tag_id(first_tag_id, connection)
    )
    second_content_list = set(
        get_content_ids_by_tag_id(second_tag_id, connection)
    )
    reset_ids_set = first_content_list - second_content_list
    remove_first_id_set = first_content_list.intersection(second_content_list)
    sql_reset_ids = (
        "UPDATE content_tags_list SET tag_id = %s "
        "WHERE content_id = %s AND tag_id = %s"
    )
    cursor = connection.cursor()
    logger.info("replace {} tag ID's".format(len(reset_ids_set)))
    for content_id in reset_ids_set:
        cursor.execute(
            sql_reset_ids, (second_tag_id, content_id, first_tag_id)
        )
    sql_delete_connection = (
        "DELETE FROM content_tags_list WHERE content_id = %s AND tag_id = %s"
    )
    logger.info(
        "delete {} content to tag connections".format(len(remove_first_id_set))
    )
    for content_id in remove_first_id_set:
        cursor.execute(sql_delete_connection, (content_id, first_tag_id))
    logger.info("reset aliases")
    sql_reset_alias = "UPDATE tag_alias SET tag_id = %s WHERE tag_id = %s"
    cursor.execute(sql_reset_alias, (second_tag_id, first_tag_id))
    logger.info("check parents")
    sql_check_parent_of_tag = "SELECT parent FROM tag where id = %s"
    sql_reset_parent = "UPDATE tag SET parent = NULL where id = %s"
    cursor.execute(sql_check_parent_of_tag, (second_tag_id,))
    parent = common.get_value_or_fail(
        cursor.fetchone(), f"Tag with ID {second_tag_id} not found"
    )
    if parent == first_tag_id:
        cursor.execute(sql_reset_parent, (second_tag_id,))
    cursor.execute(sql_check_parent_of_tag, (first_tag_id,))
    parent = common.get_value_or_fail(
        cursor.fetchone(), f"Tag with ID {first_tag_id} not found"
    )
    if parent == second_tag_id:
        cursor.execute(sql_reset_parent, (first_tag_id,))
    logger.info("REMOVING first tag")
    sql_delete_first_tag = "DELETE FROM tag WHERE id = %s"
    cursor.execute(sql_delete_first_tag, (first_tag_id,))
    cursor.close()
    connection.commit()


def get_tag_id_by_alias(alias: str, connection: psycopg2_connection):
    """
    Retrieve the tag ID associated with a given alias from the database.

    Args:
        alias (str): The alias of the tag to search for.
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.

    Returns:
        int or None: The tag ID if found, otherwise None.

    Logs:
        Logs an info message if the alias is not found in the database.
    """
    cursor = connection.cursor()
    sql_template = "SELECT tag_id FROM tag_alias WHERE title=%s"
    cursor.execute(sql_template, (alias,))
    result = cursor.fetchone()
    if result is not None:
        result = result[0]
    else:
        logger.info(f"not found tag by alias: {alias}")
    cursor.close()
    return result
