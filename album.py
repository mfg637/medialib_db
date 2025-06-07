import pathlib
from psycopg2.extensions import connection as psycopg2_connection
from psycopg2.extensions import cursor as psycopg2_cursor
from .content import Content
from .origin import _get_primary_origin

import dataclasses


@dataclasses.dataclass(frozen=True)
class OrderedContent(Content):
    order: int


@dataclasses.dataclass(frozen=True)
class OptionallyOrderedContent(Content):
    order: int | None


def get_album_title(
    album_id: int, connection: psycopg2_connection
) -> tuple[str, str] | None:
    """
    Retrieve the album title and album artist title for a given album ID
    from the database.

    Args:
        album_id (int): The ID of the album to retrieve information for.
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.

    Returns:
        tuple: A tuple containing the album title and album artist title,
            or None if the album is not found.
    """
    sql_get_album_title = (
        "select (select title from tag where tag.id = album.set_tag_id), "
        "(select title from tag where tag.id = album.album_artist_tag_id) "
        "from album where ID = %s"
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_album_title, (album_id,))
    result = cursor.fetchone()
    cursor.close()
    return result


def get_album_content(
    album_id: int, connection: psycopg2_connection
) -> list[OrderedContent]:
    """
    Retrieve the content information for all items in a given album.

    Args:
        album_id (int): The ID of the album whose content is to be retrieved.
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.

    Returns:
        list[OrderedContent]: A list of OrderedContent objects, each
            representing a content item in the album, including its metadata.

    Raises:
        psycopg2.DatabaseError:
            If a database error occurs during the query execution.

    Note:
        The function fetches content items associated with the specified album,
        ordered by their position in the album.
    """
    sql_get_content_by_album_id = (
        "select content.ID, file_path, content_type, title, description, "
        '"order", addition_date, hidden '
        "from album_order join content on album_order.content_id = content.ID "
        'where album_id = %s order by album_order."order";'
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_content_by_album_id, (album_id,))
    raw_result = cursor.fetchall()
    result = []
    for raw_data in raw_result:
        content_id = raw_data[0]
        file_path = pathlib.Path(raw_data[1])
        content_type = raw_data[2]
        title = raw_data[3]
        description = raw_data[4]
        album_order = raw_data[5]
        addition_date = raw_data[6]
        hidden = raw_data[7]
        result.append(
            OrderedContent(
                content_id,
                file_path,
                content_type,
                addition_date,
                hidden,
                title,
                description,
                album_order,
            )
        )
    cursor.close()
    return result


def get_album_related_content(
    set_tag_id: int, artist_tag_id: int, connection: psycopg2_connection
) -> list[OptionallyOrderedContent]:
    """
    Fetches content items related to a specific album and artist
    from the database.

    Args:
        set_tag_id (int): The tag ID representing the album.
        artist_tag_id (int): The tag ID representing the artist.
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.

    Returns:
        list[OptionallyOrderedContentOrigin]: A list of content items
            associated with both the specified album and artist,
            each represented as an OptionallyOrderedContentOrigin object.
            The list includes content metadata such as file path,
            content type, title, description, album order, addition date,
            hidden status.

    Raises:
        psycopg2.DatabaseError:
            If a database error occurs during query execution.

    Note:
        The function performs a SQL query to retrieve content items that have
        both the specified album and artist tags. It joins the
        'content' and 'album_order' tables.
    """
    sql_get_content_by_album_id = (
        "select content.id, file_path, content_type, title, description, "
        'album_order."order", addition_date, hidden '
        "from content left outer join album_order "
        "on content.id = album_order.content_id "
        "where content.id in "
        "(SELECT content_id FROM content_tags_list WHERE tag_id = %s) "
        "and content.id in "
        "(SELECT content_id FROM content_tags_list WHERE tag_id = %s)"
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_content_by_album_id, (set_tag_id, artist_tag_id))
    raw_result = cursor.fetchall()
    result = []
    for raw_data in raw_result:
        content_id = raw_data[0]
        file_path = pathlib.Path(raw_data[1])
        content_type = raw_data[2]
        title = raw_data[3]
        description = raw_data[4]
        album_order = raw_data[5]
        addition_date = raw_data[6]
        hidden = raw_data[7]
        result.append(
            OptionallyOrderedContent(
                content_id,
                file_path,
                content_type,
                addition_date,
                hidden,
                title,
                description,
                album_order,
            )
        )
    cursor.close()
    return result


def get_album_id(
    set_tag_id: int, artist_tag_id: int, connection: psycopg2_connection
) -> int | None:
    """
    Retrieve the album ID from the database based on the given set tag ID
    and artist tag ID.

    Args:
        set_tag_id (int): The ID of the set tag to filter albums.
        artist_tag_id (int): The ID of the album artist tag to filter albums.
        connection (psycopg2_connection):
            An active psycopg2 database connection.

    Returns:
        int | None: The ID of the album if found, otherwise None.
    """
    sql_get_album_id = (
        "select id from album where "
        "set_tag_id = %s and album_artist_tag_id = %s"
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_album_id, (set_tag_id, artist_tag_id))
    result = cursor.fetchone()
    if result is not None:
        result = result[0]
    cursor.close()
    return result


def make_album(
    set_tag_id: int, artist_tag_id: int, connection: psycopg2_connection
) -> int:
    """
    Creates a new album entry in the database and returns its ID.

    Args:
        set_tag_id (int): The ID of the set tag to associate with the album.
        artist_tag_id (int): The ID of the artist tag to associate
            with the album.
        connection (psycopg2_connection):
            An active psycopg2 database connection.

    Returns:
        int: The ID of the newly created album.

    Raises:
        Exception: If the album could not be created or no ID was returned
            from the database.
    """
    sql_register_album = (
        "INSERT INTO album VALUES (DEFAULT, %s, %s) RETURNING id"
    )
    cursor = connection.cursor()
    cursor.execute(sql_register_album, (set_tag_id, artist_tag_id))
    result = cursor.fetchone()
    cursor.close()
    if result is not None:
        return result[0]
    else:
        raise Exception(
            (
                "Failed to create album: "
                "no album ID was returned from the database. "
                "This may indicate an error "
                "with the album registration process."
            )
        )


def set_album_order(
    album_id: int,
    content_id: int,
    order: int | None,
    connection: psycopg2_connection,
):
    """
    Sets, updates, or removes the order of a content item within an album
    in the album_order table.

    If the content is not yet registered in the album and an order is provided,
    it inserts a new record.
    If the content is already registered:
        - If a new order is provided and differs from the current one,
            it updates the order.
        - If order is None, it removes the content from the album.

    Args:
        album_id (int): The ID of the album.
        content_id (int): The ID of the content item.
        order (int | None): The desired order of the content in the album.
            If None, the content is removed from the album.
        connection (psycopg2_connection):
            An active psycopg2 database connection.

    Returns:
        None
    """
    sql_verify_content_registered = (
        "select * from album_order where album_id = %s and content_id = %s"
    )
    sql_insert_content_order = "INSERT INTO album_order VALUES (%s, %s, %s)"
    sql_update_order = (
        'UPDATE album_order SET "order" = %s '
        "WHERE album_id = %s and content_id = %s"
    )
    sql_delete_content_from_album = (
        "DELETE FROM album_order WHERE album_id = %s and content_id = %s"
    )
    cursor = connection.cursor()
    cursor.execute(sql_verify_content_registered, (album_id, content_id))
    content_info = cursor.fetchone()
    if content_info is None:
        if order is not None:
            cursor.execute(
                sql_insert_content_order, (album_id, content_id, order)
            )
    else:
        if order is not None:
            if content_info[2] != order:
                cursor.execute(sql_update_order, (order, album_id, content_id))
        else:
            cursor.execute(
                sql_delete_content_from_album, (album_id, content_id)
            )


def get_content_albums(
    content_id: int, connection: psycopg2_connection
) -> list[tuple[int, str, str]]:
    """
    Retrieve album information associated with a specific content ID.

    Args:
        content_id (int): The ID of the content for which to retrieve albums.
        connection (psycopg2_connection):
            An active psycopg2 database connection.

    Returns:
        list of tuple: A list of tuples, each containing:
            - ID (int): The album ID.
            - title (str): The title of the album's set tag.
            - album_artist (str): The title of the album artist's tag.

    Raises:
        psycopg2.DatabaseError:
            If a database error occurs during the query execution.

    Note:
        The function closes the cursor after fetching the results.
    """
    sql_get_albums_by_content_id = (
        "select ID, (select title from tag where tag.id = album.set_tag_id), "
        "(select title from tag where tag.id = album.album_artist_tag_id) "
        "from album where id in "
        "(select album_id from album_order where content_id = %s)"
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_albums_by_content_id, (content_id,))
    results = cursor.fetchall()
    cursor.close()
    return results


def get_album_covers(
    connection: psycopg2_connection,
) -> list[tuple[int, str, str, str, str, int]]:
    """
    Retrieve album cover information from the database.

    This function queries the database to obtain details about album covers,
    including the content ID, file path, content type, a concatenated string
    of album title and artist, and the album ID.
    It joins the `album`, `album_order`,and `content` tables,
    and uses subqueries to fetch the album title and artist name
    from the `tag` table.

    Args:
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.

    Returns:
        list of tuple: Each tuple contains (content.id, file_path,
        content_type, concatenated album title and artist, album.id).
    """
    sql_get_album_covers = (
        "select content.id, file_path, content_type, "
        "CONCAT"
        "((select title from tag where tag.id = album.set_tag_id), ' by ', "
        "(select title from tag where tag.id = album.album_artist_tag_id)), "
        "album.id "
        "from album, lateral "
        "(select * from album_order where album.id = album_order.album_id "
        'order by album_order."order" limit 1) ao '
        "join content on content.id = ao.content_id"
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_album_covers, tuple())
    results = cursor.fetchall()
    cursor.close()
    return results
