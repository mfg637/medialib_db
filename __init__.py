from psycopg2.extensions import connection as psycopg2_connection
from . import files_by_tag_search
from . import common
from . import testing
from . import tags_indexer
from . import srs_indexer
from . import config
from datetime import datetime

import dataclasses
import enum
import pathlib


openclip_classification = None
if config.enable_openclip:
    from . import openclip_classification


def get_tag_name_by_alias(alias: str) -> str:
    """
    Retrieve the tag name associated with a given alias from the database.

    Opens a new database connection, queries for the tag name corresponding to
    the provided alias,
    and closes the connection before returning the result.

    Args:
        alias (str): The unique alias of the tag.

    Returns:
        str: The name of the tag associated with the given alias.

    Raises:
        Exception: If no tag is found for the provided alias.
    """
    connection = common.make_connection()
    cursor = connection.cursor()
    sql_template = (
        "SELECT title FROM tag WHERE id = "
        "(SELECT tag_id FROM tag_alias WHERE title=%s)"
    )
    cursor.execute(sql_template, (alias,))
    result: tuple[str] | None = cursor.fetchone()
    connection.close()
    return common.get_value_or_fail(result, f"Tag by alias {alias} not found")


def get_tag_name_by_id(tag_id: int) -> str:
    """
    Retrieve the name (title) of a tag from the database by its ID.
    It opens database to make the request.

    Args:
        tag_id (int): The unique identifier of the tag.

    Returns:
        str: The title of the tag corresponding to the given ID.

    Raises:
        Exception: If no tag with the specified ID is found in the database.
    """
    connection: psycopg2_connection = common.make_connection()
    cursor = connection.cursor()
    sql_template = "SELECT title FROM tag WHERE id = %s"
    cursor.execute(sql_template, (id,))
    result: tuple[str] | None = cursor.fetchone()
    connection.close()
    return common.get_value_or_fail(result, f"Tag by tag id {id} not found")


def get_content_metadata_by_file_path(
    path: pathlib.Path, connection: psycopg2_connection
) -> (
    tuple[
        int,
        str,
        str | None,
        str,
        str | None,
        datetime,
        str | None,
        str | None,
        bool,
    ]
    | None
):
    """
    Retrieve the metadata of content from the database by its file path.

    Args:
        path (pathlib.Path): The file path of the content.
        connection (psycopg2_connection): The database connection.

    Returns:
        tuple: A tuple containing the following fields:
            (content_id: int, file_path: str, title: str, content_type: str,
             description: str, addition_date: datetime, origin: str,
             origin_content_id: str, hidden: bool)
        or None if no content is found for the given file path.
    """
    cursor = connection.cursor()
    sql_template = "SELECT * FROM content WHERE file_path=%s"
    cursor.execute(sql_template, (str(path),))
    result: (
        tuple[
            int,
            str,
            str | None,
            str,
            str | None,
            datetime,
            str | None,
            str | None,
            bool,
        ]
        | None
    ) = cursor.fetchone()
    return result


def get_content_metadata_by_content_id(
    content_id: int, connection: psycopg2_connection
) -> (
    tuple[
        int,
        str,
        str | None,
        str,
        str | None,
        datetime,
        str | None,
        str | None,
        bool,
    ]
    | None
):
    """
    Retrieve the metadata of content from the database by content_id.

    Args:
        content_id (int): The ID of the content.
        connection (psycopg2_connection): The database connection.

    Returns:
        tuple: A tuple containing the following fields:
            (content_id: int, file_path: str, title: str, content_type: str,
             description: str, addition_date: datetime, origin: str,
             origin_content_id: str, hidden: bool)
        or None if no content is found for the given ID.
    """
    cursor = connection.cursor()
    sql_template = "SELECT * FROM content WHERE id=%s"
    cursor.execute(sql_template, (content_id,))
    result = cursor.fetchone()
    return result


def get_thumbnail_by_filepath(
    path: pathlib.Path,
    width: int,
    height: int,
    _format: str,
    connection: psycopg2_connection,
) -> tuple[str | None, str | None]:
    """
    Retrieve the file path and format of a thumbnail image
    for a given media file.

    Args:
        path (pathlib.Path): The file path of the original media content.
        width (int): The desired width of the thumbnail.
        height (int): The desired height of the thumbnail.
        _format (str): The desired format of the thumbnail
        (e.g., 'webp', 'avif').
        connection (psycopg2_connection): An active connection
        to the PostgreSQL database.

    Returns:
        tuple[str | None, str | None]: A tuple containing the file path and
        format of the thumbnail if found,
        otherwise (None, None).
    """
    sql_template = (
        "SELECT file_path, format FROM thumbnail "
        "WHERE content_id = (SELECT ID FROM content WHERE file_path=%s) "
        "and width = %s and height = %s and format = %s"
    )
    cursor = connection.cursor()
    cursor.execute(sql_template, (str(path), width, height, _format))
    result = cursor.fetchone()
    if result is not None:
        return result
    else:
        return None, None


def get_thumbnail_by_content_id(
    content_id: int,
    width: int,
    height: int,
    _format: str,
    connection: psycopg2_connection,
) -> tuple[str | None, str | None]:
    """
    Retrieve the file path and format of a thumbnail image
    for a given `content_id`.

    Args:
        content_id (int): The ID of the content.
        width (int): The desired width of the thumbnail.
        height (int): The desired height of the thumbnail.
        _format (str): The desired format of the thumbnail
        (e.g., 'webp', 'avif').
        connection (psycopg2_connection): An active connection
        to the PostgreSQL database.

    Returns:
        tuple[str | None, str | None]: A tuple containing the file path and
        format of the thumbnail if found,
        otherwise (None, None).
    """
    sql_template = (
        "SELECT file_path, format FROM thumbnail "
        "WHERE content_id = %s "
        "and width = %s and height = %s and format = %s"
    )
    cursor = connection.cursor()
    cursor.execute(sql_template, (content_id, width, height, _format))
    result = cursor.fetchone()
    if result is not None:
        return result
    else:
        return None, None


def register_thumbnail_by_content_id(
    content_id: int,
    width: int,
    height: int,
    _format: str,
    connection: psycopg2_connection,
) -> str:
    """
    Registers a thumbnail entry in the database for a given content ID and
    returns the generated thumbnail file name.

    Args:
        content_id (int): The unique identifier for the content.
        width (int): The width of the thumbnail in pixels.
        height (int): The height of the thumbnail in pixels.
        _format (str): The file format of the thumbnail (e.g., 'webp', 'avif').
        connection (psycopg2_connection): An active psycopg2
        database connection.

    Returns:
        str: The generated thumbnail file name in the format
        '{content_id}-{width}x{height}.{format}'.

    Raises:
        psycopg2.DatabaseError:
        If a database error occurs during the operation.
    """
    sql_template_register = (
        "INSERT INTO thumbnail VALUES" " (%s, %s, %s, NOW(), %s, %s)"
    )
    cursor = connection.cursor()
    thumbnail_file_name = "{}-{}x{}.{}".format(
        content_id, width, height, _format.lower()
    )
    cursor.execute(
        sql_template_register,
        (
            content_id,
            width,
            height,
            _format,
            thumbnail_file_name,
        ),
    )
    connection.commit()
    return thumbnail_file_name


def register_thumbnail_by_file_path(
    source_file: pathlib.Path,
    width: int,
    height: int,
    _format: str,
    connection: psycopg2_connection,
) -> tuple[int, str]:
    """
    Registers a thumbnail for a media file specified by its file path.

    This function retrieves the content ID
    associated with the given file path from the database,
    then registers a thumbnail for that content
    using the specified width, height, and format.

    Args:
        source_file (pathlib.Path): The path to the source media file.
        width (int): The width of the thumbnail to register.
        height (int): The height of the thumbnail to register.
        _format (str): The format of the thumbnail (e.g., 'webp', 'avif').
        connection (psycopg2_connection):
        An active connection to the PostgreSQL database.

    Returns:
        tuple: A tuple containing the content ID and
        the generated thumbnail file name in the format
        '{content_id}-{width}x{height}.{format}'.

    Raises:
        Exception: If the file path is not found in the database.
    """
    sql_template_get_id = "SELECT id from content WHERE file_path=%s"
    cursor = connection.cursor()
    cursor.execute(sql_template_get_id, (str(source_file),))
    content_id = common.get_value_or_fail(
        cursor.fetchone(), f"File path {source_file} not found in the database"
    )
    cursor.close()
    return content_id, register_thumbnail_by_content_id(
        content_id, width, height, _format, connection
    )


def drop_thumbnails(content_id: int, connection: psycopg2_connection):
    """
    Deletes all thumbnail records associated with
    the specified content ID from the database.

    Args:
        content_id (int): The ID of the content
            whose thumbnails should be deleted.
        connection (psycopg2_connection): An active connection
            to the PostgreSQL database.

    Raises:
        psycopg2.DatabaseError: If a database error occurs
            during the operation.
    """
    sql_template_get_id = "DELETE FROM thumbnail WHERE content_id = %s"
    cursor = connection.cursor()
    cursor.execute(sql_template_get_id, (content_id,))
    connection.commit()


def content_update(
    content_id: int,
    content_title: str | None,
    origin_name: str | None,
    origin_id: str | None,
    hidden: bool,
    description: str | None,
    connection: psycopg2_connection,
):
    """
    Updates the details of a content record in the database.

    Args:
        content_id (int): The unique identifier of the content to update.
        content_title (str | None): The new title of the content, or None.
        origin_name (str | None): The name of the content's origin,
            or None if unknown.
        origin_id (str | None): The identifier of the content in its origin,
            or None if not applicable.
        hidden (bool): Whether the content should be marked as hidden.
        description (str | None): The new description of the content, or None.
        connection (psycopg2_connection):
            An active psycopg2 database connection.

    Returns:
        None
    """
    cursor = connection.cursor()
    sql_template = (
        "UPDATE content "
        "SET title = %s, origin = %s, origin_content_id = %s, hidden = %s, "
        "description = %s "
        "WHERE id = %s"
    )
    cursor.execute(
        sql_template,
        (
            content_title,
            origin_name,
            origin_id,
            hidden,
            description,
            content_id,
        ),
    )
    connection.commit()


def content_register(
    connection: psycopg2_connection,
    content_title: str | None,
    file_path: pathlib.Path | str,
    content_type: str,
    addition_date: datetime,
    description: str | None,
    origin_name: str | None,
    origin_id: str | None,
    hidden=False,
    *,
    content_id=None,
) -> int:
    """
    Registers new content in the database.

    Inserts a new record into the 'content' table with the provided metadata
    and returns the generated content ID.

    Args:
        connection: A database connection object.
        content_title (str): The title of the content.
        file_path (str or Path): The file path to the content.
        content_type (str): The type/category of the content.
        addition_date (str or datetime): The date the content was added.
        description (str): A description of the content.
        origin_name (str): The name of the content's origin/source.
        origin_id (str): The identifier of the content's origin/source.
        hidden (bool, optional): Whether the content is hidden.
            Defaults to False.
        content_id (int, optional): Unused. Reserved for future use.

    Returns:
        int: The ID of the newly registered content.
    """
    sql_template = (
        "INSERT INTO content VALUES "
        "(DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
    )
    cursor = connection.cursor()
    cursor.execute(
        sql_template,
        (
            str(file_path),
            content_title,
            content_type,
            description,
            addition_date,
            origin_name,
            origin_id,
            hidden,
        ),
    )
    content_id = common.get_value_or_fail(
        cursor.fetchone(), "Content ID is none for some reason"
    )
    connection.commit()
    return content_id


sql_insert_content_id_to_tag_id = (
    "INSERT INTO content_tags_list (content_id, tag_id) VALUES (%s, %s)"
)


def add_tags_for_content(
    content_id: int,
    tags: list[tuple[str, str, str]],
    connection: psycopg2_connection,
):
    """
    Associates a list of tags with a given content item in the database.

    Each tag is represented as a tuple of (namespace, title, alias).
    The function checks if each tag exists,
    inserts new tags if necessary, and links the tags
    to the specified content ID. If a tag does not exist and
    cannot be registered, an exception is raised.

    Args:
        content_id (int): The ID of the content to associate tags with.
        tags (list[tuple[str, str, str]]): A list of tags, each as a tuple
            (namespace, title, alias).
        connection (psycopg2_connection):
            An active psycopg2 database connection.

    Raises:
        Exception: If a tag cannot be found or registered in the database.
    """
    cursor = connection.cursor()
    for tag in tags:
        tag_id = None
        if tag[1] is not None:
            tag_id = tags_indexer.check_tag_exists(tag[0], tag[1], connection)
        else:
            get_id_by_tag_alias_sql = (
                "SELECT tag_id FROM tag_alias WHERE title = %s"
            )
            cursor.execute(get_id_by_tag_alias_sql, (tag[2],))
            tag_id = cursor.fetchone()

        if tag_id is None and tag[1] is not None:
            tag_id = tags_indexer.insert_new_tag(
                tag[0], tag[1], tag[2], connection
            )
        elif tag_id is not None:
            tag_id = tag_id[0]
        else:
            raise Exception("Not registered tag error", tag[0])

        cursor.execute(sql_insert_content_id_to_tag_id, (content_id, tag_id))

    connection.commit()


def add_tags_for_content_by_tag_ids(
    content_id: int, tag_ids: list[int], connection: psycopg2_connection
):
    """
    Associates a list of tag IDs with a specific content item in the database.

    Args:
        content_id (int): The ID of the content to which tags will be added.
        tag_ids (list[int]): A list of tag IDs to associate with the content.
        connection (psycopg2_connection):
            An active psycopg2 database connection.

    Raises:
        psycopg2.DatabaseError: If a database operation fails.

    Commits:
        The changes are committed to the database
            after all associations are made.
    """
    cursor = connection.cursor()
    for tag_id in tag_ids:
        cursor.execute(sql_insert_content_id_to_tag_id, (content_id, tag_id))

    connection.commit()


def connect_tag_by_id(
    content_id: int, tag_id: int, connection: psycopg2_connection
):
    """
    Associates a tag with a content item in the database
    if the association does not already exist.

    Args:
        content_id (int): The ID of the content item to associate.
        tag_id (int): The ID of the tag to associate with the content item.
        connection (psycopg2_connection):
            An active psycopg2 database connection.

    Raises:
        psycopg2.DatabaseError: If a database error occurs during execution.

    Note:
        Commits the transaction after inserting a new association.
    """
    cursor = connection.cursor()
    sql_validate_tag_connected = (
        "SELECT * FROM content_tags_list WHERE content_id = %s and tag_id = %s"
    )
    cursor.execute(sql_validate_tag_connected, (content_id, tag_id))
    connection_exists = cursor.fetchone()
    if connection_exists is None:
        cursor.execute(sql_insert_content_id_to_tag_id, (content_id, tag_id))
    connection.commit()


def get_tags_by_content_id(
    content_id: int, auto_open_connection=True
) -> dict[str, list[tuple[int, str]]]:
    """
    Retrieve tags associated with a given content ID, grouped by category.

    Args:
        content_id (int): The ID of the content item
            for which to retrieve tags.
        auto_open_connection (bool, optional):
            Whether to automatically open a database connection.
            Defaults to True.

    Returns:
        dict[str, list[tuple[int, str]]]:
            A dictionary where each key is a tag category (str),
            and each value is a list of tuples.
            Each tuple contains the tag ID (int) and tag title (str)
            for tags in that category.

    Raises:
        Any exceptions raised by the underlying database connection
            or query execution.

    Example:
        >>> get_tags_by_content_id(42)
        {
            "characters": [(1, "twilight sparkle"), (2, "rarity")],
            "rating": [(4, "safe")]
        }
    """
    connection = common.make_connection()
    cursor = connection.cursor()
    sql_template = (
        "SELECT id, title, category FROM tag where id in "
        "(SELECT tag_id from content_tags_list where content_id = %s)"
    )
    cursor.execute(sql_template, (content_id,))
    result = dict()
    tag = cursor.fetchone()
    while tag is not None:
        if tag[2] not in result:
            result[tag[2]] = [(tag[0], tag[1])]
        else:
            result[tag[2]].append((tag[0], tag[1]))
        tag = cursor.fetchone()
    connection.close()
    return result


def find_content_from_source(
    origin: str, origin_content_id: str, connection: psycopg2_connection
) -> tuple[int, str] | None:
    """
    Searches for content in the database by its origin and origin content ID.

    Args:
        origin (str): The source/origin of the content.
        origin_content_id (str): The unique identifier of the content
            in the origin system.
        connection: A database connection object
            supporting the cursor interface.

    Returns:
        tuple[int, str] or None: A tuple containing
        the content ID (int) and file path (str) if found,
        otherwise None.

    Raises:
        Exception: Propagates any exceptions
            raised by the database connection or cursor.
    """
    sql_template = (
        "SELECT ID, file_path FROM content "
        "WHERE origin = %s and origin_content_id = %s"
    )
    cursor = connection.cursor()
    cursor.execute(sql_template, (origin, origin_content_id))
    result = cursor.fetchone()
    # it may be important for some reason
    cursor.fetchall()
    return result


def set_image_hash(
    content_id: int,
    image_hash: tuple[float, bytes, int, int],
    connection: psycopg2_connection,
):
    """
    Write or update the hash of image content in the database
        for a given content ID.

    If an entry for the specified content_id exists in the `imagehash` table,
        it updates the record.
    Otherwise, it inserts a new record with the provided hash data.

    Args:
        content_id (int): The unique identifier for the content
            whose image hash is being set.
        image_hash (tuple[float, bytes, int, int]): A tuple containing:
            - aspect_ratio (float): The aspect ratio of the image
                (32-bit float).
            - value_hash (bytes): The value hash of the image
                (256-bit byte sequence).
            - hue_hash (int): The hue hash of the image (64-bit integer).
            - saturation_hash (int): The saturation hash of the image
                (64-bit integer).
        connection (psycopg2_connection):
            An active psycopg2 database connection.

    Raises:
        Any exception raised by the underlying database operations.

    Note:
        The value_hash is stored in the database as a hexadecimal string
        and decoded to bytes using PostgreSQL's `decode` function.
    """
    sql_verify_hash_exists = "SELECT * FROM imagehash WHERE content_id=%s"
    sql_insert_image_hash = (
        "INSERT INTO imagehash "
        "(content_id, aspect_ratio, value_hash, hue_hash, saturation_hash) "
        "VALUES (%s, %s, decode(%s, 'hex'), %s, %s)"
    )
    sql_update_image_hash = (
        "UPDATE imagehash SET "
        "aspect_ratio = %s, "
        "value_hash = decode(%s, 'hex'), "
        "hue_hash = %s, saturation_hash = %s "
        "WHERE content_id = %s"
    )
    aspect_ratio, value_hash, hue_hash, saturation_hash = image_hash
    cursor = connection.cursor()
    cursor.execute(sql_verify_hash_exists, (content_id,))
    exists_hash_data = cursor.fetchone()
    if exists_hash_data is None:
        cursor.execute(
            sql_insert_image_hash,
            (
                content_id,
                aspect_ratio,
                value_hash.hex(),
                hue_hash,
                saturation_hash,
            ),
        )
    else:
        cursor.execute(
            sql_update_image_hash,
            (
                aspect_ratio,
                value_hash.hex(),
                hue_hash,
                saturation_hash,
                content_id,
            ),
        )
    connection.commit()
    cursor.close()


def update_file_path(
    content_id: int,
    file_path: pathlib.Path,
    image_hash: tuple[float, bytes, int, int] | None,
    connection: psycopg2_connection,
):
    """
    Updates the file path and addition date for a given content record
        in the database.

    Parameters:
        content_id (int): The unique identifier of the content to update.
        file_path (pathlib.Path): The new file path to set for the content.
        image_hash (tuple[float, bytes, int, int]):
            The image hash to associate with the content.
            If None, the image hash is not updated.
        connection (psycopg2_connection):
            The database connection to use for executing the update.

    Side Effects:
        - Updates the 'file_path' and 'addition_date' fields
            in the 'content' table for the specified content ID.
        - If the file has a '.srs' extension,
            updates representations table using the srs_indexer.
        - Commits the transaction to the database.
        - If an image_hash is provided, updates the image hash for the content.
    """
    sql_template = (
        "UPDATE content SET file_path = %s, addition_date=NOW() WHERE ID = %s"
    )
    cursor = connection.cursor()
    cursor.execute(
        sql_template,
        (str(file_path.relative_to(config.relative_to)), content_id),
    )
    if file_path.suffix == ".srs":
        srs_indexer.srs_update_representations(content_id, file_path, cursor)
    connection.commit()
    if image_hash is not None:
        set_image_hash(content_id, image_hash, connection)


def get_representation_by_content_id(
    content_id: int, connection: psycopg2_connection
) -> list[srs_indexer.ContentRepresentationUnit]:
    """
    Fetches all content representations
    for a given content ID from the database.

    Args:
        content_id (int): The unique identifier of the content
            whose representations are to be retrieved.
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.

    Returns:
        list[srs_indexer.ContentRepresentationUnit]:
            A list of ContentRepresentationUnit objects,
            each representing a different format and compatibility level
            for the specified content.
            The list is ordered by compatibility level.

    Raises:
        psycopg2.DatabaseError:
            If a database error occurs during the query execution.
    """
    sql_get_representations = (
        "SELECT format, compatibility_level, file_path FROM representations "
        "WHERE content_id=%s ORDER BY compatibility_level"
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_representations, (content_id,))
    results = []
    raw_representation = cursor.fetchone()
    while raw_representation is not None:
        results.append(
            srs_indexer.ContentRepresentationUnit(
                config.relative_to.joinpath(raw_representation[2]),
                raw_representation[1],
                raw_representation[0],
            )
        )
        raw_representation = cursor.fetchone()
    cursor.close()
    return results


def get_image_hash(
    content_id: int, connection: psycopg2_connection
) -> tuple[int, float, int, int, bytes, bool] | None:
    """
    Retrieve the image hash data for a given content ID
    from the 'imagehash' table.

    Args:
        content_id (int): The ID of the content
            whose image hash is to be retrieved.
        connection (psycopg2_connection):
            An active psycopg2 database connection.

    Returns:
        tuple or None: A tuple containing the image hash data if found,
        otherwise None.
            The tuple corresponds to the columns:
            — content_id: int
            — aspect_ratio: float
            — hue_hash: int
            — saturation_hash: int
            — value_hash: bytes
            — alternate_version: bool
    """
    sql_get_hash = "SELECT * FROM imagehash WHERE content_id=%s"
    cursor = connection.cursor()
    cursor.execute(sql_get_hash, (content_id,))
    exists_hash_data = cursor.fetchone()
    cursor.close()
    return exists_hash_data


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
) -> (
    list[
        tuple[
            int, str, str, str | None, str | None, str | None, str | None, int
        ]
    ]
    | None
):
    """
    Retrieve the content associated with a specific album from the database.

    Args:
        album_id (int): The ID of the album whose content is to be retrieved.
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.

    Returns:
        list of tuple: A list of tuples,
        each containing the following fields for each content item:
            - ID (int): The unique identifier of the content.
            - file_path (str): The file path of the content.
            - content_type (str): The type of the content.
            - title (str | None): The title of the content.
            - description (str | None): The description of the content.
            - origin (str | None): The origin of the content.
            - origin_content_id (str | None): The ID of the content
                in the origin, if applicable.
            - order (int): The order of the content within the album.

    Raises:
        psycopg2.DatabaseError: If a database error occurs during the query.
    """
    sql_get_content_by_album_id = (
        "select content.ID, file_path, content_type, title, description, "
        'origin, origin_content_id, "order" '
        "from album_order join content on album_order.content_id = content.ID "
        'where album_id = %s order by album_order."order";'
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_content_by_album_id, (album_id,))
    result = cursor.fetchall()
    cursor.close()
    return result


def get_album_related_content(
    set_tag_id: int, artist_tag_id: int, connection: psycopg2_connection
) -> (
    list[
        tuple[
            int,
            str,
            str,
            str | None,
            str | None,
            str | None,
            str | None,
            int | None,
        ]
    ]
    | None
):
    """
    Retrieve content items
    related to a specific album and artist from the database.

    Args:
        set_tag_id (int): The tag ID representing the album.
        artist_tag_id (int): The tag ID representing the artist.
        connection (psycopg2.connection): Database connection object.

    Returns:
        list of tuple: A list of tuples, each containing the following fields
        for matching content:
            - id (int): Content ID.
            - file_path (str): Path to the content file.
            - content_type (str): Type of the content.
            - title (str | None): Title of the content.
            - description (str | None): Description of the content.
            - origin (str | None): Origin of the content.
            - origin_content_id (str | None):
                ID of the content in the origin system.
            - order (int | None):
                Order of the content in the album, if available.

    Notes:
        The function performs a SQL query to select content that is associated
        with both the specified album and artist tags.
        It joins the 'content' and 'album_order' tables
        to retrieve ordering information.
    """

    sql_get_content_by_album_id = (
        "select content.id, file_path, content_type, title, description, "
        'origin, origin_content_id, album_order."order" '
        "from content left outer join album_order "
        "on content.id = album_order.content_id "
        "where content.id in "
        "(SELECT content_id FROM content_tags_list WHERE tag_id = %s) "
        "and content.id in "
        "(SELECT content_id FROM content_tags_list WHERE tag_id = %s)"
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_content_by_album_id, (set_tag_id, artist_tag_id))
    result = cursor.fetchall()
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


@dataclasses.dataclass
class DuplicatedContentItem:
    content_id: int
    file_path: str
    content_type: str
    title: str
    is_alternate_version: bool
    content_metadata = None


@dataclasses.dataclass
class DuplicateImageHashItem:
    value_hash: bytes
    hue_hash: int
    saturation_hash: int
    duplicated_images: list[DuplicatedContentItem]


def find_duplicates(
    connection: psycopg2_connection, show_alternates: bool = False
) -> list[DuplicateImageHashItem]:
    """
    Finds duplicate images in the database based on their hash values.

    This function queries the `imagehash` table
    to identify duplicate image hashes,
    optionally including or excluding alternate versions.
    For each set of duplicates,
    it retrieves the associated content metadata
    and returns a list of duplicate image hash items.

    Args:
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.
        show_alternates (bool, optional):
            If True, includes alternate versions in the duplicate search.
            If False, excludes alternate versions. Defaults to False.

    Returns:
        list[DuplicateImageHashItem]:
            A list of DuplicateImageHashItem objects, each containing
            the hash values and a list of duplicated content items.

    Raises:
        Any exceptions raised by the underlying database connection
            or cursor operations.
    """
    sql_find_duplicates_and_hide_alternates = (
        "select encode(value_hash, 'hex'), hue_hash, saturation_hash from "
        "(select count(*) as c1, value_hash, hue_hash, saturation_hash, "
        "alternate_version from imagehash "
        "group by value_hash, hue_hash, saturation_hash, alternate_version) "
        "as u1 where u1.c1 > 1 and u1.alternate_version = false;"
    )
    sql_find_duplicates_and_alternates = (
        "select encode(value_hash, 'hex'), hue_hash, saturation_hash from "
        "(select count(*) as c1, value_hash, hue_hash, saturation_hash "
        "from imagehash group by value_hash, hue_hash, saturation_hash) as u1 "
        "where u1.c1 > 1;"
    )
    sql_find_duplicates_by_hash = (
        "select "
        "content.ID, file_path, content_type, title, alternate_version "
        "from content join imagehash on content.ID = imagehash.content_id "
        "where value_hash = decode(%s, 'hex') "
        "and hue_hash = %s "
        "and saturation_hash = %s;"
    )
    results: list[DuplicateImageHashItem] = []
    cursor = connection.cursor()
    if show_alternates:
        cursor.execute(sql_find_duplicates_and_alternates, tuple())
    else:
        cursor.execute(sql_find_duplicates_and_hide_alternates, tuple())
    image_hash_list = cursor.fetchall()
    for image_hash in image_hash_list:
        hash_item = DuplicateImageHashItem(
            image_hash[0], image_hash[1], image_hash[2], []
        )
        cursor.execute(
            sql_find_duplicates_by_hash,
            (
                hash_item.value_hash,
                hash_item.hue_hash,
                hash_item.saturation_hash,
            ),
        )
        image_data = cursor.fetchone()
        while image_data is not None:
            image_item = DuplicatedContentItem(*image_data)
            hash_item.duplicated_images.append(image_item)
            image_data = cursor.fetchone()
        results.append(hash_item)
    cursor.close()
    return results


def find_content_by_hash(
    value_hash: str,
    hue_hash: int,
    saturation_hash: int,
    connection: psycopg2_connection,
) -> list[tuple[int, bool]]:
    """
    Finds content entries in the database
    that match the given image hash values.

    Args:
        value_hash (str):
            The hexadecimal string representing the value hash of the image.
        hue_hash (int): The integer representing the hue hash of the image.
        saturation_hash (int):
            The integer representing the saturation hash of the image.
        connection: A database connection object supporting cursor operations.

    Returns:
        list[tuple[int, bool]]: A list of tuples,
            each containing the content ID (int) and a boolean
            indicating if it is an alternate version.
    """
    sql_find_duplicates_by_hash = (
        "select content_id, alternate_version "
        "from imagehash  "
        "where value_hash = decode(%s, 'hex') "
        "and hue_hash = %s "
        "and saturation_hash = %s;"
    )
    cursor = connection.cursor()
    cursor.execute(
        sql_find_duplicates_by_hash, (value_hash, hue_hash, saturation_hash)
    )
    raw_result = cursor.fetchall()
    cursor.close()
    return raw_result


def mark_alternate_version(
    first_content_id: int,
    second_content_id: int,
    connection: psycopg2_connection,
):
    """
    Marks two content items as alternate versions
    of each other in the database.

    This function updates the 'imagehash' table,
    etting the 'alternate_version' field to TRUE
    for the rows corresponding to the provided content IDs.

    Args:
        first_content_id (int):
            The content ID of the first item to mark as an alternate version.
        second_content_id (int):
            The content ID of the second item to mark as an alternate version.
        connection (psycopg2_connection):
            An active psycopg2 database connection.

    Raises:
        psycopg2.DatabaseError:
            If a database error occurs during the update operation.
    """
    sql_mark_alternate = (
        "update imagehash "
        "set alternate_version = TRUE where content_id in (%s, %s)"
    )
    cursor = connection.cursor()
    cursor.execute(sql_mark_alternate, (first_content_id, second_content_id))
    cursor.close()
    connection.commit()


class ACCESS_LEVEL(enum.IntEnum):
    BAN = 0
    DEFAULT = 1
    SUGGESTIVE = 2
    NSFW = 3
    GAY = 4
    ULTIMATE = 5


@dataclasses.dataclass
class User:
    id: int
    platform: str
    platform_id: int
    username: str
    access_level: ACCESS_LEVEL


def register_user_and_get_info(
    user_platform_id, platform, connection, username=None, password=None
):
    sql_get_user = (
        'SELECT * FROM "user" as u where '
        "u.platform = %s and u.platform_id = %s"
    )
    sql_register_telegram = (
        'insert into "user" (platform, platform_id, username) '
        "values (%s, %s, %s)"
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_user, (platform, user_platform_id))
    user_data = cursor.fetchone()
    if user_data is None:
        if platform != "telegram":
            raise NotImplementedError
        if platform == "telegram":
            cursor.execute(
                sql_register_telegram, (platform, user_platform_id, username)
            )
        connection.commit()
        cursor.execute(sql_get_user, (platform, user_platform_id))
        user_data = cursor.fetchone()
        cursor.close()
    return User(
        user_data[0],
        user_data[1],
        user_data[2],
        user_data[3],
        ACCESS_LEVEL[user_data[5].upper()],
    )


@dataclasses.dataclass
class TGChat:
    id: int
    title: str
    access_level: ACCESS_LEVEL


def register_channel_and_get_info(chat_id, title, connection):
    sql_get_chat = "SELECT * FROM telegram_bot.chat as u where u.id = %s"
    sql_register_telegram = (
        "insert into telegram_bot.chat (id, title) values (%s, %s)"
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_chat, (chat_id,))
    chat_data = cursor.fetchone()
    if chat_data is None:
        cursor.execute(sql_register_telegram, (chat_id, title))
        connection.commit()
        cursor.execute(sql_get_chat, (chat_id,))
        chat_data = cursor.fetchone()
        cursor.close()
    return TGChat(
        chat_data[0], chat_data[1], ACCESS_LEVEL[chat_data[2].upper()]
    )


def register_post(user_id, content_id, connection):
    sql_register_post = (
        "INSERT INTO telegram_bot.post (user_id, content_id) "
        "VALUES (%s, %s) RETURNING id"
    )
    cursor = connection.cursor()
    cursor.execute(sql_register_post, (user_id, content_id))
    post_id = cursor.fetchone()[0]
    cursor.close()
    connection.commit()
    return post_id


def get_post(post_id, connection):
    sql_get_post = "SELECT * FROM telegram_bot.post WHERE id = %s"
    cursor = connection.cursor()
    cursor.execute(sql_get_post, (post_id,))
    result = cursor.fetchone()
    cursor.close()
    return result


def register_representation(
    content_id: int,
    _format: str,
    compatibility_level: int,
    file_path: str,
    connection: psycopg2_connection,
):
    """
    Registers a new representation for a given content item in the database.

    Args:
        content_id (int): The unique identifier of the content item.
        _format (str): The format of the representation (e.g., 'webp', 'avif').
        compatibility_level (int):
            The compatibility level of the representation.
        file_path (str): The file system path to the representation file.
        connection: A database connection object supporting cursor().

    Raises:
        Any exception raised by the underlying database driver
        during execution.

    Note:
        The file_path should be stored as a string.
        If relative paths are needed, consider using
        `str(file_path.relative_to(medialib_db.config.relative_to))`.
    """
    sql_insert_representation = (
        "INSERT INTO representations "
        "(content_id, format, compatibility_level, file_path) "
        "VALUES (%s, %s, %s, %s)"
    )
    cursor = connection.cursor()
    cursor.execute(
        sql_insert_representation,
        (
            content_id,
            _format,
            compatibility_level,
            # use this to store in database:
            # str(file_path.relative_to(config.relative_to))
            file_path,
        ),
    )
    cursor.close()


def delete_tag(content_id: int, tag_id: int, connection: psycopg2_connection):
    """
    Deletes a tag association
    from the content_tags_list table
    for a given content and tag ID.

    Args:
        content_id (int): The ID of the content item.
        tag_id (int): The ID of the tag to be removed from the content.
        connection (psycopg2_connection):
            An active psycopg2 database connection.

    Raises:
        psycopg2.DatabaseError: If a database error occurs during execution.

    Note:
        The function does not commit the transaction;
        the caller is responsible for committing or rolling back.
    """
    sql_tag_delete = (
        "delete from content_tags_list where content_id=%s and tag_id=%s;"
    )
    cursor = connection.cursor()
    cursor.execute(sql_tag_delete, (content_id, tag_id))
    cursor.close()
