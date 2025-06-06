from psycopg2.extensions import connection as psycopg2_connection
from psycopg2.extensions import cursor as psycopg2_cursor
from datetime import datetime
from . import common

import dataclasses
import pathlib


@dataclasses.dataclass(frozen=True)
class Content:
    content_id: int
    file_path: pathlib.Path
    content_type: str
    addition_date: datetime
    hidden: bool
    title: str | None
    description: str | None


def get_content_metadata_by_path(
    path: pathlib.Path, connection: psycopg2_connection
) -> Content | None:
    """
    Retrieve the metadata of content from the database by its file path.

    Args:
        path (pathlib.Path): The file path of the content.
        connection (psycopg2_connection): The database connection.

    Returns:
        Content: The Content object if found,
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
    if result is not None:
        return Content(
            result[0],
            pathlib.Path(result[1]),
            result[3],
            result[5],
            bool(result[6]),
            result[2],
            result[4],
        )
    else:
        return None


def _get_content_metadata_by_id(
    content_id: int, cursor: psycopg2_cursor
) -> Content | None:
    sql_template = "SELECT * FROM content WHERE id=%s"
    cursor.execute(sql_template, (content_id,))
    result = cursor.fetchone()
    if result is not None:
        return Content(
            result[0],
            pathlib.Path(result[1]),
            result[3],
            result[5],
            bool(result[6]),
            result[2],
            result[4],
        )
    else:
        return None


def get_content_metadata_by_id(
    content_id: int, connection: psycopg2_connection
) -> Content | None:
    """
    Retrieve the metadata of content from the database by content_id.

    Args:
        content_id (int): The ID of the content.
        connection (psycopg2_connection): The database connection.

    Returns:
        Content: The Content object if found,
            or None if no content is found for the given ID.
    """
    cursor = connection.cursor()
    result = _get_content_metadata_by_id(content_id, cursor)
    cursor.close()
    return result


def content_update(
    content_id: int,
    content_title: str | None,
    hidden: bool,
    description: str | None,
    connection: psycopg2_connection,
):
    """
    Updates the details of a content record in the database.

    Args:
        content_id (int): The unique identifier of the content to update.
        content_title (str | None): The new title of the content, or None.
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
        "SET title = %s, hidden = %s, description = %s "
        "WHERE id = %s"
    )
    cursor.execute(
        sql_template,
        (
            content_title,
            hidden,
            description,
            content_id,
        ),
    )
    connection.commit()


def _content_register(
    cursor: psycopg2_cursor,
    content_title: str | None,
    file_path: pathlib.Path | str,
    content_type: str,
    addition_date: datetime | None,
    description: str | None,
    hidden: bool,
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
        hidden (bool, optional): Whether the content is hidden.
            Defaults to False.

    Returns:
        int: The ID of the newly registered content.
    """
    sql_regular_template = (
        "INSERT INTO content VALUES "
        "(DEFAULT, %s, %s, %s, %s, %s, %s) RETURNING id"
    )
    sql_date_now_template = (
        "INSERT INTO content VALUES "
        "(DEFAULT, %s, %s, %s, %s, NOW(), %s) RETURNING id"
    )
    if addition_date is not None:
        cursor.execute(
            sql_regular_template,
            (
                str(file_path),
                common.postgres_string_format(
                    content_title, common.CONTENT_TITLE_MAX_SIZE
                ),
                content_type,
                description,
                addition_date,
                hidden,
            ),
        )
    else:
        cursor.execute(
            sql_date_now_template,
            (
                str(file_path),
                common.postgres_string_format(
                    content_title, common.CONTENT_TITLE_MAX_SIZE
                ),
                content_type,
                description,
                hidden,
            ),
        )
    content_id = common.get_value_or_fail(
        cursor.fetchone(), "Content ID is none for some reason"
    )
    return content_id


def content_register(
    connection: psycopg2_connection,
    content_title: str | None,
    file_path: pathlib.Path | str,
    content_type: str,
    addition_date: datetime | None,
    description: str | None,
    hidden: bool = False,
) -> int:
    """
    Registers new content in the media library database.

    Args:
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.
        content_title (str | None):
            The title of the content to register. Can be None.
        file_path (pathlib.Path | str): The file path to the content.
        content_type (str): The type/category of the content.
        addition_date (datetime): The date the content was added.
        description (str | None): A description of the content. Can be None.
        hidden (bool, optional):
            Whether the content should be hidden. Defaults to False.

    Returns:
        int: The ID of the newly registered content.
    """
    cursor = connection.cursor()
    result = _content_register(
        cursor,
        content_title,
        file_path,
        content_type,
        addition_date,
        description,
        hidden,
    )
    cursor.close()
    connection.commit()
    return result
