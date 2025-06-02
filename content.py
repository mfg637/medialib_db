from psycopg2.extensions import connection as psycopg2_connection
from datetime import datetime
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
            bool(result[8]),
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
    sql_template = "SELECT * FROM content WHERE id=%s"
    cursor.execute(sql_template, (content_id,))
    result = cursor.fetchone()
    if result is not None:
        return Content(
            result[0],
            pathlib.Path(result[1]),
            result[3],
            result[5],
            bool(result[8]),
            result[2],
            result[4],
        )
    else:
        return None


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
