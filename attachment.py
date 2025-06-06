from psycopg2.extensions import connection as psycopg2_connection
from psycopg2.extensions import cursor as psycopg2_cursor

import dataclasses
import pathlib
import base64

try:
    from . import config
except ImportError:
    import config


@dataclasses.dataclass(frozen=True)
class Attachment:
    content_id: int
    format: str
    file_path: pathlib.Path
    title: str

    def get_path_str(self):
        return base64.b32encode(str(self.file_path).encode("utf-8")).decode(
            "utf-8"
        )


def _add_attachment(
    cursor: psycopg2_cursor,
    content_id: int,
    _format: str,
    file_path: pathlib.Path,
    title: str,
):
    sql_template = (
        "INSERT INTO attachment (content_id, format, file_path, title) "
        "VALUES (%s, %s, %s, %s)"
    )
    cursor.execute(
        sql_template,
        (
            content_id,
            _format,
            str(file_path.relative_to(config.relative_to)),
            title,
        ),
    )


def add_attachment(
    connection: psycopg2_connection,
    content_id: int,
    _format: str,
    file_path: pathlib.Path,
    title: str,
):
    """
    Adds an attachment to the database for a given content item.

    Args:
        connection (psycopg2_connection): The database connection object.
        content_id (int):
            The ID of the content to which the attachment will be linked.
        _format (str): The format of the attachment (e.g., 'pdf', 'jpg').
        file_path (pathlib.Path | str): The file path to the attachment.
        title (str): The title of the attachment.

    Raises:
        Any exceptions raised by the underlying database operations.
    """
    cursor = connection.cursor()
    _add_attachment(cursor, content_id, _format, file_path, title)
    cursor.close()
    connection.commit()


def _get_attachments_for_content(
    cursor: psycopg2_cursor, content_id: int
) -> list[Attachment]:
    sql_template = "SELECT * FROM attachment where content_id = %s"
    cursor.execute(sql_template, (content_id,))
    raw_results = cursor.fetchall()
    attachments: list[Attachment] = []
    for result in raw_results:
        attachments.append(
            Attachment(
                result[0], result[1], pathlib.Path(result[2]), result[3]
            )
        )
    return attachments


def get_attachments_for_content(
    connection: psycopg2_connection, content_id: int
) -> list[Attachment]:
    """
    Retrieve a list of Attachment objects
    associated with a specific content ID from the database.

    Args:
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.
        content_id (int):
            The ID of the content for which attachments are to be retrieved.

    Returns:
        list[Attachment]:
            A list of Attachment objects linked to the specified content.

    Raises:
        psycopg2.DatabaseError: If a database error occurs during the query.
    """
    cursor = connection.cursor()
    results = _get_attachments_for_content(cursor, content_id)
    cursor.close()
    return results
