from psycopg2.extensions import connection as psycopg2_connection
from psycopg2.extensions import cursor as psycopg2_cursor

import dataclasses
import typing
import abc


class AbstractOriginType(abc.ABC):
    @abc.abstractmethod
    def generate_url(self, origin_content_id: str) -> str:
        pass

    @abc.abstractmethod
    def get_prefix(self) -> str:
        pass


class SimpleOriginType(AbstractOriginType):
    @abc.abstractmethod
    def _get_template_string(self) -> str:
        pass

    def generate_url(self, origin_content_id):
        return self._get_template_string().format(origin_content_id)


class DerpibooruOrigin(SimpleOriginType):
    def _get_template_string(self):
        return "https://derpibooru.org/images/{}"

    def get_prefix(self):
        return "db"


class PonybooruOrigin(SimpleOriginType):
    def _get_template_string(self):
        return "https://ponybooru.org/images/{}"

    def get_prefix(self):
        return "pb"


class TwibooruOrigin(SimpleOriginType):
    def _get_template_string(self):
        return "https://twibooru.org/{}"

    def get_prefix(self):
        return "tb"


class E621Origin(SimpleOriginType):
    def _get_template_string(self):
        return "https://e621.net/posts/{}"

    def get_prefix(self):
        return "ef"


class FurbooruOrigin(SimpleOriginType):
    def _get_template_string(self):
        return "https://furbooru.org/images/{}"

    def get_prefix(self):
        return "fb"


class TantabusAIOrigin(SimpleOriginType):
    def _get_template_string(self):
        return "https://tantabus.ai/images/{}"

    def get_prefix(self):
        return "ta"


class CivitAIOrigin(SimpleOriginType):
    def _get_template_string(self):
        return "https://civitai.com/images/{}"

    def get_prefix(self):
        return "ca"


class FurAffinityOrigin(SimpleOriginType):
    def _get_template_string(self):
        return "https://www.furaffinity.net/view/{}/"

    def get_prefix(self):
        return "fa"


class TwitterXOrigin(AbstractOriginType):
    def generate_url(self, origin_content_id):
        id_parts: list[str] = origin_content_id.split("#")
        if len(id_parts) == 3:
            return (
                f"https://x.com/{id_parts[0]}/status/"
                "{id_parts[1]}/photo/{id_parts[2]}"
            )
        elif len(id_parts) == 2:
            return f"https://x.com/{id_parts[0]}/status/{id_parts[1]}"
        else:
            raise ValueError("Incorrect X (Twitter) ID!")

    def get_prefix(self):
        return "tx-"


ORIGIN_TYPE: dict[str, typing.Type[AbstractOriginType]] = {
    "derpibooru": DerpibooruOrigin,
    "ponybooru": PonybooruOrigin,
    "twibooru": TwibooruOrigin,
    "e621": E621Origin,
    "furbooru": FurbooruOrigin,
    "tantabus": TantabusAIOrigin,
    "furaffinity": FurAffinityOrigin,
    "twitter": TwitterXOrigin,
    "civit ai": CivitAIOrigin,
}


@dataclasses.dataclass(frozen=True)
class Origin:
    medialib_id: int
    origin_name: str
    origin_id: str | None
    alternate: bool

    def generate_url(self) -> str | None:
        if self.origin_id is None:
            return None
        generator = ORIGIN_TYPE.get(self.origin_name, None)
        if generator is None:
            return None
        return generator().generate_url(self.origin_id)

    def get_prefix(self) -> str | None:
        origin_type = ORIGIN_TYPE.get(self.origin_name, None)
        if origin_type is None:
            return None
        return origin_type().get_prefix()


def _get_origins_of_content(
    cursor: psycopg2_cursor, content_id: int
) -> list[Origin]:
    sql_template = "SELECT * FROM origin WHERE medialib_content_id=%s"
    cursor.execute(sql_template, (content_id,))
    raw_results: list[tuple[int, str, str | None, bool]] = cursor.fetchall()
    result: list[Origin] = []
    for raw_result in raw_results:
        result.append(
            Origin(raw_result[0], raw_result[1], raw_result[2], raw_result[3])
        )
    return result


def get_origins_of_content(
    connection: psycopg2_connection, content_id: int
) -> list[Origin]:
    """
    Fetches the origins associated with a specific content item
    from the database.

    Args:
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.
        content_id (int):The unique identifier of the content
            whose origins are to be retrieved.

    Returns:
        list[Origin]:
            A list of Origin objects corresponding to the specified content ID.

    Raises:
        psycopg2.DatabaseError:
            If a database error occurs during the query execution.

    Note:
        Assumes that the 'Origin' class is defined elsewhere
            and matches the structure of the query result.
    """
    cursor = connection.cursor()
    result = _get_origins_of_content(cursor, content_id)
    cursor.close()
    return result


def _find_content(
    cursor: psycopg2_cursor, origin_name: str, origin_id: str
) -> int | None:
    """
    Finds and returns the medialib_content_id associated
    with a given origin name and origin content ID.

    Args:
        cursor (psycopg2_cursor): A database cursor for executing SQL queries.
        origin_name (str): The name of the origin to search for.
        origin_id (str): The content ID from the origin to search for.

    Returns:
        int | None: The medialib_content_id if found, otherwise None.
    """
    sql_query = (
        "SELECT medialib_content_id "
        "from origin where origin_name = %s and origin_content_id = %s"
    )
    cursor.execute(sql_query, (origin_name, origin_id))
    result = cursor.fetchone()
    # it may be important for some reason
    cursor.fetchall()
    if result is not None:
        return result[0]
    else:
        return None


def find_content(
    connection: psycopg2_connection, origin_name: str, origin_id: str
) -> int | None:
    """
    Finds and returns the internal content ID
    corresponding to a given origin name and origin ID.

    Args:
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.
        origin_name (str):
            The name of the content origin (e.g., provider or source).
        origin_id (str):
            The unique identifier of the content in the origin system.

    Returns:
        int | None: The internal content ID if found, otherwise None.
    """
    cursor = connection.cursor()
    result = _find_content(cursor, origin_name, origin_id)
    cursor.close()
    return result


def find_content_from_source(
    origin: str, origin_content_id: str, connection: psycopg2_connection
) -> tuple[int, str] | None:
    """
    Finds and returns content information from the database
    based on the given origin and origin_content_id.

    Args:
        origin (str): The source/origin identifier of the content.
        origin_content_id (str):
            The unique content ID from the specified origin.
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.

    Returns:
        tuple[int, str] | None:
            A tuple containing the content's internal ID
            and file path if found, otherwise None.
    """
    sql_template = "SELECT ID, file_path FROM content " "WHERE id = %s"
    cursor = connection.cursor()
    content_id = _find_content(cursor, origin, origin_content_id)
    if content_id is None:
        return None
    cursor.execute(sql_template, (content_id,))
    result = cursor.fetchone()
    cursor.close()
    return result


def _add_origin(
    cursor: psycopg2_cursor,
    content_id: int,
    origin_name: str,
    origin_id: str | None,
    alternate: bool,
):
    sql_template = "INSERT INTO origin VALUES (%s, %s, %s, %s)"
    cursor.execute(
        sql_template, (content_id, origin_name, origin_id, alternate)
    )


def add_origin(
    connection: psycopg2_connection,
    content_id: int,
    origin_name: str,
    origin_id: str | None,
    alternate: bool,
):
    """
    Adds a new origin entry to the database for a given content item.

    Args:
        connection (psycopg2_connection):
            An active connection to the PostgreSQL database.
        content_id (int):
            The unique identifier of the content
            to which the origin is associated.
        origin_name (str): The name of the origin (e.g., provider or source).
        origin_id (str): The unique identifier for the origin.
        alternate (bool): Indicates whether this origin is an alternate source.

    Raises:
        psycopg2.DatabaseError:
            If a database error occurs during the operation.
    """
    cursor = connection.cursor()
    _add_origin(cursor, content_id, origin_name, origin_id, alternate)
    connection.commit()
    cursor.close()
