from typing import TypeVar
from html import escape as html_escape

try:
    import psycopg2
except ImportError:
    raise Exception("Connector psycopq2 not properly installed")

try:
    from .. import config
except ImportError:
    import config

try:
    from . import backup
except ImportError:
    pass


CONTENT_TITLE_MAX_SIZE = 64
CONTENT_ORIGIN_MAX_SIZE = 32
CONTENT_ORIGIN_ID_MAX_SIZE = 128
TAG_TITLE_MAX_SIZE = 240
TAG_ALIAS_MAX_SIZE = 255
THUMBNAIL_FORMAT_MAX_SIZE = 8


def make_connection():
    return psycopg2.connect(
        host=config.db_host,
        database=config.db_name,
        user=config.db_user,
        password=config.db_password,  # type: ignore
    )


def sanitize_string(input_str: str | None) -> str | None:
    if input_str is None:
        return None
    return html_escape(str(input_str).replace("\x00", ""))


def postgres_string_format(input_string: str | None, size: int) -> str | None:
    """
    Check that `tag_name` is shorter than `size`.
    If len of `tag_name` exceeds `size`,
    copy first words that fits in `size` limit,
    and add "…" in the end.
    If `tag_name` is none, just return None.
    """
    if input_string is None:
        return None
    string_to_validate = sanitize_string(input_string)
    if len(string_to_validate) > size:
        words = []
        if "_" in input_string:
            words = input_string.split("_")
        else:
            words = input_string.split(" ")
        words_copy = 1
        words_count = len(words)
        string_to_validate = sanitize_string(
            " ".join(words[: words_copy + 1]) + "…"
        )
        if len(string_to_validate) < size:
            while (
                len(" ".join(words[: words_copy + 1]) + "…") < size
                and words_copy < words_count
            ):
                words_copy += 1
                string_to_validate = sanitize_string(
                    " ".join(words[: words_copy + 1]) + "…"
                )
            formatted_string = sanitize_string(
                " ".join(words[: words_copy + 1]) + "…"
            )
            return formatted_string
        elif len(string_to_validate) > size:
            characters_copy = size - 1
            string_to_validate = sanitize_string(
                input_string[:characters_copy]+"…"
            )
            while len(string_to_validate) > size:
                characters_copy -= 1
                string_to_validate = sanitize_string(
                    input_string[:characters_copy]+"…"
                )
            formatted_string = sanitize_string(
                input_string[:characters_copy]+"…"
            )
            return formatted_string
        return string_to_validate
    return string_to_validate


def get_value_or_fail(
    db_record: tuple | None, error_message: str | Exception, _index: int = 0
):
    """
    Retrieve a value from a database record tuple
    or raise an error if the record is None.

    Args:
        db_record (tuple[T] | None):
            The database record as a tuple, or None if not found.
        error_message (str | Exception):
            The error message or Exception to raise if db_record is None.
        _index (int, optional):
            The index of the value to retrieve from the tuple. Defaults to 0.

    Returns:
        T: The value at the specified index in the db_record tuple.

    Raises:
        Exception: If db_record is None and error_message is a string.
        Exception: If db_record is None
            and error_message is an Exception instance (raises it directly).
    """
    if db_record is not None:
        return db_record[_index]
    else:
        if isinstance(error_message, Exception):
            raise error_message
        else:
            raise Exception(error_message)
