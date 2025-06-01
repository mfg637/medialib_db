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
        host=config.db_host, database=config.db_name, user=config.db_user, password=config.db_password
    )


def postgres_string_format(tag_name: str | None, size: int) -> str | None:
    """
    Check that `tag_name` is shorter than `size`.
    If len of `tag_name` exceeds `size`,
    copy first words that fits in `size` limit,
    and add "…" in the end.
    If `tag_name` is none, just return None.
    """
    if tag_name is None:
        return None
    elif len(tag_name) > size:
        words = []
        if "_" in tag_name:
            words = tag_name.split("_")
        else:
            words = tag_name.split(" ")
        new_tag_name = ""
        words_copy = 1
        words_count = len(words)
        while len(" ".join(words[:words_copy + 1])+"…") < size \
                and words_copy < words_count:
            words_copy += 1
        new_tag_name = " ".join(words[:words_copy])+"…"
        return new_tag_name[:size]
    return tag_name
