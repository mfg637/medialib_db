from medialib_db.common import make_connection
from psycopg2.extensions import connection as psycopg2_connection
from psycopg2.extensions import cursor as psycopg2_cursor


db_connection = make_connection()
sql_search_attachments = (
    "SELECT * FROM representations where compatibility_level = -1"
)
cursor = db_connection.cursor()
cursor.execute(sql_search_attachments, tuple())
results = cursor.fetchall()
print(results)
sql_delete_attachments_from_representations = (
    "DELETE FROM representations where compatibility_level = -1"
)
cursor.execute(sql_delete_attachments_from_representations, tuple())
sql_insert_attachment = (
    "INSERT INTO attachment (content_id, format, file_path, title) "
    "VALUES (%s, %s, %s, %s)"
)
for attachment in results:
    cursor.execute(
        sql_insert_attachment,
        (attachment[0], attachment[1], attachment[3], "ComfyUI Workflow"),
    )
cursor.close()
db_connection.commit()
db_connection.close()
