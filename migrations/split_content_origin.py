from medialib_db.common import make_connection
from psycopg2.extensions import connection as psycopg2_connection
from psycopg2.extensions import cursor as psycopg2_cursor


connection: psycopg2_connection = make_connection()
cursor: psycopg2_cursor = connection.cursor()

# Collect data from content
cursor.execute(
    (
        "SELECT id, origin, origin_content_id, FALSE "
        "FROM content WHERE origin IS NOT NULL"
    )
)
content_rows = cursor.fetchall()

# Collect data from alternate_sources
cursor.execute(
    (
        "SELECT content_id, origin, origin_content_id, TRUE "
        "FROM alternate_sources WHERE origin IS NOT NULL"
    )
)
alternate_rows = cursor.fetchall()

# Combine all rows
all_rows = content_rows + alternate_rows

# Insert into origin table
insert_sql = """
    INSERT INTO origin
    (medialib_content_id, origin_name, origin_content_id, alternate)
    VALUES (%s, %s, %s, %s)
"""

try:
    cursor.executemany(insert_sql, all_rows)
    connection.commit()
    print(f"Inserted {cursor.rowcount} rows into origin table.")
except Exception as e:
    connection.rollback()
    print("Error inserting data:", e)
finally:
    connection.close()
