import common
import tags_indexer


def main():
    mysql_connection = common.mysql_make_connection()
    postgresql_connection = common.make_connection()

    mysql_cursor = mysql_connection.cursor()
    postgres_cursor = postgresql_connection.cursor()

    mysql_get_all_tags = "SELECT ID, title, category from tag"
    postgres_insert_new_tag = \
        "INSERT INTO tag (id, title, category) VALUES (DEFAULT, %s, %s) RETURNING id"
    mysql_cursor.execute(mysql_get_all_tags, tuple())
    tags_list = mysql_cursor.fetchall()
    mysql_get_tag_aliases = "SELECT title FROM tag_alias WHERE tag_id = %s"
    postgres_insert_tag_alias = "INSERT INTO tag_alias (tag_id, title) VALUES (%s, %s)"
    postgres_get_tag_id = "SELECT id FROM tag WHERE title = %s and category = %s"

    exists_tags_list = set()

    for tag_data in tags_list:
        if tag_data[2] == "original character":
            tag = (common.postgres_string_format(tag_data[1], 32), "character")
        else:
            tag = (common.postgres_string_format(tag_data[1], 32), tag_data[2])
        if tag not in exists_tags_list:
            exists_tags_list.add(tag)
            postgres_cursor.execute(
                postgres_insert_new_tag,
                tag
            )
            postgres_tag_id = postgres_cursor.fetchone()[0]
        # mysql_cursor.execute(mysql_get_tag_aliases, (tag_data[0],))
        # raw_tag_aliases_list = mysql_cursor.fetchall()
        # for raw_tag_alias_data in raw_tag_aliases_list:
        #     postgres_cursor.execute(
        #         postgres_insert_tag_alias,
        #         (
        #             postgres_string_format(postgres_tag_id, 64), raw_tag_alias_data[0]
        #         )
        #     )
            tag_alias = tag[0]
            if tag[1] == "artist":
                tag_alias = "artist:{}".format(tag[0])
            elif tag[1] in "character":
                tag_alias = "character:{}".format(tag[0])
            elif tag[1] in "copyright":
                tag_alias = "copyright:{}".format(tag[0])
            postgres_cursor.execute(
                postgres_insert_tag_alias,
                (
                    postgres_tag_id, tag_alias
                )
            )
            postgresql_connection.commit()
    mysql_get_tags_with_parents = (
        "SELECT title, category, parent FROM tag WHERE parent IS NOT NULL"
    )
    mysql_get_tag_by_id = "SELECT title, category FROM tag WHERE ID = %s"
    postgres_set_parent_tag = "UPDATE tag SET parent = %s WHERE id = %s"
    mysql_cursor.execute(mysql_get_tags_with_parents, tuple())
    tags_with_parents = mysql_cursor.fetchall()
    for current_tag in tags_with_parents:
        current_category = current_tag[1]
        if current_category == "original character":
            current_category = "character"
        postgres_cursor.execute(postgres_get_tag_id, (current_tag[0], current_category))
        postgres_current_tag_id = postgres_cursor.fetchone()[0]
        mysql_cursor.execute(mysql_get_tag_by_id, (current_tag[2],))
        parent_tag_data = mysql_cursor.fetchone()
        parent_tag_category = parent_tag_data[1]
        if parent_tag_category == "original character":
            parent_tag_category = "character"
        postgres_cursor.execute(
            postgres_get_tag_id, (parent_tag_data[0], parent_tag_category)
        )
        postgres_parent_tag_id = postgres_cursor.fetchone()[0]
        postgres_cursor.execute(
            postgres_set_parent_tag, (postgres_parent_tag_id, postgres_current_tag_id)
        )
        postgresql_connection.commit()

    # mysql_get_content = "SELECT * FROM content"
    # mysql_cursor.execute(mysql_get_content, tuple())
    # raw_content_list = mysql_cursor.fetchall()
    # mysql_get_tags_by_content_id = (
    #     "SELECT title, category FROM tag WHERE ID IN "
    #     "(SELECT tag_id from content_tags_list WHERE content_id = %s)"
    # )
    # postgres_insert_content = (
    #     "INSERT INTO content "
    #     "(id, file_path, title, content_type, description, addition_date, origin, origin_content_id, hidden) "
    #     "VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
    # )
    # postgres_content_tag_connect = (
    #     "INSERT INTO content_tags_list (content_id, tag_id) VALUES (%s, %s)"
    # )
    # for raw_content_data in raw_content_list:
    #     content_data = []
    #     content_data.append(raw_content_data[1])
    #     content_data.append(postgres_string_format(raw_content_data[2], 64))
    #     content_data.extend(raw_content_data[3:-1])
    #     content_data.append(bool(raw_content_data))
    #     postgres_cursor.execute(
    #         postgres_insert_content,
    #         tuple(content_data)
    #     )
    #     postgres_content_id = postgres_cursor.fetchone()
    #     postgresql_connection.commit()


if __name__ == "__main__":
    main()