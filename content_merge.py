import pathlib

import config

try:
    from . import common
except ImportError:
    import common

import content
import origin


if __name__ == "__main__":
    content_to_merge_id = int(input("Type content_id from content to merge: "))
    destination_content_id = int(input("Type destination content_id: "))

    print(
        "Merging content {} → {}".format(
            content_to_merge_id, destination_content_id
        )
    )

    connection = common.make_connection()
    cursor = connection.cursor()

    print("Loading tags to merge…")

    print(
        "This tags will be merged with {} source".format(
            destination_content_id
        )
    )
    sql_get_source_exclusive_tags = (
        "select title, category from tag where ID in "
        "(select tag_id from content_tags_list where content_id = %s) and not id in "
        "(select tag_id from content_tags_list where content_id = %s);"
    )
    cursor.execute(
        sql_get_source_exclusive_tags,
        (content_to_merge_id, destination_content_id),
    )
    taglist = cursor.fetchall()
    for tag in taglist:
        print(tag)
    print()

    print("Loading thumbnails to remove…")

    sql_get_thumbnail_file_names = (
        "select file_path from thumbnail where content_id = %s"
    )
    cursor.execute(sql_get_thumbnail_file_names, (content_to_merge_id,))
    thumbnails_file_paths = []

    print("These thumbnail files will be removed:")
    thumbnails_file_name = cursor.fetchone()
    while thumbnails_file_name is not None:
        thumbnails_file_path = config.thumbnails_storage.joinpath(
            thumbnails_file_name[0]
        )
        print(thumbnails_file_path)
        thumbnails_file_paths.append(thumbnails_file_path)
        thumbnails_file_name = cursor.fetchone()
    print()

    print("Loading content metadata: {}".format(content_to_merge_id))
    content_to_merge_metadata = content._get_content_metadata_by_id(
        content_to_merge_id, cursor
    )
    can_have_representations = False
    delete_content_file = True
    if content_to_merge_metadata is None:
        print("Error: content doesn't exists")
        exit(-1)
    file_path = config.relative_to.joinpath(
        content_to_merge_metadata.file_path
    )
    print("File path:", file_path)
    print("Title:", content_to_merge_metadata.title)
    print("Type:", content_to_merge_metadata.content_type)
    print("Description:", content_to_merge_metadata.description)
    print("Register date:", content_to_merge_metadata.addition_date)
    print("Is it hidden:", content_to_merge_metadata.hidden)
    if (
        content_to_merge_metadata.title is not None
        or content_to_merge_metadata.description is not None
    ):
        print("Warning: title and description wouldn't to be merged.")
    if content_to_merge_metadata.file_path.suffix == ".srs":
        can_have_representations = True
        print("Content may probably have representations.")
    elif content_to_merge_metadata.file_path.suffix == ".mpd":
        delete_content_file = False

    print("Loading content origins: {}".format(content_to_merge_id))
    origins = origin._get_origins_of_content(cursor, content_to_merge_id)
    for index, current_origin in enumerate(origins):
        print(
            (
                f"origin # {index} "
                f"({"alternate" if current_origin.alternate else "primary"})"
            )
        )
        print("Source origin:", current_origin.origin_name)
        print("Source origin ID:", current_origin.origin_id)

    representations_list: list[pathlib.Path] = []

    if can_have_representations:
        print("Check for representations…")
        sql_representations_check = (
            "select file_path from representations where content_id = %s"
        )
        cursor.execute(sql_representations_check, (content_to_merge_id,))
        print("These files will be removed:")
        representation_file_name = cursor.fetchone()
        while representation_file_name is not None:
            representation_file = config.relative_to.joinpath(
                representation_file_name[0]
            )
            print(representation_file)
            representations_list.append(representation_file)
            representation_file_name = cursor.fetchone()
        if len(representations_list) == 0:
            print("No registered representations files")
            delete_content_file = False
    if not delete_content_file:
        print(
            (
                "Warning: Content have unknown number of representations. "
                "You'll have to delete it by yourself."
            )
        )

    continue_flag = bool(input("Continue? (y/n): ") == "y")
    if continue_flag:
        print("Starting merging…")
        sql_copy_tags = (
            "insert into content_tags_list (content_id, tag_id) "
            "select %s, ID from tag where ID in "
            "(select tag_id from content_tags_list where content_id = %s) "
            "and not id in "
            "(select tag_id from content_tags_list where content_id = %s);"
        )
        print("Copying tags to content destination…")
        cursor.execute(
            sql_copy_tags,
            (
                destination_content_id,
                content_to_merge_id,
                destination_content_id,
            ),
        )
        sql_remove_tags = (
            "delete from content_tags_list where content_id = %s;"
        )
        print("Deleting tags…")
        cursor.execute(sql_remove_tags, (content_to_merge_id,))
        sql_remove_thumbnails = "delete from thumbnail where content_id = %s;"
        print("Add alternate source")
        if len(origins):
            for current_origin in origins:
                if current_origin.origin_id is not None:
                    origin._add_origin(
                        cursor,
                        destination_content_id,
                        current_origin.origin_name,
                        current_origin.origin_id,
                        True,
                    )
            origin._purge_origins(cursor, content_to_merge_id)
        print("Deleting thumbnails metadata…")
        cursor.execute(sql_remove_thumbnails, (content_to_merge_id,))
        sql_remove_imagehash = "delete from imagehash where content_id = %s;"
        print("Remove image hash…")
        cursor.execute(sql_remove_imagehash, (content_to_merge_id,))
        if len(representations_list) > 0:
            sql_remove_reps = (
                "delete from representations where content_id = %s"
            )
            print("Removing representations from database…")
            cursor.execute(sql_remove_reps, (content_to_merge_id,))
        sql_remove_content = "delete from content where ID = %s"
        print("Removing content metadata…")
        cursor.execute(sql_remove_content, (content_to_merge_id,))
        print("Committing transaction…")
        connection.commit()
        print(("Successful transaction commit. " "Deleting files…"))
        print("Deleting thumbnails…")
        for thumbnail in thumbnails_file_paths:
            thumbnail.unlink(missing_ok=True)
        if len(representations_list) > 0:
            print("Deleting representations…")
            for representation in representations_list:
                representation.unlink(missing_ok=True)
        if delete_content_file:
            print("Deleting content file…")
            file_path.unlink(missing_ok=True)
        print("Successful content merge!")

    cursor.close()
    connection.close()
