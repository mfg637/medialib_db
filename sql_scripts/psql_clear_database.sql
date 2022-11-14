DELETE FROM thumbnail;
DELETE FROM content_tags_list;
DELETE FROM tag_alias;
DELETE FROM content;
DELETE FROM tag;

ALTER SEQUENCE content_id_seq RESTART WITH 1;
ALTER SEQUENCE tag_id_seq RESTART WITH 1;