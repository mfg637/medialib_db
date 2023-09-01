DELETE FROM thumbnail;
DELETE FROM alternate_sources;
DELETE FROM imagehash;
DELETE FROM album_order;
DELETE FROM album;
DELETE FROM representations;
DELETE FROM content_tags_list;
DELETE FROM tag_alias;
DELETE FROM telegram_bot.post;
DELETE FROM content;
DELETE FROM tag;


ALTER SEQUENCE album_id_seq RESTART WITH 1;
ALTER SEQUENCE content_id_seq RESTART WITH 1;
ALTER SEQUENCE tag_id_seq RESTART WITH 1;