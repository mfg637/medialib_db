CREATE DATABASE medialib
  ENCODING = 'utf8mb4'
  LC_COLLATE = 'utf8mb4_unicode_ci';

create type T_CONTENT_TYPE as enum ('image', 'audio', 'video', 'video-loop');

create table content
(
    ID                serial primary key,
    file_path         text                                           not null,
    title             varchar(64)                                    null,
    content_type      T_CONTENT_TYPE                                 not null,
    description       text                                           null,
    addition_date     timestamp                                      not null,
    origin            varchar(32)                                    null,
    origin_content_id varchar(128)                                   null,
    hidden            boolean default FALSE                          not null,
    constraint file_path
        unique (file_path)
);

create type T_CATEGORY as enum
    ('artist', 'set', 'copyright', 'rating', 'species', 'content', 'character');

create table tag
(
    ID       bigserial      primary key,
    title    varchar(240)   null,
    category T_CATEGORY     null,
    parent   bigint         null,
    constraint uniq_tag
        unique (title, category),
    constraint tag_ibfk_1
        foreign key (parent) references tag (ID)
);

create index parent
    on tag (parent);

create table content_tags_list
(
    content_id int4   not null,
    tag_id     bigint not null,
    constraint content_tags_list_FK
        foreign key (content_id) references content (ID),
    constraint content_tags_list_FK_1
        foreign key (tag_id) references tag (ID),
    constraint uniq_keys UNIQUE (content_id, tag_id)
);

create table tag_alias
(
    tag_id bigint              not null,
    title  varchar(255)        not null,
    constraint title
        unique (title),
    constraint tag_alias_FK
        foreign key (tag_id) references tag (ID)
);

create index tag_alias_index on tag_alias (title);

CREATE FUNCTION get_tags_ids (IN tag_alias_name varchar(255))
RETURNS TABLE (id bigint)
language sql as $$
    -- taken from https://stackoverflow.com/a/33737203
    with recursive get_tags_ids_r (id, parent_id) as (
        select      ID,
                    parent
        from        tag
        where       ID = (SELECT tag_id from tag_alias where tag_alias.title=tag_alias_name)
        union all
        select      t.ID,
                    t.parent
        from        tag t
        inner join  get_tags_ids_r as t_rec
                on  t.parent = t_rec.ID
    )
    select ID from get_tags_ids_r;
$$;

CREATE TABLE thumbnail
(
	content_id      int4         not null,
	width           int2         not null,
	height          int2         not null,
	generation_date timestamp    not null,
	format          varchar(8)   not null,
	file_path       text         not null,
    primary key (content_id, width, height, format),
	foreign key (content_id) references content(ID)
);

create table representations (
    content_id integer    not null
        references content,
    width           smallint   null,
    height          smallint   null,
    format          varchar(8) not null,
    compatibility_level smallint null,
    file_path       text       not null
);

create table imagehash (
    content_id integer
        not null
        unique
        references content,
    aspect_ratio real not null,
    hs_hash int not null,
    value_hash bigint not null,
    alternate_version boolean not null default false
);

create index imagehash_index on imagehash (
    value_hash, hs_hash
);

create table imagehash (
    content_id integer
        not null
        unique
        references content,
    aspect_ratio real not null,
    hue_hash bigint not null,
    saturation_hash bigint not null,
    value_hash bytea not null,
    alternate_version boolean not null default false
);

create index imagehash_index on imagehash (
    value_hash, hue_hash, saturation_hash
);

-- album is an ordered set of media content
-- this table represent general information about an album
-- ID field needed to prevent repeating set id and album artist id in each
--      content to album relation
create table album (
    ID                  serial primary key,
    set_tag_id          integer not null references tag,
    album_artist_tag_id integer not null references tag,
    constraint uniq_album_id unique (set_tag_id, album_artist_tag_id)
);

-- tag types validation trigger function
create function new_album() RETURNS trigger AS $new_album$
    begin
        if (select tag.category from tag where ID = NEW.set_tag_id) != 'set' then
            raise exception 'tag_id_set points to non set tag type';
        end if;
        if (select tag.category from tag where ID = NEW.album_artist_tag_id) != 'artist' then
            raise exception 'tag_id_artist points to non artist tag type';
        end if;
        return NEW;
    end;
$new_album$ LANGUAGE plpgsql;


CREATE TRIGGER new_album BEFORE INSERT OR UPDATE ON album
    FOR EACH ROW EXECUTE FUNCTION new_album();

create table album_order (
    album_id integer not null references album,
    content_id integer not null references content,
    "order" integer not null
);
