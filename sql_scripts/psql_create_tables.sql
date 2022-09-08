CREATE DATABASE medialib
  ENCODING = 'utf8mb4'
  LC_COLLATE = 'utf8mb4_unicode_ci';

create type T_CONTENT_TYPE as enum ('image', 'audio', 'video', 'video-loop');

create table content
(
    ID                serial primary key,
    file_path         text                                           not null,
    title             char(64)                                       null,
    content_type      T_CONTENT_TYPE                                 not null,
    description       text                                           null,
    addition_date     timestamp                                      not null,
    origin            char(32)                                       null,
    origin_content_id char(64)                                       null,
    hidden            boolean default FALSE                          not null,
    constraint file_path
        unique (file_path)
);

create type T_CATEGORY as enum
    ('artist', 'set', 'copyright', 'original character', 'rating', 'species', 'content', 'character');

create table tag
(
    ID       bigserial  primary key,
    title    char(32)   null,
    category T_CATEGORY null,
    parent   bigint     null,
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
    tag_id bigint          not null,
    title  char(64)        not null,
    constraint title
        unique (title),
    constraint tag_alias_FK
        foreign key (tag_id) references tag (ID)
);

CREATE PROCEDURE get_tags_ids (IN tag_alias_name VARCHAR(251))
language sql
as $$
begin;
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
end;$$;

CREATE TABLE thumbnail
(
	content_id      int4      not null,
	width           int2      not null,
	height          int2      not null,
	generation_date timestamp not null,
	format          char(8)   not null,
	file_path       text      not null,
    primary key (content_id, width, height, format),
	foreign key (content_id) references content(ID)
);
