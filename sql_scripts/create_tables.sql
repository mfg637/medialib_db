create table content
(
    ID                bigint unsigned auto_increment
        primary key,
    file_path         varchar(255)                                   not null UNIQUE,
    title             varchar(255)                                   null,
    content_type      enum ('image', 'audio', 'video', 'video-loop') not null,
    description       text                                           null,
    addition_date     datetime                                       not null,
    origin            varchar(32)                                    null,
    origin_content_id varchar(255)                                   null
)
    collate = utf8mb4_unicode_ci;

create table tag
(
    ID       bigint unsigned auto_increment
        primary key,
    title    varchar(250)                                                                                            null,
    category enum ('artist', 'set', 'copyright', 'original character', 'rating', 'species', 'content', 'characters') null,
    parent   bigint unsigned                                                                                         null,
    constraint uniq_tag
        unique (title, category),
    constraint tag_ibfk_1
        foreign key (parent) references tag (ID)
)
    collate = utf8mb4_unicode_ci;

create index parent
    on tag (parent);

create table content_tags_list
(
    content_id bigint unsigned not null,
    tag_id     bigint unsigned not null,
    constraint content_tags_list_FK
        foreign key (content_id) references content (ID),
    constraint content_tags_list_FK_1
        foreign key (tag_id) references tag (ID)
);

create table tag_alias
(
    ID     bigint unsigned auto_increment
        primary key,
    tag_id bigint unsigned not null,
    title  varchar(251)    not null,
    constraint title
        unique (title),
    constraint tag_alias_FK
        foreign key (tag_id) references tag (ID)
)
    charset = utf16;

CREATE PROCEDURE get_tags_ids (IN tag_aliace_name VARCHAR(251))
BEGIN
    # taken from https://stackoverflow.com/a/33737203
    with recursive get_tags_ids_r (id, parent_id) as (
        select      ID,
                    parent
        from        tag
        where       ID = (SELECT tag_id from tag_alias where tag_alias.title=tag_aliace_name COLLATE utf8mb4_unicode_ci)
        union all
        select      t.ID,
                    t.parent
        from        tag t
        inner join  get_tags_ids_r as t_rec
                on  t.parent = t_rec.ID
    )
    select ID from get_tags_ids_r;
end;
