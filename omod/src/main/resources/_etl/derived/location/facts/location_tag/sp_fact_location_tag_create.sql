-- $BEGIN

CREATE TABLE mamba_fact_location_tag
(
    location_tag_id int              primary key,
    name            varchar(50)          not null,
    description     varchar(255)         null,
    creator         int                  not null,
    date_created    datetime             not null,
    retired         tinyint(1) default 0 not null,
    retired_by      int                  null,
    date_retired    datetime             null,
    retire_reason   varchar(255)         null,
    uuid            char(38)             not null,
    changed_by      int                  null,
    date_changed    datetime             null,
    incremental_record INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_name (name),
    INDEX mamba_idx_location_tag_id (location_tag_id),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
