-- $BEGIN

CREATE TABLE mamba_fact_location_attribute_type
(
    location_attribute_type_id int                  primary key,
    name                       varchar(255)         not null,
    description                varchar(1024)        null,
    datatype                   varchar(255)         null,
    datatype_config            text                 null,
    preferred_handler          varchar(255)         null,
    handler_config             text                 null,
    min_occurs                 int                  not null,
    max_occurs                 int                  null,
    creator                    int                  not null,
    date_created               datetime             not null,
    changed_by                 int                  null,
    date_changed               datetime             null,
    retired                    tinyint(1) default 0 not null,
    retired_by                 int                  null,
    date_retired               datetime             null,
    retire_reason              varchar(255)         null,
    uuid                       char(38)             not null,
    incremental_record INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_location_attribute_type_id (location_attribute_type_id),
    INDEX mamba_idx_name (name),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
