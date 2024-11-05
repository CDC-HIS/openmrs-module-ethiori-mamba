-- $BEGIN

CREATE TABLE mamba_fact_location_attribute
(
    location_attribute_id int           primary key,
    location_id           int                  not null,
    attribute_type_id     int                  not null,
    value_reference       text                 not null,
    uuid                  char(38)             not null,
    creator               int                  not null,
    date_created          datetime             not null,
    changed_by            int                  null,
    date_changed          datetime             null,
    voided                tinyint(1) default 0 not null,
    voided_by             int                  null,
    date_voided           datetime             null,
    void_reason           varchar(255)         null,
    incremental_record INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_location_attribute_id (location_attribute_id),
    INDEX mamba_idx_location_id (location_id),
    INDEX mamba_idx_attribute_type_id (attribute_type_id),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
