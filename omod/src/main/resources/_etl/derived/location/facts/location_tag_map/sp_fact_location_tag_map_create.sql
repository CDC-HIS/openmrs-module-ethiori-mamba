-- $BEGIN

CREATE TABLE mamba_fact_location_tag_map
(
    location_id     int not null,
    location_tag_id int not null,
    primary key (location_id, location_tag_id),
    incremental_record INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_location_id (location_id),
    INDEX mamba_idx_location_tag_id (location_tag_id),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
