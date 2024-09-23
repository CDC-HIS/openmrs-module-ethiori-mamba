CREATE database IF NOT EXISTS mamba_etl_db;
~-~-

USE mamba_etl_db;
~-~-


        
    
        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_calculate_agegroup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_calculate_agegroup;


~-~-
CREATE FUNCTION fn_mamba_calculate_agegroup(age INT) RETURNS VARCHAR(15)
    DETERMINISTIC
BEGIN
    DECLARE agegroup VARCHAR(15);
    CASE
        WHEN age < 1 THEN SET agegroup = '<1';
        WHEN age between 1 and 4 THEN SET agegroup = '1-4';
        WHEN age between 5 and 9 THEN SET agegroup = '5-9';
        WHEN age between 10 and 14 THEN SET agegroup = '10-14';
        WHEN age between 15 and 19 THEN SET agegroup = '15-19';
        WHEN age between 20 and 24 THEN SET agegroup = '20-24';
        WHEN age between 25 and 29 THEN SET agegroup = '25-29';
        WHEN age between 30 and 34 THEN SET agegroup = '30-34';
        WHEN age between 35 and 39 THEN SET agegroup = '35-39';
        WHEN age between 40 and 44 THEN SET agegroup = '40-44';
        WHEN age between 45 and 49 THEN SET agegroup = '45-49';
        WHEN age between 50 and 54 THEN SET agegroup = '50-54';
        WHEN age between 55 and 59 THEN SET agegroup = '55-59';
        WHEN age between 60 and 64 THEN SET agegroup = '60-64';
        ELSE SET agegroup = '65+';
        END CASE;

    RETURN agegroup;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_get_obs_value_column  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_get_obs_value_column;


~-~-
CREATE FUNCTION fn_mamba_get_obs_value_column(conceptDatatype VARCHAR(20)) RETURNS VARCHAR(20)
    DETERMINISTIC
BEGIN
    DECLARE obsValueColumn VARCHAR(20);

        IF conceptDatatype = 'Text' THEN
            SET obsValueColumn = 'obs_value_text';

        ELSEIF conceptDatatype = 'Coded'
           OR conceptDatatype = 'N/A' THEN
            SET obsValueColumn = 'obs_value_text';

        ELSEIF conceptDatatype = 'Boolean' THEN
            SET obsValueColumn = 'obs_value_boolean';

        ELSEIF  conceptDatatype = 'Date'
                OR conceptDatatype = 'Datetime' THEN
            SET obsValueColumn = 'obs_value_datetime';

        ELSEIF conceptDatatype = 'Numeric' THEN
            SET obsValueColumn = 'obs_value_numeric';

        ELSE
            SET obsValueColumn = 'obs_value_text';

        END IF;

    RETURN (obsValueColumn);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_age_calculator  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_age_calculator;


~-~-
CREATE FUNCTION fn_mamba_age_calculator(birthdate DATE, deathDate DATE) RETURNS INTEGER
    DETERMINISTIC
BEGIN
    DECLARE today DATE;
    DECLARE age INT;

    -- Check if birthdate is not null and not an empty string
    IF birthdate IS NULL OR TRIM(birthdate) = '' THEN
        RETURN NULL;
    ELSE
        SET today = IFNULL(CURDATE(), '0000-00-00');
        -- Check if birthdate is a valid date using STR_TO_DATE and if it's not in the future
        IF STR_TO_DATE(birthdate, '%Y-%m-%d') IS NULL OR STR_TO_DATE(birthdate, '%Y-%m-%d') > today THEN
            RETURN NULL;
        END IF;

        -- If deathDate is provided and in the past, set today to deathDate
        IF deathDate IS NOT NULL AND today > deathDate THEN
            SET today = deathDate;
        END IF;

        SET age = YEAR(today) - YEAR(birthdate);

        -- Adjust age based on month and day
        IF MONTH(today) < MONTH(birthdate) OR (MONTH(today) = MONTH(birthdate) AND DAY(today) < DAY(birthdate)) THEN
            SET age = age - 1;
        END IF;

        RETURN age;
    END IF;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_get_datatype_for_concept  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_get_datatype_for_concept;


~-~-
CREATE FUNCTION fn_mamba_get_datatype_for_concept(conceptDatatype VARCHAR(20)) RETURNS VARCHAR(20)
    DETERMINISTIC
BEGIN
    DECLARE mysqlDatatype VARCHAR(20);


    IF conceptDatatype = 'Text' THEN
        SET mysqlDatatype = 'TEXT';

    ELSEIF conceptDatatype = 'Coded'
        OR conceptDatatype = 'N/A' THEN
        SET mysqlDatatype = 'VARCHAR(250)';

    ELSEIF conceptDatatype = 'Boolean' THEN
        SET mysqlDatatype = 'BOOLEAN';

    ELSEIF conceptDatatype = 'Date' THEN
        SET mysqlDatatype = 'DATE';

    ELSEIF conceptDatatype = 'Datetime' THEN
        SET mysqlDatatype = 'DATETIME';

    ELSEIF conceptDatatype = 'Numeric' THEN
        SET mysqlDatatype = 'DOUBLE';

    ELSE
        SET mysqlDatatype = 'TEXT';

    END IF;

    RETURN mysqlDatatype;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_generate_json_from_mamba_flat_table_config  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_generate_json_from_mamba_flat_table_config;


~-~-
CREATE FUNCTION fn_mamba_generate_json_from_mamba_flat_table_config(
    is_incremental TINYINT(1)
) RETURNS JSON
    DETERMINISTIC
BEGIN
    DECLARE report_array JSON;
    SET session group_concat_max_len = 200000;

    SELECT CONCAT('{"flat_report_metadata":[', GROUP_CONCAT(
            CONCAT(
                    '{',
                    '"report_name":', JSON_EXTRACT(table_json_data, '$.report_name'),
                    ',"flat_table_name":', JSON_EXTRACT(table_json_data, '$.flat_table_name'),
                    ',"encounter_type_uuid":', JSON_EXTRACT(table_json_data, '$.encounter_type_uuid'),
                    ',"table_columns": ', JSON_EXTRACT(table_json_data, '$.table_columns'),
                    '}'
            ) SEPARATOR ','), ']}')
    INTO report_array
    FROM mamba_flat_table_config
    WHERE (IF(is_incremental = 1, incremental_record = 1, 1));

    RETURN report_array;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_array_length  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_array_length;

~-~-
CREATE FUNCTION fn_mamba_array_length(array_string TEXT) RETURNS INT
    DETERMINISTIC
BEGIN
  DECLARE length INT DEFAULT 0;
  DECLARE i INT DEFAULT 1;

  -- If the array_string is not empty, initialize length to 1
    IF TRIM(array_string) != '' AND TRIM(array_string) != '[]' THEN
        SET length = 1;
    END IF;

  -- Count the number of commas in the array string
    WHILE i <= CHAR_LENGTH(array_string) DO
        IF SUBSTRING(array_string, i, 1) = ',' THEN
          SET length = length + 1;
        END IF;
        SET i = i + 1;
    END WHILE;

RETURN length;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_get_array_item_by_index  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_get_array_item_by_index;

~-~-
CREATE FUNCTION fn_mamba_get_array_item_by_index(array_string TEXT, item_index INT) RETURNS TEXT
    DETERMINISTIC
BEGIN
  DECLARE elem_start INT DEFAULT 1;
  DECLARE elem_end INT DEFAULT 0;
  DECLARE current_index INT DEFAULT 0;
  DECLARE result TEXT DEFAULT '';

    -- If the item_index is less than 1 or the array_string is empty, return an empty string
    IF item_index < 1 OR array_string = '[]' OR TRIM(array_string) = '' THEN
        RETURN '';
    END IF;

    -- Loop until we find the start quote of the desired index
    WHILE current_index < item_index DO
        -- Find the start quote of the next element
        SET elem_start = LOCATE('"', array_string, elem_end + 1);
        -- If we can't find a new element, return an empty string
        IF elem_start = 0 THEN
          RETURN '';
        END IF;

        -- Find the end quote of this element
        SET elem_end = LOCATE('"', array_string, elem_start + 1);
        -- If we can't find the end quote, return an empty string
        IF elem_end = 0 THEN
          RETURN '';
        END IF;

        -- Increment the current_index
        SET current_index = current_index + 1;
    END WHILE;

    -- When the loop exits, current_index should equal item_index, and elem_start/end should be the positions of the quotes
    -- Extract the element
    SET result = SUBSTRING(array_string, elem_start + 1, elem_end - elem_start - 1);

    RETURN result;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_json_array_length  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_json_array_length;

~-~-
CREATE FUNCTION fn_mamba_json_array_length(json_array TEXT) RETURNS INT
    DETERMINISTIC
BEGIN
    DECLARE array_length INT DEFAULT 0;
    DECLARE current_pos INT DEFAULT 1;
    DECLARE char_val CHAR(1);

    IF json_array IS NULL THEN
        RETURN 0;
    END IF;

  -- Iterate over the string to count the number of objects based on commas and curly braces
    WHILE current_pos <= CHAR_LENGTH(json_array) DO
        SET char_val = SUBSTRING(json_array, current_pos, 1);

    -- Check for the start of an object
        IF char_val = '{' THEN
            SET array_length = array_length + 1;

      -- Move current_pos to the end of this object
            SET current_pos = LOCATE('}', json_array, current_pos) + 1;
        ELSE
            SET current_pos = current_pos + 1;
        END IF;
    END WHILE;

RETURN array_length;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_json_extract  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_json_extract;

~-~-
CREATE FUNCTION fn_mamba_json_extract(json TEXT, key_name VARCHAR(255)) RETURNS VARCHAR(255)
    DETERMINISTIC
BEGIN
  DECLARE start_index INT;
  DECLARE end_index INT;
  DECLARE key_length INT;
  DECLARE key_index INT;

  SET key_name = CONCAT( key_name, '":');
  SET key_length = CHAR_LENGTH(key_name);
  SET key_index = LOCATE(key_name, json);

    IF key_index = 0 THEN
        RETURN NULL;
    END IF;

    SET start_index = key_index + key_length;

    CASE
        WHEN SUBSTRING(json, start_index, 1) = '"' THEN
            SET start_index = start_index + 1;
            SET end_index = LOCATE('"', json, start_index);
        ELSE
            SET end_index = LOCATE(',', json, start_index);
            IF end_index = 0 THEN
                SET end_index = LOCATE('}', json, start_index);
            END IF;
    END CASE;

RETURN SUBSTRING(json, start_index, end_index - start_index);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_json_extract_array  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_json_extract_array;

~-~-
CREATE FUNCTION fn_mamba_json_extract_array(json TEXT, key_name VARCHAR(255)) RETURNS TEXT
    DETERMINISTIC
BEGIN
DECLARE start_index INT;
DECLARE end_index INT;
DECLARE array_text TEXT;

    SET key_name = CONCAT('"', key_name, '":');
    SET start_index = LOCATE(key_name, json);

    IF start_index = 0 THEN
        RETURN NULL;
    END IF;

    SET start_index = start_index + CHAR_LENGTH(key_name);

    IF SUBSTRING(json, start_index, 1) != '[' THEN
        RETURN NULL;
    END IF;

    SET start_index = start_index + 1; -- Start after the '['
    SET end_index = start_index;

    -- Loop to find the matching closing bracket for the array
    SET @bracket_counter = 1;
    WHILE @bracket_counter > 0 AND end_index <= CHAR_LENGTH(json) DO
        SET end_index = end_index + 1;
        IF SUBSTRING(json, end_index, 1) = '[' THEN
          SET @bracket_counter = @bracket_counter + 1;
        ELSEIF SUBSTRING(json, end_index, 1) = ']' THEN
          SET @bracket_counter = @bracket_counter - 1;
        END IF;
    END WHILE;

    IF @bracket_counter != 0 THEN
        RETURN NULL; -- The brackets are not balanced, return NULL
    END IF;

SET array_text = SUBSTRING(json, start_index, end_index - start_index);

RETURN array_text;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_json_extract_object  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_json_extract_object;

~-~-
CREATE FUNCTION fn_mamba_json_extract_object(json_string TEXT, key_name VARCHAR(255)) RETURNS TEXT
    DETERMINISTIC
BEGIN
  DECLARE start_index INT;
  DECLARE end_index INT;
  DECLARE nested_level INT DEFAULT 0;
  DECLARE substring_length INT;
  DECLARE key_str VARCHAR(255);
  DECLARE result TEXT DEFAULT '';

  SET key_str := CONCAT('"', key_name, '": {');

  -- Find the start position of the key
  SET start_index := LOCATE(key_str, json_string);
    IF start_index = 0 THEN
        RETURN NULL;
    END IF;

    -- Adjust start_index to the start of the value
    SET start_index := start_index + CHAR_LENGTH(key_str);

    -- Initialize the end_index to start_index
    SET end_index := start_index;

    -- Find the end of the object
    WHILE nested_level >= 0 AND end_index <= CHAR_LENGTH(json_string) DO
        SET end_index := end_index + 1;
        SET substring_length := end_index - start_index;

        -- Check for nested objects
        IF SUBSTRING(json_string, end_index, 1) = '{' THEN
          SET nested_level := nested_level + 1;
        ELSEIF SUBSTRING(json_string, end_index, 1) = '}' THEN
          SET nested_level := nested_level - 1;
        END IF;
    END WHILE;

    -- Get the JSON object
    IF nested_level < 0 THEN
    -- We found a matching pair of curly braces
        SET result := SUBSTRING(json_string, start_index, substring_length);
    END IF;

RETURN result;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_json_keys_array  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP FUNCTION IF EXISTS fn_mamba_json_keys_array;

~-~-
CREATE FUNCTION fn_mamba_json_keys_array(json_object TEXT) RETURNS TEXT
    DETERMINISTIC
BEGIN
    DECLARE finished INT DEFAULT 0;
    DECLARE start_index INT DEFAULT 1;
    DECLARE end_index INT DEFAULT 1;
    DECLARE key_name TEXT DEFAULT '';
    DECLARE my_keys TEXT DEFAULT '';
    DECLARE json_length INT;
    DECLARE key_end_index INT;

    SET json_length = CHAR_LENGTH(json_object);

    -- Initialize the my_keys string as an empty 'array'
    SET my_keys = '';

    -- This loop goes through the JSON object and extracts the my_keys
    WHILE NOT finished DO
            -- Find the start of the key
            SET start_index = LOCATE('"', json_object, end_index);
            IF start_index = 0 OR start_index >= json_length THEN
                SET finished = 1;
            ELSE
                -- Find the end of the key
                SET end_index = LOCATE('"', json_object, start_index + 1);
                SET key_name = SUBSTRING(json_object, start_index + 1, end_index - start_index - 1);

                -- Append the key to the 'array' of my_keys
                IF my_keys = ''
                    THEN
                    SET my_keys = CONCAT('["', key_name, '"');
                ELSE
                    SET my_keys = CONCAT(my_keys, ',"', key_name, '"');
                END IF;

                -- Move past the current key-value pair
                SET key_end_index = LOCATE(',', json_object, end_index);
                IF key_end_index = 0 THEN
                    SET key_end_index = LOCATE('}', json_object, end_index);
                END IF;
                IF key_end_index = 0 THEN
                    -- Closing brace not found - malformed JSON
                    SET finished = 1;
                ELSE
                    -- Prepare for the next iteration
                    SET end_index = key_end_index + 1;
                END IF;
            END IF;
    END WHILE;

    -- Close the 'array' of my_keys
    IF my_keys != '' THEN
        SET my_keys = CONCAT(my_keys, ']');
    END IF;

    RETURN my_keys;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_json_length  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_json_length;

~-~-
CREATE FUNCTION fn_mamba_json_length(json_array TEXT) RETURNS INT
    DETERMINISTIC
BEGIN
    DECLARE element_count INT DEFAULT 0;
    DECLARE current_position INT DEFAULT 1;

    WHILE current_position <= CHAR_LENGTH(json_array) DO
        SET element_count = element_count + 1;
        SET current_position = LOCATE(',', json_array, current_position) + 1;

        IF current_position = 0 THEN
            RETURN element_count;
        END IF;
    END WHILE;

RETURN element_count;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_json_object_at_index  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_json_object_at_index;

~-~-
CREATE FUNCTION fn_mamba_json_object_at_index(json_array TEXT, index_pos INT) RETURNS TEXT
    DETERMINISTIC
BEGIN
  DECLARE obj_start INT DEFAULT 1;
  DECLARE obj_end INT DEFAULT 1;
  DECLARE current_index INT DEFAULT 0;
  DECLARE obj_text TEXT;

    -- Handle negative index_pos or json_array being NULL
    IF index_pos < 1 OR json_array IS NULL THEN
        RETURN NULL;
    END IF;

    -- Find the start of the requested object
    WHILE obj_start < CHAR_LENGTH(json_array) AND current_index < index_pos DO
        SET obj_start = LOCATE('{', json_array, obj_end);

        -- If we can't find a new object, return NULL
        IF obj_start = 0 THEN
          RETURN NULL;
        END IF;

        SET current_index = current_index + 1;
        -- If this isn't the object we want, find the end and continue
        IF current_index < index_pos THEN
          SET obj_end = LOCATE('}', json_array, obj_start) + 1;
        END IF;
    END WHILE;

    -- Now obj_start points to the start of the desired object
    -- Find the end of it
    SET obj_end = LOCATE('}', json_array, obj_start);
    IF obj_end = 0 THEN
        -- The object is not well-formed
        RETURN NULL;
    END IF;

    -- Extract the object
    SET obj_text = SUBSTRING(json_array, obj_start, obj_end - obj_start + 1);

RETURN obj_text;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_json_value_by_key  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_json_value_by_key;

~-~-
CREATE FUNCTION fn_mamba_json_value_by_key(json TEXT, key_name VARCHAR(255)) RETURNS VARCHAR(255)
    DETERMINISTIC
BEGIN
    DECLARE start_index INT;
    DECLARE end_index INT;
    DECLARE key_length INT;
    DECLARE key_index INT;
    DECLARE value_length INT;
    DECLARE extracted_value VARCHAR(255);

    -- Add the key structure to search for in the JSON string
    SET key_name = CONCAT('"', key_name, '":');
    SET key_length = CHAR_LENGTH(key_name);

    -- Locate the key within the JSON string
    SET key_index = LOCATE(key_name, json);

    -- If the key is not found, return NULL
    IF key_index = 0 THEN
        RETURN NULL;
    END IF;

    -- Set the starting index of the value
    SET start_index = key_index + key_length;

    -- Check if the value is a string (starts with a quote)
    IF SUBSTRING(json, start_index, 1) = '"' THEN
        -- Set the start index to the first character of the value (skipping the quote)
        SET start_index = start_index + 1;

        -- Find the end of the string value (the next quote)
        SET end_index = LOCATE('"', json, start_index);
        IF end_index = 0 THEN
            -- If there's no end quote, the JSON is malformed
            RETURN NULL;
        END IF;
    ELSE
        -- The value is not a string (e.g., a number, boolean, or null)
        -- Find the end of the value (either a comma or closing brace)
        SET end_index = LOCATE(',', json, start_index);
        IF end_index = 0 THEN
            SET end_index = LOCATE('}', json, start_index);
        END IF;
    END IF;

    -- Calculate the length of the extracted value
    SET value_length = end_index - start_index;

    -- Extract the value
    SET extracted_value = SUBSTRING(json, start_index, value_length);

    -- Return the extracted value without leading or trailing quotes
RETURN TRIM(BOTH '"' FROM extracted_value);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_remove_all_whitespace  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_remove_all_whitespace;

~-~-
CREATE FUNCTION fn_mamba_remove_all_whitespace(input_string TEXT) RETURNS TEXT
    DETERMINISTIC

BEGIN
  DECLARE cleaned_string TEXT;
  SET cleaned_string = input_string;

  -- Replace common whitespace characters
  SET cleaned_string = REPLACE(cleaned_string, CHAR(9), '');   -- Horizontal tab
  SET cleaned_string = REPLACE(cleaned_string, CHAR(10), '');  -- Line feed
  SET cleaned_string = REPLACE(cleaned_string, CHAR(13), '');  -- Carriage return
  SET cleaned_string = REPLACE(cleaned_string, CHAR(32), '');  -- Space
  -- SET cleaned_string = REPLACE(cleaned_string, CHAR(160), ''); -- Non-breaking space

RETURN TRIM(cleaned_string);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_remove_quotes  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_remove_quotes;

~-~-
CREATE FUNCTION fn_mamba_remove_quotes(original TEXT) RETURNS TEXT
    DETERMINISTIC
BEGIN
  DECLARE without_quotes TEXT;

  -- Replace both single and double quotes with nothing
  SET without_quotes = REPLACE(REPLACE(original, '"', ''), '''', '');

RETURN fn_mamba_remove_all_whitespace(without_quotes);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_remove_special_characters  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_remove_special_characters;


~-~-
CREATE FUNCTION fn_mamba_remove_special_characters(input_text VARCHAR(255))
    RETURNS VARCHAR(255) DETERMINISTIC
BEGIN
    DECLARE modified_string VARCHAR(255);
    DECLARE special_chars VARCHAR(255);
    DECLARE char_index INT DEFAULT 1;
    DECLARE current_char CHAR(1);

    SET modified_string = input_text;
    -- SET special_chars = '!@#$%^&*?/,()"-=+£:;><ã';
    SET special_chars = '!@#$%^&*?/,()"-=+£:;><ã\|[]{}\'~`.'; -- TODO: Added '.' xter as well but Remove after adding backtick support

    WHILE char_index <= CHAR_LENGTH(special_chars) DO
            SET current_char = SUBSTRING(special_chars, char_index, 1);
            SET modified_string = REPLACE(modified_string, current_char, '');
            SET char_index = char_index + 1;
        END WHILE;

    RETURN modified_string;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_functions_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_stored_functions_in_schema;


~-~-
CREATE PROCEDURE sp_xf_system_drop_all_stored_functions_in_schema(
    IN database_name CHAR(255) CHARACTER SET UTF8MB4
)
BEGIN
    DELETE FROM `mysql`.`proc` WHERE `type` = 'FUNCTION' AND `db` = database_name; -- works in mysql before v.8

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_stored_procedures_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_stored_procedures_in_schema;


~-~-
CREATE PROCEDURE sp_xf_system_drop_all_stored_procedures_in_schema(
    IN database_name CHAR(255) CHARACTER SET UTF8MB4
)
BEGIN

    DELETE FROM `mysql`.`proc` WHERE `type` = 'PROCEDURE' AND `db` = database_name; -- works in mysql before v.8

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_objects_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_objects_in_schema;


~-~-
CREATE PROCEDURE sp_xf_system_drop_all_objects_in_schema(
    IN database_name CHAR(255) CHARACTER SET UTF8MB4
)
BEGIN

    CALL sp_xf_system_drop_all_stored_functions_in_schema(database_name);
    CALL sp_xf_system_drop_all_stored_procedures_in_schema(database_name);
    CALL sp_mamba_system_drop_all_tables(database_name);
    # CALL sp_xf_system_drop_all_views_in_schema (database_name);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_system_drop_all_tables  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_system_drop_all_tables;


-- CREATE PROCEDURE sp_mamba_system_drop_all_tables(IN database_name CHAR(255) CHARACTER SET UTF8MB4)
~-~-
CREATE PROCEDURE sp_mamba_system_drop_all_tables()
BEGIN

    DECLARE tables_count INT;

    SET @database_name = (SELECT DATABASE());

    SELECT COUNT(1)
    INTO tables_count
    FROM information_schema.tables
    WHERE TABLE_TYPE = 'BASE TABLE'
      AND TABLE_SCHEMA = @database_name;

    IF tables_count > 0 THEN

        SET session group_concat_max_len = 20000;

        SET @tbls = (SELECT GROUP_CONCAT(@database_name, '.', TABLE_NAME SEPARATOR ', ')
                     FROM information_schema.tables
                     WHERE TABLE_TYPE = 'BASE TABLE'
                       AND TABLE_SCHEMA = @database_name
                       AND TABLE_NAME REGEXP '^(mamba_|dim_|fact_|flat_)');

        IF (@tbls IS NOT NULL) THEN

            SET @drop_tables = CONCAT('DROP TABLE IF EXISTS ', @tbls);

            SET foreign_key_checks = 0; -- Remove check, so we don't have to drop tables in the correct order, or care if they exist or not.
            PREPARE drop_tbls FROM @drop_tables;
            EXECUTE drop_tbls;
            DEALLOCATE PREPARE drop_tbls;
            SET foreign_key_checks = 1;

        END IF;

    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_scheduler_wrapper  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_scheduler_wrapper;


~-~-
CREATE PROCEDURE sp_mamba_etl_scheduler_wrapper()

BEGIN

    DECLARE etl_ever_scheduled TINYINT(1);
    DECLARE incremental_mode TINYINT(1);
    DECLARE incremental_mode_cascaded TINYINT(1);

    SELECT COUNT(1)
    INTO etl_ever_scheduled
    FROM _mamba_etl_schedule;

    SELECT incremental_mode_switch
    INTO incremental_mode
    FROM _mamba_etl_user_settings;

    IF etl_ever_scheduled <= 1 OR incremental_mode = 0 THEN
        SET incremental_mode_cascaded = 0;
        CALL sp_mamba_data_processing_drop_and_flatten();
    ELSE
        SET incremental_mode_cascaded = 1;
        CALL sp_mamba_data_processing_increment_and_flatten();
    END IF;

    CALL sp_mamba_data_processing_etl(incremental_mode_cascaded);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_schedule_table_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_schedule_table_create;


~-~-
CREATE PROCEDURE sp_mamba_etl_schedule_table_create()
BEGIN

    CREATE TABLE IF NOT EXISTS _mamba_etl_schedule
    (
        id                         INT      NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
        start_time                 DATETIME NOT NULL DEFAULT NOW(),
        end_time                   DATETIME,
        next_schedule              DATETIME,
        execution_duration_seconds BIGINT,
        missed_schedule_by_seconds BIGINT,
        completion_status          ENUM ('SUCCESS', 'ERROR'),
        transaction_status         ENUM ('RUNNING', 'COMPLETED'),
        success_or_error_message   MEDIUMTEXT,

        INDEX mamba_idx_start_time (start_time),
        INDEX mamba_idx_end_time (end_time),
        INDEX mamba_idx_transaction_status (transaction_status),
        INDEX mamba_idx_completion_status (completion_status)
    )
        CHARSET = UTF8MB4;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_schedule  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_schedule;


~-~-
CREATE PROCEDURE sp_mamba_etl_schedule()

BEGIN

    DECLARE etl_execution_delay_seconds TINYINT(2) DEFAULT 0; -- 0 Seconds
    DECLARE interval_seconds INT;
    DECLARE start_time_seconds BIGINT;
    DECLARE end_time_seconds BIGINT;
    DECLARE time_now DATETIME;
    DECLARE txn_end_time DATETIME;
    DECLARE next_schedule_time DATETIME;
    DECLARE next_schedule_seconds BIGINT;
    DECLARE missed_schedule_seconds INT DEFAULT 0;
    DECLARE time_taken BIGINT;
    DECLARE etl_is_ready_to_run BOOLEAN DEFAULT FALSE;

    -- check if _mamba_etl_schedule is empty(new) or last transaction_status
    -- is 'COMPLETED' AND it was a 'SUCCESS' AND its 'end_time' was set.
    SET etl_is_ready_to_run = (SELECT COALESCE(
                                              (SELECT IF(end_time IS NOT NULL
                                                             AND transaction_status = 'COMPLETED'
                                                             AND completion_status = 'SUCCESS',
                                                         TRUE, FALSE)
                                               FROM _mamba_etl_schedule
                                               ORDER BY id DESC
                                               LIMIT 1), TRUE));

    IF etl_is_ready_to_run THEN

        SET time_now = NOW();
        SET start_time_seconds = UNIX_TIMESTAMP(time_now);

        INSERT INTO _mamba_etl_schedule(start_time, transaction_status)
        VALUES (time_now, 'RUNNING');

        SET @last_inserted_id = LAST_INSERT_ID();

        UPDATE _mamba_etl_user_settings
        SET last_etl_schedule_insert_id = @last_inserted_id
        WHERE TRUE
        ORDER BY id DESC
        LIMIT 1;

        -- Call ETL
        CALL sp_mamba_etl_scheduler_wrapper();

        SET txn_end_time = NOW();
        SET end_time_seconds = UNIX_TIMESTAMP(txn_end_time);

        SET time_taken = (end_time_seconds - start_time_seconds);


        SET interval_seconds = (SELECT etl_interval_seconds
                                FROM _mamba_etl_user_settings
                                ORDER BY id DESC
                                LIMIT 1);

        SET next_schedule_seconds = start_time_seconds + interval_seconds + etl_execution_delay_seconds;
        SET next_schedule_time = FROM_UNIXTIME(next_schedule_seconds);

        -- Run ETL immediately if schedule was missed (give allowance of 1 second)
        IF end_time_seconds > next_schedule_seconds THEN
            SET missed_schedule_seconds = end_time_seconds - next_schedule_seconds;
            SET next_schedule_time = FROM_UNIXTIME(end_time_seconds + 1);
        END IF;

        UPDATE _mamba_etl_schedule
        SET end_time                   = txn_end_time,
            next_schedule              = next_schedule_time,
            execution_duration_seconds = time_taken,
            missed_schedule_by_seconds = missed_schedule_seconds,
            completion_status          = 'SUCCESS',
            transaction_status         = 'COMPLETED'
        WHERE id = @last_inserted_id;

    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_setup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_setup;


~-~-
CREATE PROCEDURE sp_mamba_etl_setup(
    IN openmrs_database VARCHAR(256) CHARACTER SET UTF8MB4,
    IN etl_database VARCHAR(256) CHARACTER SET UTF8MB4,
    IN concepts_locale CHAR(4) CHARACTER SET UTF8MB4,
    IN table_partition_number INT,
    IN incremental_mode_switch TINYINT(1),
    IN automatic_flattening_mode_switch TINYINT(1),
    IN etl_interval_seconds INT
)
BEGIN

    -- Setup ETL Error log Table
    CALL sp_mamba_etl_error_log();

    -- Setup ETL configurations
    CALL sp_mamba_etl_user_settings(openmrs_database,
                                    etl_database,
                                    concepts_locale,
                                    table_partition_number,
                                    incremental_mode_switch,
                                    automatic_flattening_mode_switch,
                                    etl_interval_seconds);

    -- create ETL schedule log table
    CALL sp_mamba_etl_schedule_table_create();

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_create;


~-~-
CREATE PROCEDURE sp_mamba_flat_encounter_table_create(
    IN flat_encounter_table_name VARCHAR(60) CHARSET UTF8MB4
)
BEGIN

    SET session group_concat_max_len = 20000;
    SET @column_labels := NULL;

    SET @drop_table = CONCAT('DROP TABLE IF EXISTS `', flat_encounter_table_name, '`');

    SELECT GROUP_CONCAT(CONCAT('`', column_label, '` ', fn_mamba_get_datatype_for_concept(concept_datatype)) SEPARATOR ', ')
    INTO @column_labels
    FROM mamba_concept_metadata
    WHERE flat_table_name = flat_encounter_table_name
      AND concept_datatype IS NOT NULL;

    IF @column_labels IS NOT NULL THEN
        SET @create_table = CONCAT(
            'CREATE TABLE `', flat_encounter_table_name, '` (`encounter_id` INT NOT NULL, `visit_id` INT NULL, `client_id` INT NOT NULL, `encounter_datetime` DATETIME NOT NULL, `location_id` INT NULL, ', @column_labels, ', INDEX `mamba_idx_encounter_id` (`encounter_id`), INDEX `mamba_idx_visit_id` (`visit_id`), INDEX `mamba_idx_client_id` (`client_id`), INDEX `mamba_idx_encounter_datetime` (`encounter_datetime`), INDEX `mamba_idx_location_id` (`location_id`));');
    END IF;

    IF @column_labels IS NOT NULL THEN
        PREPARE deletetb FROM @drop_table;
        PREPARE createtb FROM @create_table;

        EXECUTE deletetb;
        EXECUTE createtb;

        DEALLOCATE PREPARE deletetb;
        DEALLOCATE PREPARE createtb;
    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_create_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder
DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_create_all;


~-~-
CREATE PROCEDURE sp_mamba_flat_encounter_table_create_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT(flat_table_name) FROM mamba_concept_metadata;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_mamba_flat_encounter_table_create(tbl_name);

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_insert;


-- NOTE: this script was merged with the sp_mamba_flat_table_obs_incremental_insert script
-- NOTE: ONLY difference was the filter  WHERE o.encounter_id = ''', @enc_id, '''
-- NOTE: Here we were only inserting all obs/encounters for this flat table
-- NOTE: Where as in the sp_mamba_flat_table_obs_incremental_insert we were only updating the encounter in the flat table that has changes

~-~-
CREATE PROCEDURE sp_mamba_flat_encounter_table_insert(
    IN `flat_encounter_table_name` VARCHAR(60) CHARACTER SET UTF8MB4,
    IN encounter_id INT -- Optional parameter for incremental insert
)
BEGIN
    SET session group_concat_max_len = 20000;
    SET @tbl_name = `flat_encounter_table_name`;
    SET @enc_id = encounter_id;

    -- called outside the incremental script
    -- Handle the optional encounter_id parameter by setting a default value if NULL
    IF @enc_id IS NULL THEN
        SET @enc_id = 0;
    ELSE
        -- called through the incremental script
        -- if enc_id exits in the table @tbl_name, then delete the record (to be replaced with the new one)
        SET @delete_stmt = CONCAT('DELETE FROM `', @tbl_name, '` WHERE `encounter_id` = ', @enc_id);
        PREPARE `deletetb` FROM @delete_stmt;
        EXECUTE `deletetb`;
        DEALLOCATE PREPARE `deletetb`;
    END IF;

    -- Precompute the concept metadata table to minimize repeated queries
    CREATE TEMPORARY TABLE IF NOT EXISTS `mamba_temp_concept_metadata`
    (
        `id`                 INT          NOT NULL,
        `flat_table_name`     VARCHAR(60)  NOT NULL,
        `encounter_type_uuid` CHAR(38)     NOT NULL,
        `column_label`        VARCHAR(255) NOT NULL,
        `concept_uuid`        CHAR(38)     NOT NULL,
        `obs_value_column`    VARCHAR(50),
        `concept_answer_obs`  INT,

        INDEX `mamba_idx_id` (`id`),
        INDEX `mamba_idx_column_label` (`column_label`),
        INDEX `mamba_idx_concept_uuid` (`concept_uuid`),
        INDEX `mamba_idx_concept_answer_obs` (`concept_answer_obs`),
        INDEX `mamba_idx_flat_table_name` (`flat_table_name`),
        INDEX `mamba_idx_encounter_type_uuid` (`encounter_type_uuid`)
    )
        CHARSET = UTF8MB4;

    TRUNCATE TABLE `mamba_temp_concept_metadata`;

    INSERT INTO `mamba_temp_concept_metadata`
    SELECT DISTINCT `id`,
                    `flat_table_name`,
                    `encounter_type_uuid`,
                    `column_label`,
                    `concept_uuid`,
                    fn_mamba_get_obs_value_column(`concept_datatype`) AS `obs_value_column`,
                    `concept_answer_obs`
    FROM `mamba_concept_metadata`
    WHERE `flat_table_name` = @tbl_name
      AND `concept_id` IS NOT NULL
      AND `concept_datatype` IS NOT NULL;

    SELECT GROUP_CONCAT(DISTINCT
                            CONCAT('MAX(CASE WHEN `column_label` = ''', `column_label`, ''' THEN ',
                                   `obs_value_column`, ' END) `', `column_label`, '`')
                            ORDER BY `id` ASC)
    INTO @column_labels
    FROM `mamba_temp_concept_metadata`;

    SELECT DISTINCT `encounter_type_uuid` INTO @tbl_encounter_type_uuid FROM `mamba_temp_concept_metadata`;

    IF @column_labels IS NOT NULL THEN
        -- First Insert: `concept_answer_obs` = 0
        SET @insert_stmt = CONCAT(
                'INSERT INTO `', @tbl_name,
                '` SELECT `o`.`encounter_id`, MAX(`o`.`visit_id`) AS `visit_id`, `o`.`person_id`, `o`.`encounter_datetime`, MAX(`o`.`location_id`) AS `location_id`, ',
                @column_labels, '
                FROM `mamba_z_encounter_obs` `o`
                    INNER JOIN `mamba_temp_concept_metadata` `tcm`
                    ON `tcm`.`concept_uuid` = `o`.`obs_question_uuid`
                WHERE 1=1 ',
                IF(@enc_id <> 0, CONCAT('AND `o`.`encounter_id` = ', @enc_id), ''),
                ' AND `o`.`encounter_type_uuid` = ''', @tbl_encounter_type_uuid, '''
                AND `tcm`.`concept_answer_obs` = 0
                AND `tcm`.`obs_value_column` IS NOT NULL
                AND `o`.`obs_group_id` IS NULL AND `o`.`voided` = 0
                GROUP BY `o`.`encounter_id`, `o`.`person_id`, `o`.`encounter_datetime`
                ORDER BY `o`.`encounter_id` ASC');

    PREPARE `inserttbl` FROM @insert_stmt;
    EXECUTE `inserttbl`;
    DEALLOCATE PREPARE `inserttbl`;

    -- Second Insert: `concept_answer_obs` = 1, Handle potential duplicates
    SET @update_stmt =
                    (SELECT GROUP_CONCAT(CONCAT('`', `column_label`, '` = COALESCE(VALUES(`', `column_label`, '`), `', `column_label`, '`)'))
                     FROM `mamba_temp_concept_metadata`);

        SET @insert_stmt = CONCAT(
                'INSERT INTO `', @tbl_name,
                '` SELECT `o`.`encounter_id`, MAX(`o`.`visit_id`) AS `visit_id`, `o`.`person_id`, `o`.`encounter_datetime`, MAX(`o`.`location_id`) AS `location_id`, ',
                @column_labels, '
                FROM `mamba_z_encounter_obs` `o`
                    INNER JOIN `mamba_temp_concept_metadata` `tcm`
                    ON `tcm`.`concept_uuid` = `o`.`obs_value_coded_uuid`
                WHERE 1=1 ',
                IF(@enc_id <> 0, CONCAT('AND `o`.`encounter_id` = ', @enc_id), ''),
                ' AND `o`.`encounter_type_uuid` = ''', @tbl_encounter_type_uuid, '''
                AND `tcm`.`concept_answer_obs` = 1
                AND `tcm`.`obs_value_column` IS NOT NULL
                AND `o`.`obs_group_id` IS NULL AND `o`.`voided` = 0
                GROUP BY `o`.`encounter_id`, `o`.`person_id`, `o`.`encounter_datetime`
                ORDER BY `o`.`encounter_id` ASC
                ON DUPLICATE KEY UPDATE ', @update_stmt);

    PREPARE `inserttbl` FROM @insert_stmt;
    EXECUTE `inserttbl`;
    DEALLOCATE PREPARE `inserttbl`;
    END IF;

    DROP TEMPORARY TABLE IF EXISTS `mamba_temp_concept_metadata`;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_insert_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder
DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_insert_all;


~-~-
CREATE PROCEDURE sp_mamba_flat_encounter_table_insert_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT(flat_table_name) FROM mamba_concept_metadata;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_mamba_flat_encounter_table_insert(tbl_name, NULL); -- Insert all OBS/Encounters for this flat table

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_incremental_create_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_incremental_create_all;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_incremental_create_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT(flat_table_name)
        FROM mamba_concept_metadata md
        WHERE incremental_record = 1;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_mamba_drop_table(tbl_name);
        CALL sp_mamba_flat_encounter_table_create(tbl_name);

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_incremental_insert_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_incremental_insert_all;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_incremental_insert_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT(flat_table_name)
        FROM mamba_concept_metadata md
        WHERE incremental_record = 1;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_mamba_flat_encounter_table_insert(tbl_name, NULL); -- Insert all OBS/Encounters for this flat table

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_incremental_update_encounter  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_incremental_update_encounter;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_incremental_update_encounter()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;
    DECLARE encounter_id INT;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT eo.encounter_id, cm.flat_table_name
        FROM mamba_z_encounter_obs eo
                 INNER JOIN mamba_concept_metadata cm ON eo.encounter_type_uuid = cm.encounter_type_uuid
        WHERE eo.incremental_record = 1;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO encounter_id, tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_mamba_flat_encounter_table_insert(tbl_name, encounter_id); -- Update only OBS/Encounters that have been modified for this flat table

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_incremental_update_encounter  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_incremental_update_encounter;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_incremental_update_encounter()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;
    DECLARE encounter_id INT;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT eo.encounter_id, cm.flat_table_name
        FROM mamba_z_encounter_obs eo
                 INNER JOIN mamba_concept_metadata cm ON eo.encounter_type_uuid = cm.encounter_type_uuid
        WHERE eo.incremental_record = 1;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO encounter_id, tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_mamba_flat_encounter_table_insert(tbl_name, encounter_id); -- Update only OBS/Encounters that have been modified for this flat table

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_obs_group_table_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS `sp_mamba_flat_encounter_obs_group_table_create`;

~-~-
CREATE PROCEDURE `sp_mamba_flat_encounter_obs_group_table_create`(
    IN `flat_encounter_table_name` VARCHAR(60) CHARSET UTF8MB4,
    `obs_group_concept_name` VARCHAR(255) CHARSET UTF8MB4
)
BEGIN

    SET session group_concat_max_len = 20000;
    SET @column_labels := NULL;
        SET @tbl_obs_group_name = CONCAT(LEFT(`flat_encounter_table_name`, 50), '_', `obs_group_concept_name`); -- TODO: 50 + 12 to make 62

        SET @drop_table = CONCAT('DROP TABLE IF EXISTS `', @tbl_obs_group_name, '`');

    SELECT GROUP_CONCAT(CONCAT(`column_label`, ' ', fn_mamba_get_datatype_for_concept(`concept_datatype`)) SEPARATOR ', ')
    INTO @column_labels
    FROM `mamba_concept_metadata` `cm`
             INNER JOIN
         (SELECT DISTINCT `obs_question_concept_id`
          FROM `mamba_z_encounter_obs` `eo`
                   INNER JOIN `mamba_obs_group` `og`
                              ON `eo`.`obs_id` = `og`.`obs_id`
          WHERE `obs_group_id` IS NOT NULL
            AND `og`.`obs_group_concept_name` = `obs_group_concept_name`) `eo`
         ON `cm`.`concept_id` = `eo`.`obs_question_concept_id`
    WHERE `flat_table_name` = `flat_encounter_table_name`
      AND `concept_datatype` IS NOT NULL;

    IF @column_labels IS NOT NULL THEN
            SET @create_table = CONCAT(
                    'CREATE TABLE `', @tbl_obs_group_name,
                    '` (`encounter_id` INT NOT NULL, `visit_id` INT NULL, `client_id` INT NOT NULL, `encounter_datetime` DATETIME NOT NULL, `location_id` INT NULL, ',
                    @column_labels,
                    ', INDEX `mamba_idx_encounter_id` (`encounter_id`), INDEX `mamba_idx_visit_id` (`visit_id`), INDEX `mamba_idx_client_id` (`client_id`), INDEX `mamba_idx_encounter_datetime` (`encounter_datetime`), INDEX `mamba_idx_location_id` (`location_id`));');
    END IF;

        IF @column_labels IS NOT NULL THEN
            PREPARE `deletetb` FROM @drop_table;
    PREPARE `createtb` FROM @create_table;

    EXECUTE `deletetb`;
    EXECUTE `createtb`;

    DEALLOCATE PREPARE `deletetb`;
    DEALLOCATE PREPARE `createtb`;
    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_obs_group_table_create_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder

DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_obs_group_table_create_all;

~-~-
CREATE PROCEDURE sp_mamba_flat_encounter_obs_group_table_create_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;
    DECLARE obs_name CHAR(50) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT 0;

    DECLARE cursor_flat_tables CURSOR FOR
    SELECT DISTINCT(flat_table_name) FROM mamba_concept_metadata;

    DECLARE cursor_obs_group_tables CURSOR FOR
    SELECT DISTINCT(obs_group_concept_name) FROM mamba_obs_group;

    -- DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN cursor_flat_tables;

        REPEAT
            FETCH cursor_flat_tables INTO tbl_name;
                IF NOT done THEN
                    OPEN cursor_obs_group_tables;
                        block2: BEGIN
                            DECLARE doneobs_name INT DEFAULT 0;
                            DECLARE firstobs_name varchar(255) DEFAULT '';
                            DECLARE i int DEFAULT 1;
                            DECLARE CONTINUE HANDLER FOR NOT FOUND SET doneobs_name = 1;

                            REPEAT
                                FETCH cursor_obs_group_tables INTO obs_name;

                                    IF i = 1 THEN
                                        SET firstobs_name = obs_name;
                                    END IF;

                                    CALL sp_mamba_flat_encounter_obs_group_table_create(tbl_name,obs_name);
                                    SET i = i + 1;

                                UNTIL doneobs_name
                            END REPEAT;

                            CALL sp_mamba_flat_encounter_obs_group_table_create(tbl_name,firstobs_name);
                        END block2;
                    CLOSE cursor_obs_group_tables;
                END IF;
            UNTIL done
        END REPEAT;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_obs_group_table_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS `sp_mamba_flat_encounter_obs_group_table_insert`;

~-~-
CREATE PROCEDURE `sp_mamba_flat_encounter_obs_group_table_insert`(
    IN `flat_encounter_table_name` VARCHAR(60) CHARACTER SET UTF8MB4,
    `obs_group_concept_name` VARCHAR(255) CHARSET UTF8MB4
)
BEGIN

    SET session group_concat_max_len = 20000;
    SET @tbl_name = `flat_encounter_table_name`;

        SET @tbl_obs_group_name = CONCAT(LEFT(@tbl_name, 50), '_', `obs_group_concept_name`); -- TODO: 50 + 12 to make 62

        SET @old_sql = (SELECT GROUP_CONCAT(`COLUMN_NAME` SEPARATOR ', ')
                        FROM `INFORMATION_SCHEMA`.`COLUMNS`
                        WHERE `TABLE_NAME` = @tbl_name
                          AND `TABLE_SCHEMA` = DATABASE());

    SELECT GROUP_CONCAT(DISTINCT
                            CONCAT(' MAX(CASE WHEN `column_label` = ''', `column_label`, ''' THEN ',
                                   fn_mamba_get_obs_value_column(`concept_datatype`), ' END) ', `column_label`)
                            ORDER BY `id` ASC)
    INTO @column_labels
    FROM `mamba_concept_metadata` `cm`
             INNER JOIN
         (SELECT DISTINCT `obs_question_concept_id`
          FROM `mamba_z_encounter_obs` `eo`
                   INNER JOIN `mamba_obs_group` `og`
                              ON `eo`.`obs_id` = `og`.`obs_id`
          WHERE `obs_group_id` IS NOT NULL
            AND `og`.`obs_group_concept_name` = `obs_group_concept_name`) `eo`
         ON `cm`.`concept_id` = `eo`.`obs_question_concept_id`
    WHERE `flat_table_name` = @tbl_name;

    IF @column_labels IS NOT NULL THEN
            IF (SELECT COUNT(*) FROM `information_schema`.`tables` WHERE `table_name` = @tbl_obs_group_name) > 0 THEN
                SET @insert_stmt = CONCAT(
                        'INSERT INTO `', @tbl_obs_group_name,
                        '` SELECT `eo`.`encounter_id`, MAX(`eo`.`visit_id`) AS `visit_id`, `eo`.`person_id`, `eo`.`encounter_datetime`, MAX(`eo`.`location_id`) AS `location_id`, ',
                        @column_labels, '
                        FROM `mamba_z_encounter_obs` `eo`
                            INNER JOIN `mamba_concept_metadata` `cm`
                            ON IF(`cm`.`concept_answer_obs`=1, `cm`.`concept_uuid`=`eo`.`obs_value_coded_uuid`, `cm`.`concept_uuid`=`eo`.`obs_question_uuid`)
                        WHERE  `cm`.`flat_table_name` = ''', @tbl_name, '''
                        AND `eo`.`encounter_type_uuid` = `cm`.`encounter_type_uuid`
                        AND `eo`.`obs_group_id` IS NOT NULL
                        GROUP BY `eo`.`encounter_id`, `eo`.`person_id`, `eo`.`encounter_datetime`, `eo`.`obs_group_id`;');
    END IF;
    END IF;

        IF @column_labels IS NOT NULL THEN
            PREPARE `inserttbl` FROM @insert_stmt;
    EXECUTE `inserttbl`;
    DEALLOCATE PREPARE `inserttbl`;
    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_obs_group_table_insert_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder

DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_obs_group_table_insert_all;

~-~-
CREATE PROCEDURE sp_mamba_flat_encounter_obs_group_table_insert_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;
    DECLARE obs_name CHAR(50) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT 0;

    DECLARE cursor_flat_tables CURSOR FOR
    SELECT DISTINCT(flat_table_name) FROM mamba_concept_metadata;

    DECLARE cursor_obs_group_tables CURSOR FOR
    SELECT DISTINCT(obs_group_concept_name) FROM mamba_obs_group;

    -- DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN cursor_flat_tables;

        REPEAT
            FETCH cursor_flat_tables INTO tbl_name;
                IF NOT done THEN
                    OPEN cursor_obs_group_tables;
                        block2: BEGIN
                            DECLARE doneobs_name INT DEFAULT 0;
                            DECLARE firstobs_name varchar(255) DEFAULT '';
                            DECLARE i int DEFAULT 1;
                            DECLARE CONTINUE HANDLER FOR NOT FOUND SET doneobs_name = 1;

                            REPEAT
                                FETCH cursor_obs_group_tables INTO obs_name;

                                    IF i = 1 THEN
                                        SET firstobs_name = obs_name;
                                    END IF;

                                    CALL sp_mamba_flat_encounter_obs_group_table_insert(tbl_name,obs_name);
                                    SET i = i + 1;

                                UNTIL doneobs_name
                            END REPEAT;

                            CALL sp_mamba_flat_encounter_obs_group_table_insert(tbl_name,firstobs_name);
                        END block2;
                    CLOSE cursor_obs_group_tables;
            END IF;
                        UNTIL done
        END REPEAT;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_multiselect_values_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS `sp_mamba_multiselect_values_update`;


~-~-
CREATE PROCEDURE `sp_mamba_multiselect_values_update`(
    IN table_to_update CHAR(100) CHARACTER SET UTF8MB4,
    IN column_names TEXT CHARACTER SET UTF8MB4,
    IN value_yes CHAR(100) CHARACTER SET UTF8MB4,
    IN value_no CHAR(100) CHARACTER SET UTF8MB4
)
BEGIN

    SET @table_columns = column_names;
    SET @start_pos = 1;
    SET @comma_pos = locate(',', @table_columns);
    SET @end_loop = 0;

    SET @column_label = '';

    REPEAT
        IF @comma_pos > 0 THEN
            SET @column_label = substring(@table_columns, @start_pos, @comma_pos - @start_pos);
            SET @end_loop = 0;
        ELSE
            SET @column_label = substring(@table_columns, @start_pos);
            SET @end_loop = 1;
        END IF;

        -- UPDATE fact_hts SET @column_label=IF(@column_label IS NULL OR '', new_value_if_false, new_value_if_true);

        SET @update_sql = CONCAT(
                'UPDATE ', table_to_update, ' SET ', @column_label, '= IF(', @column_label, ' IS NOT NULL, ''',
                value_yes, ''', ''', value_no, ''');');
        PREPARE stmt FROM @update_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        IF @end_loop = 0 THEN
            SET @table_columns = substring(@table_columns, @comma_pos + 1);
            SET @comma_pos = locate(',', @table_columns);
        END IF;
    UNTIL @end_loop = 1
        END REPEAT;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_load_agegroup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_load_agegroup;


~-~-
CREATE PROCEDURE sp_mamba_load_agegroup()
BEGIN
    DECLARE age INT DEFAULT 0;
    WHILE age <= 120
        DO
            INSERT INTO mamba_dim_agegroup(age, datim_agegroup, normal_agegroup)
            VALUES (age, fn_mamba_calculate_agegroup(age), IF(age < 15, '<15', '15+'));
            SET age = age + 1;
        END WHILE;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_write_automated_json_config  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_write_automated_json_config;


~-~-
CREATE PROCEDURE sp_mamba_write_automated_json_config()
BEGIN

    DECLARE done INT DEFAULT FALSE;
    DECLARE jsonData JSON;
    DECLARE cur CURSOR FOR
        SELECT json_data FROM mamba_flat_table_config;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

        SET @report_data = '{"flat_report_metadata":[';

        OPEN cur;
        FETCH cur INTO jsonData;

        IF NOT done THEN
                    SET @report_data = CONCAT(@report_data, jsonData);
        FETCH cur INTO jsonData; -- Fetch next record after the first one
        END IF;

                read_loop: LOOP
                    IF done THEN
                        LEAVE read_loop;
        END IF;

                    SET @report_data = CONCAT(@report_data, ',', jsonData);
        FETCH cur INTO jsonData;
        END LOOP;
        CLOSE cur;

        SET @report_data = CONCAT(@report_data, ']}');

        CALL sp_mamba_extract_report_metadata(@report_data, 'mamba_concept_metadata');

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_locale_insert_helper  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_locale_insert_helper;


~-~-
CREATE PROCEDURE sp_mamba_locale_insert_helper(
    IN concepts_locale CHAR(4) CHARACTER SET UTF8MB4
)
BEGIN

    SET @conc_locale = concepts_locale;
    SET @insert_stmt = CONCAT('INSERT INTO mamba_dim_locale (locale) VALUES (''', @conc_locale, ''');');

    PREPARE inserttbl FROM @insert_stmt;
    EXECUTE inserttbl;
    DEALLOCATE PREPARE inserttbl;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_extract_report_column_names  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_extract_report_column_names;


~-~-
CREATE PROCEDURE sp_mamba_extract_report_column_names()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE proc_name VARCHAR(255);
    DECLARE cur CURSOR FOR SELECT DISTINCT report_columns_procedure_name FROM mamba_dim_report_definition;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO proc_name;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Fetch the parameters for the procedure and provide empty string values for each
        SET @params := NULL;

        SELECT GROUP_CONCAT('\'\'' SEPARATOR ', ')
        INTO @params
        FROM mamba_dim_report_definition_parameters rdp
                 INNER JOIN mamba_dim_report_definition rd on rdp.report_id = rd.report_id
        WHERE rd.report_columns_procedure_name = proc_name;

        IF @params IS NULL THEN
            SET @procedure_call = CONCAT('CALL ', proc_name, '();');
        ELSE
            SET @procedure_call = CONCAT('CALL ', proc_name, '(', @params, ');');
        END IF;

        PREPARE stmt FROM @procedure_call;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END LOOP;

    CLOSE cur;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_extract_report_definition_metadata  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_extract_report_definition_metadata;


~-~-
CREATE PROCEDURE sp_mamba_extract_report_definition_metadata(
    IN report_definition_json JSON,
    IN metadata_table VARCHAR(255) CHARSET UTF8MB4
)
BEGIN

    IF report_definition_json IS NULL OR JSON_LENGTH(report_definition_json) = 0 THEN
        SIGNAL SQLSTATE '02000'
            SET MESSAGE_TEXT = 'Warn: report_definition_json is empty or null.';
    ELSE

        SET session group_concat_max_len = 20000;

        SELECT JSON_EXTRACT(report_definition_json, '$.report_definitions') INTO @report_array;
        SELECT JSON_LENGTH(@report_array) INTO @report_array_len;

        SET @report_count = 0;
        WHILE @report_count < @report_array_len
            DO

                SELECT JSON_EXTRACT(@report_array, CONCAT('$[', @report_count, ']')) INTO @report;
                SELECT JSON_UNQUOTE(JSON_EXTRACT(@report, '$.report_name')) INTO @report_name;
                SELECT JSON_UNQUOTE(JSON_EXTRACT(@report, '$.report_id')) INTO @report_id;
                SELECT CONCAT('sp_mamba_report_', @report_id, '_query') INTO @report_procedure_name;
                SELECT CONCAT('sp_mamba_report_', @report_id, '_columns_query') INTO @report_columns_procedure_name;
                SELECT CONCAT('mamba_report_', @report_id) INTO @table_name;
                SELECT JSON_UNQUOTE(JSON_EXTRACT(@report, CONCAT('$.report_sql.sql_query'))) INTO @sql_query;
                SELECT JSON_EXTRACT(@report, CONCAT('$.report_sql.query_params')) INTO @query_params_array;

                INSERT INTO mamba_dim_report_definition(report_id,
                                                        report_procedure_name,
                                                        report_columns_procedure_name,
                                                        sql_query,
                                                        table_name,
                                                        report_name)
                VALUES (@report_id,
                        @report_procedure_name,
                        @report_columns_procedure_name,
                        @sql_query,
                        @table_name,
                        @report_name);

                -- Iterate over the "params" array for each report
                SELECT JSON_LENGTH(@query_params_array) INTO @total_params;

                SET @parameters := NULL;
                SET @param_count = 0;
                WHILE @param_count < @total_params
                    DO
                        SELECT JSON_EXTRACT(@query_params_array, CONCAT('$[', @param_count, ']')) INTO @param;
                        SELECT JSON_UNQUOTE(JSON_EXTRACT(@param, '$.name')) INTO @param_name;
                        SELECT JSON_UNQUOTE(JSON_EXTRACT(@param, '$.type')) INTO @param_type;
                        SET @param_position = @param_count + 1;

                        INSERT INTO mamba_dim_report_definition_parameters(report_id,
                                                                           parameter_name,
                                                                           parameter_type,
                                                                           parameter_position)
                        VALUES (@report_id,
                                @param_name,
                                @param_type,
                                @param_position);

                        SET @param_count = @param_position;
                    END WHILE;


--                SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
--                INTO @column_names
--                FROM INFORMATION_SCHEMA.COLUMNS
--                -- WHERE TABLE_SCHEMA = 'alive' TODO: add back after verifying schema name
--                WHERE TABLE_NAME = @report_id;
--
--                SET @drop_table = CONCAT('DROP TABLE IF EXISTS `', @report_id, '`');
--
--                SET @createtb = CONCAT('CREATE TEMP TABLE AS SELECT ', @report_id, ';', CHAR(10),
--                                       'CREATE PROCEDURE ', @report_procedure_name, '(', CHAR(10),
--                                       @parameters, CHAR(10),
--                                       ')', CHAR(10),
--                                       'BEGIN', CHAR(10),
--                                       @sql_query, CHAR(10),
--                                       'END;', CHAR(10));
--
--                PREPARE deletetb FROM @drop_table;
--                PREPARE createtb FROM @create_table;
--
--               EXECUTE deletetb;
--               EXECUTE createtb;
--
--                DEALLOCATE PREPARE deletetb;
--                DEALLOCATE PREPARE createtb;

                --                SELECT GROUP_CONCAT(CONCAT('IN ', parameter_name, ' ', parameter_type) SEPARATOR ', ')
--                INTO @parameters
--                FROM mamba_dim_report_definition_parameters
--                WHERE report_id = @report_id
--                ORDER BY parameter_position;
--
--                SET @procedure_definition = CONCAT('DROP PROCEDURE IF EXISTS ', @report_procedure_name, ';', CHAR(10),
--                                                   'CREATE PROCEDURE ', @report_procedure_name, '(', CHAR(10),
--                                                   @parameters, CHAR(10),
--                                                   ')', CHAR(10),
--                                                   'BEGIN', CHAR(10),
--                                                   @sql_query, CHAR(10),
--                                                   'END;', CHAR(10));
--
--                PREPARE CREATE_PROC FROM @procedure_definition;
--                EXECUTE CREATE_PROC;
--                DEALLOCATE PREPARE CREATE_PROC;
--
                SET @report_count = @report_count + 1;
            END WHILE;

    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_generate_report_wrapper  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_generate_report_wrapper;


~-~-
CREATE PROCEDURE sp_mamba_generate_report_wrapper(IN generate_columns_flag TINYINT(1),
                                                  IN report_identifier VARCHAR(255),
                                                  IN parameter_list JSON)
BEGIN

    DECLARE proc_name VARCHAR(255);
    DECLARE sql_args VARCHAR(1000);
    DECLARE arg_name VARCHAR(50);
    DECLARE arg_value VARCHAR(255);
    DECLARE tester VARCHAR(255);
    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_parameter_names CURSOR FOR
        SELECT DISTINCT (p.parameter_name)
        FROM mamba_dim_report_definition_parameters p
        WHERE p.report_id = report_identifier;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    IF generate_columns_flag = 1 THEN
        SET proc_name = (SELECT DISTINCT (rd.report_columns_procedure_name)
                         FROM mamba_dim_report_definition rd
                         WHERE rd.report_id = report_identifier);
    ELSE
        SET proc_name = (SELECT DISTINCT (rd.report_procedure_name)
                         FROM mamba_dim_report_definition rd
                         WHERE rd.report_id = report_identifier);
    END IF;

    OPEN cursor_parameter_names;
    read_loop:
    LOOP
        FETCH cursor_parameter_names INTO arg_name;

        IF done THEN
            LEAVE read_loop;
        END IF;

        SET arg_value = IFNULL((JSON_EXTRACT(parameter_list, CONCAT('$[', ((SELECT p.parameter_position
                                                                            FROM mamba_dim_report_definition_parameters p
                                                                            WHERE p.parameter_name = arg_name
                                                                              AND p.report_id = report_identifier) - 1),
                                                                    '].value'))), 'NULL');
        SET tester = CONCAT_WS(', ', tester, arg_value);
        SET sql_args = IFNULL(CONCAT_WS(', ', sql_args, arg_value), NULL);

    END LOOP;

    CLOSE cursor_parameter_names;

    SET @sql = CONCAT('CALL ', proc_name, '(', IFNULL(sql_args, ''), ')');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_get_report_column_names  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_get_report_column_names;


~-~-
CREATE PROCEDURE sp_mamba_get_report_column_names(IN report_identifier VARCHAR(255))
BEGIN

    -- We could also pick the column names from the report definition table but it is in a comma-separated list (weigh both options)
    SELECT table_name
    INTO @table_name
    FROM mamba_dim_report_definition
    WHERE report_id = report_identifier;

    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = @table_name;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_reset_incremental_update_flag  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Given a table name, this procedure will reset the incremental_record column to 0 for all rows where the incremental_record is 1.
-- This is useful when we want to re-run the incremental updates for a table.

DROP PROCEDURE IF EXISTS sp_mamba_reset_incremental_update_flag;


~-~-
CREATE PROCEDURE sp_mamba_reset_incremental_update_flag(
    IN table_name VARCHAR(60) CHARACTER SET UTF8MB4
)
BEGIN

    SET @tbl_name = table_name;

    SET @update_stmt =
            CONCAT('UPDATE ', @tbl_name, ' SET incremental_record = 0 WHERE incremental_record = 1');
    PREPARE updatetb FROM @update_stmt;
    EXECUTE updatetb;
    DEALLOCATE PREPARE updatetb;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_reset_incremental_update_flag_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_reset_incremental_update_flag_all;


~-~-
CREATE PROCEDURE sp_mamba_reset_incremental_update_flag_all()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_reset_incremental_update_flag_all', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_reset_incremental_update_flag_all', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Given a table name, this procedure will reset the incremental_record column to 0 for all rows where the incremental_record is 1.
-- This is useful when we want to re-run the incremental updates for a table.

CALL sp_mamba_reset_incremental_update_flag('mamba_dim_location');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_patient_identifier_type');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_concept_datatype');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_concept_name');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_concept');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_concept_answer');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_encounter_type');
CALL sp_mamba_reset_incremental_update_flag('mamba_flat_table_config');
CALL sp_mamba_reset_incremental_update_flag('mamba_concept_metadata');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_encounter');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_person_name');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_person');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_person_attribute_type');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_person_attribute');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_person_address');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_users');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_relationship');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_patient_identifier');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_orders');
CALL sp_mamba_reset_incremental_update_flag('mamba_z_encounter_obs');

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_concept_metadata_create();
CALL sp_mamba_concept_metadata_insert();
CALL sp_mamba_concept_metadata_missing_columns_insert(); -- Update/insert table column metadata configs without table_columns json
CALL sp_mamba_concept_metadata_update();
CALL sp_mamba_concept_metadata_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_create;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_concept_metadata
(
    id                  INT          NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    concept_id          INT          NULL,
    concept_uuid        CHAR(38)     NOT NULL,
    concept_name        VARCHAR(255) NULL,
    column_number       INT,
    column_label        VARCHAR(60)  NOT NULL,
    concept_datatype    VARCHAR(255) NULL,
    concept_answer_obs  TINYINT(1)   NOT NULL DEFAULT 0,
    report_name         VARCHAR(255) NOT NULL,
    flat_table_name     VARCHAR(60)  NULL,
    encounter_type_uuid CHAR(38)     NOT NULL,
    row_num             INT          NULL     DEFAULT 1,
    incremental_record  INT          NOT NULL DEFAULT 0,

    INDEX mamba_idx_concept_id (concept_id),
    INDEX mamba_idx_concept_uuid (concept_uuid),
    INDEX mamba_idx_encounter_type_uuid (encounter_type_uuid),
    INDEX mamba_idx_row_num (row_num),
    INDEX mamba_idx_concept_datatype (concept_datatype),
    INDEX mamba_idx_flat_table_name (flat_table_name),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_insert;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN


SET @is_incremental = 0;
-- SET @report_data = fn_mamba_generate_json_from_mamba_flat_table_config(@is_incremental);
CALL sp_mamba_concept_metadata_insert_helper(@is_incremental, 'mamba_concept_metadata');


-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_insert_helper  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_insert_helper;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_insert_helper(
    IN is_incremental TINYINT(1),
    IN metadata_table VARCHAR(255) CHARSET UTF8MB4
)
BEGIN

    DECLARE is_incremental_record TINYINT(1) DEFAULT 0;
    DECLARE report_json JSON;
    DECLARE done INT DEFAULT FALSE;
    DECLARE cur CURSOR FOR
        -- selects rows where incremental_record is 1. If is_incremental is not 1, it selects all rows.
        SELECT table_json_data
        FROM mamba_flat_table_config
        WHERE (IF(is_incremental = 1, incremental_record = 1, 1));

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SET session group_concat_max_len = 20000;

    SELECT DISTINCT(table_partition_number)
    INTO @table_partition_number
    FROM _mamba_etl_user_settings;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO report_json;

        IF done THEN
            LEAVE read_loop;
        END IF;

        SELECT JSON_EXTRACT(report_json, '$.report_name') INTO @report_name;
        SELECT JSON_EXTRACT(report_json, '$.flat_table_name') INTO @flat_table_name;
        SELECT JSON_EXTRACT(report_json, '$.encounter_type_uuid') INTO @encounter_type;
        SELECT JSON_EXTRACT(report_json, '$.table_columns') INTO @column_array;

        SELECT JSON_KEYS(@column_array) INTO @column_keys_array;
        SELECT JSON_LENGTH(@column_keys_array) INTO @column_keys_array_len;

        -- if is_incremental = 1, delete records (if they exist) from mamba_concept_metadata table with encounter_type_uuid = @encounter_type
        IF is_incremental = 1 THEN

            SET is_incremental_record = 1;
            SET @delete_query = CONCAT('DELETE FROM mamba_concept_metadata WHERE encounter_type_uuid = ''',
                                       JSON_UNQUOTE(@encounter_type), '''');

            PREPARE stmt FROM @delete_query;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END IF;

        IF @column_keys_array_len = 0 THEN

            INSERT INTO mamba_concept_metadata
            (report_name,
             flat_table_name,
             encounter_type_uuid,
             column_label,
             concept_uuid,
             incremental_record)
            VALUES (JSON_UNQUOTE(@report_name),
                    JSON_UNQUOTE(@flat_table_name),
                    JSON_UNQUOTE(@encounter_type),
                    'AUTO-GENERATE',
                    'AUTO-GENERATE',
                    is_incremental_record);
        ELSE

            SET @col_count = 0;
            SET @table_name = JSON_UNQUOTE(@flat_table_name);
            SET @current_table_count = 1;

            WHILE @col_count < @column_keys_array_len
                DO
                    SELECT JSON_EXTRACT(@column_keys_array, CONCAT('$[', @col_count, ']')) INTO @field_name;
                    SELECT JSON_EXTRACT(@column_array, CONCAT('$.', @field_name)) INTO @concept_uuid;

                    IF @col_count > @table_partition_number THEN

                        SET @table_name = CONCAT(LEFT(JSON_UNQUOTE(@flat_table_name), 57), '_', @current_table_count);
                        SET @current_table_count = @current_table_count;

                        INSERT INTO mamba_concept_metadata
                        (report_name,
                         flat_table_name,
                         encounter_type_uuid,
                         column_label,
                         concept_uuid,
                         incremental_record)
                        VALUES (JSON_UNQUOTE(@report_name),
                                JSON_UNQUOTE(@table_name),
                                JSON_UNQUOTE(@encounter_type),
                                JSON_UNQUOTE(@field_name),
                                JSON_UNQUOTE(@concept_uuid),
                                is_incremental_record);

                    ELSE
                        INSERT INTO mamba_concept_metadata
                        (report_name,
                         flat_table_name,
                         encounter_type_uuid,
                         column_label,
                         concept_uuid,
                         incremental_record)
                        VALUES (JSON_UNQUOTE(@report_name),
                                JSON_UNQUOTE(@flat_table_name),
                                JSON_UNQUOTE(@encounter_type),
                                JSON_UNQUOTE(@field_name),
                                JSON_UNQUOTE(@concept_uuid),
                                is_incremental_record);
                    END IF;
                    SET @col_count = @col_count + 1;
                END WHILE;
        END IF;
    END LOOP;

    CLOSE cur;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_update;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update the Concept datatypes, concept_name and concept_id based on given locale
UPDATE mamba_concept_metadata md
    INNER JOIN mamba_dim_concept c
    ON md.concept_uuid = c.uuid
SET md.concept_datatype = c.datatype,
    md.concept_id       = c.concept_id,
    md.concept_name     = c.name
WHERE md.id > 0;

-- Update to True if this field is an obs answer to an obs Question
UPDATE mamba_concept_metadata md
    INNER JOIN mamba_dim_concept_answer ca
    ON md.concept_id = ca.answer_concept
SET md.concept_answer_obs = 1
WHERE md.id > 0
  AND md.concept_id IN (SELECT DISTINCT ca.concept_id
                        FROM mamba_dim_concept_answer ca);

-- Update to for multiple selects/dropdowns/options this field is an obs answer to an obs Question
-- TODO: check this implementation here
UPDATE mamba_concept_metadata md
SET md.concept_answer_obs = 1
WHERE md.id > 0
  and concept_datatype = 'N/A';

-- Update row number
SET @row_number = 0;
SET @prev_flat_table_name = NULL;
SET @prev_concept_id = NULL;

UPDATE mamba_concept_metadata md
    INNER JOIN (SELECT flat_table_name,
                       concept_id,
                       id,
                       @row_number := CASE
                                          WHEN @prev_flat_table_name = flat_table_name
                                              AND @prev_concept_id = concept_id
                                              THEN @row_number + 1
                                          ELSE 1
                           END AS num,
                       @prev_flat_table_name := flat_table_name,
                       @prev_concept_id := concept_id
                FROM mamba_concept_metadata
                ORDER BY flat_table_name, concept_id, id) m ON md.id = m.id
SET md.row_num = num
WHERE md.id > 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_cleanup;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_cleanup()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata_cleanup', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata_cleanup', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- delete un wanted rows after inserting columns that were not given in the .json config file into the meta data table,
-- all rows with 'AUTO-GENERATE' are not used anymore. Delete them/1
DELETE
FROM mamba_concept_metadata
WHERE concept_uuid = 'AUTO-GENERATE'
  AND column_label = 'AUTO-GENERATE';

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_missing_columns_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_missing_columns_insert;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_missing_columns_insert()
BEGIN

    DECLARE encounter_type_uuid_value CHAR(38);
    DECLARE report_name_val VARCHAR(100);
    DECLARE encounter_type_id_val INT;
    DECLARE flat_table_name_val VARCHAR(255);

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_encounters CURSOR FOR
        SELECT DISTINCT(encounter_type_uuid), m.report_name, m.flat_table_name, et.encounter_type_id
        FROM mamba_concept_metadata m
                 INNER JOIN mamba_dim_encounter_type et ON m.encounter_type_uuid = et.uuid
        WHERE et.retired = 0
          AND m.concept_uuid = 'AUTO-GENERATE'
          AND m.column_label = 'AUTO-GENERATE';

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_encounters;
    computations_loop:
    LOOP
        FETCH cursor_encounters
            INTO encounter_type_uuid_value, report_name_val, flat_table_name_val, encounter_type_id_val;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        SET @insert_stmt = CONCAT(
                'INSERT INTO mamba_concept_metadata
                (
                    report_name,
                    flat_table_name,
                    encounter_type_uuid,
                    column_label,
                    concept_uuid
                )
                SELECT
                    ''', report_name_val, ''',
                ''', flat_table_name_val, ''',
                ''', encounter_type_uuid_value, ''',
                field_name,
                concept_uuid,
                FROM (
                     SELECT
                          DISTINCT et.encounter_type_id,
                          c.auto_table_column_name AS field_name,
                          c.uuid AS concept_uuid
                     FROM mamba_source_db.obs o
                          INNER JOIN mamba_source_db.encounter e
                            ON e.encounter_id = o.encounter_id
                          INNER JOIN mamba_dim_encounter_type et
                            ON e.encounter_type = et.encounter_type_id
                          INNER JOIN mamba_dim_concept c
                            ON o.concept_id = c.concept_id
                     WHERE et.encounter_type_id = ''', encounter_type_id_val, '''
                       AND et.retired = 0
                ) mamba_missing_concept;
            ');

        PREPARE inserttbl FROM @insert_stmt;
        EXECUTE inserttbl;
        DEALLOCATE PREPARE inserttbl;

    END LOOP computations_loop;
    CLOSE cursor_encounters;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_incremental;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_concept_metadata_incremental_insert();
CALL sp_mamba_concept_metadata_missing_columns_incremental_insert(); -- Update/insert table column metadata configs without table_columns json
CALL sp_mamba_concept_metadata_incremental_update();
CALL sp_mamba_concept_metadata_incremental_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN


SET @is_incremental = 1;
-- SET @report_data = fn_mamba_generate_json_from_mamba_flat_table_config(@is_incremental);
CALL sp_mamba_concept_metadata_insert_helper(@is_incremental, 'mamba_concept_metadata');


-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update the Concept datatypes, concept_name and concept_id based on given locale
UPDATE mamba_concept_metadata md
    INNER JOIN mamba_dim_concept c
    ON md.concept_uuid = c.uuid
SET md.concept_datatype = c.datatype,
    md.concept_id       = c.concept_id,
    md.concept_name     = c.name
WHERE md.incremental_record = 1;

-- Update to True if this field is an obs answer to an obs Question
UPDATE mamba_concept_metadata md
    INNER JOIN mamba_dim_concept_answer ca
    ON md.concept_id = ca.answer_concept
SET md.concept_answer_obs = 1
WHERE md.incremental_record = 1
  AND md.concept_id IN (SELECT DISTINCT ca.concept_id
                        FROM mamba_dim_concept_answer ca);

-- Update to for multiple selects/dropdowns/options this field is an obs answer to an obs Question
-- TODO: check this implementation here
UPDATE mamba_concept_metadata md
SET md.concept_answer_obs = 1
WHERE md.incremental_record = 1
  and concept_datatype = 'N/A';

-- Update row number
SET @row_number = 0;
SET @prev_flat_table_name = NULL;
SET @prev_concept_id = NULL;

UPDATE mamba_concept_metadata md
    INNER JOIN (SELECT flat_table_name,
                       concept_id,
                       id,
                       @row_number := CASE
                                          WHEN @prev_flat_table_name = flat_table_name
                                              AND @prev_concept_id = concept_id
                                              THEN @row_number + 1
                                          ELSE 1
                           END AS num,
                       @prev_flat_table_name := flat_table_name,
                       @prev_concept_id := concept_id
                FROM mamba_concept_metadata
                -- WHERE incremental_record = 1
                ORDER BY flat_table_name, concept_id, id) m ON md.id = m.id
SET md.row_num = num
WHERE md.incremental_record = 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_incremental_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_incremental_cleanup;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_incremental_cleanup()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata_incremental_cleanup', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata_incremental_cleanup', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- delete un wanted rows after inserting columns that were not given in the .json config file into the meta data table,
-- all rows with 'AUTO-GENERATE' are not used anymore. Delete them/1
DELETE
FROM mamba_concept_metadata
WHERE incremental_record = 1
  AND concept_uuid = 'AUTO-GENERATE'
  AND column_label = 'AUTO-GENERATE';

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_missing_columns_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_missing_columns_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_missing_columns_incremental_insert()
BEGIN

    DECLARE encounter_type_uuid_value CHAR(38);
    DECLARE report_name_val VARCHAR(100);
    DECLARE encounter_type_id_val INT;
    DECLARE flat_table_name_val VARCHAR(255);

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_encounters CURSOR FOR
        SELECT DISTINCT(encounter_type_uuid), m.report_name, m.flat_table_name, et.encounter_type_id
        FROM mamba_concept_metadata m
                 INNER JOIN mamba_dim_encounter_type et ON m.encounter_type_uuid = et.uuid
        WHERE et.retired = 0
          AND m.concept_uuid = 'AUTO-GENERATE'
          AND m.column_label = 'AUTO-GENERATE'
          AND m.incremental_record = 1;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_encounters;
    computations_loop:
    LOOP
        FETCH cursor_encounters
            INTO encounter_type_uuid_value, report_name_val, flat_table_name_val, encounter_type_id_val;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        SET @insert_stmt = CONCAT(
                'INSERT INTO mamba_concept_metadata
                (
                    report_name,
                    flat_table_name,
                    encounter_type_uuid,
                    column_label,
                    concept_uuid,
                    incremental_record
                )
                SELECT
                    ''', report_name_val, ''',
                ''', flat_table_name_val, ''',
                ''', encounter_type_uuid_value, ''',
                field_name,
                concept_uuid,
                1
                FROM (
                     SELECT
                          DISTINCT et.encounter_type_id,
                          c.auto_table_column_name AS field_name,
                          c.uuid AS concept_uuid
                     FROM mamba_source_db.obs o
                          INNER JOIN mamba_source_db.encounter e
                            ON e.encounter_id = o.encounter_id
                          INNER JOIN mamba_dim_encounter_type et
                            ON e.encounter_type = et.encounter_type_id
                          INNER JOIN mamba_dim_concept c
                            ON o.concept_id = c.concept_id
                     WHERE et.encounter_type_id = ''', encounter_type_id_val, '''
                       AND et.retired = 0
                ) mamba_missing_concept;
            ');

        PREPARE inserttbl FROM @insert_stmt;
        EXECUTE inserttbl;
        DEALLOCATE PREPARE inserttbl;

    END LOOP computations_loop;
    CLOSE cursor_encounters;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_create;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_flat_table_config
(
    id                   INT           NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    encounter_type_id    INT           NOT NULL UNIQUE,
    report_name          VARCHAR(100)  NOT NULL,
    table_json_data      JSON          NOT NULL,
    table_json_data_hash CHAR(32)      NULL,
    encounter_type_uuid  CHAR(38)      NOT NULL,
    incremental_record   INT DEFAULT 0 NOT NULL COMMENT 'Whether `table_json_data` has been modified or not',

    INDEX mamba_idx_encounter_type_id (encounter_type_id),
    INDEX mamba_idx_report_name (report_name),
    INDEX mamba_idx_table_json_data_hash (table_json_data_hash),
    INDEX mamba_idx_uuid (encounter_type_uuid),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_insert_helper_manual  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- manually extracts user given flat table config file json into the mamba_flat_table_config table
-- this data together with automatically extracted flat table data is inserted into the mamba_flat_table_config table
-- later it is processed by the 'fn_mamba_generate_report_array_from_automated_json_table' function
-- into the @report_data variable inside the compile-mysql.sh script

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_insert_helper_manual;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_insert_helper_manual(
    IN report_data JSON
)
BEGIN

    DECLARE report_count INT DEFAULT 0;
    DECLARE report_array_len INT;
    DECLARE report_enc_type_id INT DEFAULT NULL;
    DECLARE report_enc_type_uuid VARCHAR(50);
    DECLARE report_enc_name VARCHAR(500);

    SET session group_concat_max_len = 200000;

    SELECT JSON_EXTRACT(report_data, '$.flat_report_metadata') INTO @report_array;
    SELECT JSON_LENGTH(@report_array) INTO report_array_len;

    WHILE report_count < report_array_len
        DO

            SELECT JSON_EXTRACT(@report_array, CONCAT('$[', report_count, ']')) INTO @report_data_item;
            SELECT JSON_EXTRACT(@report_data_item, '$.report_name') INTO report_enc_name;
            SELECT JSON_EXTRACT(@report_data_item, '$.encounter_type_uuid') INTO report_enc_type_uuid;

            SET report_enc_type_uuid = JSON_UNQUOTE(report_enc_type_uuid);

            SET report_enc_type_id = (SELECT DISTINCT et.encounter_type_id
                                      FROM mamba_dim_encounter_type et
                                      WHERE et.uuid = report_enc_type_uuid
                                      LIMIT 1);

            IF report_enc_type_id IS NOT NULL THEN
                INSERT INTO mamba_flat_table_config
                (report_name,
                 encounter_type_id,
                 table_json_data,
                 encounter_type_uuid)
                VALUES (JSON_UNQUOTE(report_enc_name),
                        report_enc_type_id,
                        @report_data_item,
                        report_enc_type_uuid);
            END IF;

            SET report_count = report_count + 1;

        END WHILE;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_insert_helper_auto  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder
DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_insert_helper_auto;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_insert_helper_auto()
main_block:
BEGIN

    DECLARE encounter_type_name CHAR(50) CHARACTER SET UTF8MB4;
    DECLARE is_automatic_flattening TINYINT(1);

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_encounter_type_name CURSOR FOR
        SELECT DISTINCT et.name
        FROM mamba_source_db.obs o
                 INNER JOIN mamba_source_db.encounter e ON e.encounter_id = o.encounter_id
                 INNER JOIN mamba_dim_encounter_type et ON e.encounter_type = et.encounter_type_id
        WHERE et.encounter_type_id NOT IN (SELECT DISTINCT tc.encounter_type_id from mamba_flat_table_config tc)
          AND et.retired = 0;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SELECT DISTINCT(automatic_flattening_mode_switch)
    INTO is_automatic_flattening
    FROM _mamba_etl_user_settings;

    -- If auto-flattening is not switched on, do nothing
    IF is_automatic_flattening = 0 THEN
        LEAVE main_block;
    END IF;

    OPEN cursor_encounter_type_name;
    computations_loop:
    LOOP
        FETCH cursor_encounter_type_name INTO encounter_type_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        SET @insert_stmt = CONCAT(
                'INSERT INTO mamba_flat_table_config(report_name, encounter_type_id, table_json_data, encounter_type_uuid)
                    SELECT
                        name,
                        encounter_type_id,
                         CONCAT(''{'',
                            ''"report_name": "'', name, ''", '',
                            ''"flat_table_name": "'', table_name, ''", '',
                            ''"encounter_type_uuid": "'', uuid, ''", '',
                            ''"table_columns": '', json_obj, '' '',
                            ''}'') AS table_json_data,
                        encounter_type_uuid
                    FROM (
                        SELECT DISTINCT
                            et.name,
                            encounter_type_id,
                            et.auto_flat_table_name AS table_name,
                            et.uuid, ',
                '(
                SELECT DISTINCT CONCAT(''{'', GROUP_CONCAT(CONCAT(''"'', name, ''":"'', uuid, ''"'') SEPARATOR '','' ),''}'') x
                FROM (
                        SELECT
                            DISTINCT et.encounter_type_id,
                            c.auto_table_column_name AS name,
                            c.uuid
                        FROM mamba_source_db.obs o
                        INNER JOIN mamba_source_db.encounter e
                                  ON e.encounter_id = o.encounter_id
                        INNER JOIN mamba_dim_encounter_type et
                                  ON e.encounter_type = et.encounter_type_id
                        INNER JOIN mamba_dim_concept c
                                  ON o.concept_id = c.concept_id
                        WHERE et.name = ''', encounter_type_name, '''
                                    AND et.retired = 0
                                ) json_obj
                        ) json_obj,
                       et.uuid as encounter_type_uuid
                    FROM mamba_dim_encounter_type et
                    INNER JOIN mamba_source_db.encounter e
                        ON e.encounter_type = et.encounter_type_id
                    WHERE et.name = ''', encounter_type_name, '''
                ) X  ;   ');
        PREPARE inserttbl FROM @insert_stmt;
        EXECUTE inserttbl;
        DEALLOCATE PREPARE inserttbl;
    END LOOP computations_loop;
    CLOSE cursor_encounter_type_name;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_insert;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN


SET @report_data = '{"flat_report_metadata":[{
  "encounter_type_uuid": "136b2ded-22a3-4831-a39a-088d35a50ef5",
  "flat_table_name": "mamba_flat_encounter_follow_up",
  "report_name": "Follow Up",
  "table_columns": {
    "adherence": "23d97715-589c-4dcf-bb86-70e26bba2269",
    "anitiretroviral_adherence_level": "b1a646d3-78ff-4dd5-823a-5bef7d69ff3d",
    "antiretroviral_art_dispensed_dose_i": "f3911009-1a8f-42ee-bdfc-1e343c2839aa",
    "are_there_any_ois_": "cd16e7e6-85cb-441a-a718-18d76957edac",
    "are_there_any_side_effects_": "6d9482a5-4686-4fa2-a35a-ea6c0daa5d1f",
    "are_there_any_side_effects_for_tpt_": "2705e6f9-960c-4d8c-be78-2929a8ee418d",
    "art_antiretroviral_start_date": "ae329187-6232-4142-aa91-22c85bc8e5b5",
    "assessed_for_pain": "7c67f18a-d9ff-4e4c-8c55-35ea70b9c697",
    "assessment_date": "78c8abfb-1989-444a-8750-947227f4bde8",
    "assessment_status": "e6e24c87-d3a0-4a38-bd04-560b13f92298",
    "biopsy_result": "df94b4c4-8a3a-46b2-be5b-e948403081a0",
    "biopsy_result_received_date": "473c914c-53d8-496c-a85c-32787a7d95b0",
    "biopsy_sample_collected_date": "5c93668e-6206-4cce-bdf9-7c6fb02991df",
    "bmi_for_age_children_between_age_5_": "eeeaf256-e133-42fa-b167-dbf9edf33e77",
    "bmi_text_": "e3c2efba-3ec9-4029-9bd9-716948995433",
    "cd4_": "730AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cd4_count": "5497AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cd4_done": "4868dd2d-4d56-4e72-8c89-8658a32a9072",
    "cervical_cancer_screening_method_st": "c842a287-f94c-48ee-a370-bd6540a0d1af",
    "cervical_cancer_screening_status": "01c546b4-e08a-4c0c-82ef-d387cab6bbbf",
    "client_sets_hiv_prevention_plan": "a3758771-f534-495e-9a3c-290acc79dda0",
    "colposcopy_exam_date": "eb135e8e-5e19-4d6e-ad71-c6bdab26f73d",
    "colposcopy_of_cervix_findings": "93bf2a3e-1675-44c9-b7ee-b8ba9cb32b22",
    "confirmed_cervical_cancer_cases_bas": "6b78badd-0b92-47f8-b16c-46559d5179b2",
    "cotrimoxazole_adherence_level": "af63fc87-379b-4fc4-8e68-936026da9f54",
    "cotrimoxazole_dispensed_dose_in_day": "629c6f5d-a10d-408a-9daa-d44383c8d653",
    "cotrimoxazole_prophylaxis_start_dat": "164361AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cotrimoxazole_prophylaxis_stop_date": "164362AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "current_functional_status": "2b7b8471-b19e-4d55-b2a4-4fe5b80f889a",
    "current_visit_observations": "7d175fa9-e64c-4923-ae6d-e35512be07a3",
    "current_who_hiv_stage": "5356AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "currently_breastfeeding_child": "5632AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "currently_taking_tuberculosis_proph": "166449AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cytology_done_": "ccf40a4b-072c-4648-ae34-731d4278d15d",
    "cytology_sample_collection_date": "3b5034de-ce0f-4017-80ab-17746ab3fe15",
    "date_active_tbrx_completed": "d4a98e9e-26b0-429b-82cb-e6a2197eeb05",
    "date_active_tbrx_dc": "93c114b1-ddcb-41eb-bca5-0c08a2a8c349",
    "date_client_arrived_in_the_referred": "88571a39-5caf-4260-b8d6-d0e28ca37410",
    "date_client_served_in_the_referred_": "080e8ad4-809e-4d51-a2a9-6eaac774ad38",
    "date_completed_tuberculosis_prophyl": "162279AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_counseling_given": "72a28ebe-77ba-4592-9291-ac91e46ea770",
    "date_cytology_result_received": "f0892f21-406c-446b-abd5-bb62f3ea2387",
    "date_discontinued_tuberculosis_prop": "162281AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_enrolled_for_cervical_cancer_s": "c7ecf767-325a-41c2-80a7-79c91762ab3e",
    "date_first_enhanced_adherence_couns": "70788495-b7d5-4484-b571-88383409c386",
    "date_hpv_test_was_done": "8b57d62c-c9a3-454a-b1af-929ca69603ce",
    "date_linked_to_cervical_cancer_scre": "2df6bd1b-c200-4363-8293-0d72ef24e8b7",
    "date_medically_eligible_for_antiret": "162227AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_of_event": "160753AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_of_last_menstrual_period_lmp_": "30d8d278-5ace-4d01-b2f1-8efc28372070",
    "date_of_reported_hiv_viral_load": "163281AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_patient_referred_out": "161561AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_second_enhanced_adherence_coun": "25f8b459-94b5-4ea9-8b64-9ccf3f8422e7",
    "date_started_on_tuberculosis_prophy": "162320AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_third_enhanced_adherence_couns": "95f843ff-07d4-4c46-8642-06e935b69be2",
    "date_viral_load_results_received": "beeede36-cae4-4f6e-b4b9-e39e37353a82",
    "date_visual_inspection_of_the_cervi": "f46c7ed3-65c3-451c-a8e1-4c615f795db1",
    "developmental_exam_findings": "1200AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "diagnosis_date": "159948AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "diagnostic_test": "002240c0-8672-4631-a32d-9bb9c34e4665",
    "dsd_category": "defeb4ff-d07b-4e4a-bbd6-d4281c1384a2",
    "dsd_category_changed_by_this_date": "eae5830f-ac48-48f0-bebd-35c500de8ff9",
    "eats_nutritious_foods": "161005AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "edema": "6175fb23-7046-4fc0-9cc9-e43f5d448cd8",
    "eligibility_status": "9ed5856a-a20a-44d2-bc8e-2acaa68cf11b",
    "eligible_for_tpt": "30b80afc-6358-404a-89c4-af5762faaeee",
    "enhanced_adherence_counseling_provi": "ebb4f716-7da3-4110-b9e2-6d6838140346",
    "enrolled_to_otz_operation_triple_zo": "d61f6440-5d4a-4389-a162-61c1ba13963e",
    "estimated_date_of_confinement": "5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "fluconazole_start_date": "5ac4300a-5e19-45c8-8692-31a57d6d5b8c",
    "fluconazole_started": "a1173486-b6c1-4d0a-886d-1126b6b558a1",
    "follow_up_date_followup_": "5c118396-52dc-4cac-8860-e6d8e4a7f296",
    "follow_up_status": "222f64a8-a603-4d2e-b70e-2d90b622bb04",
    "gene_xpert_result": "a10a7db7-5ec5-4d62-9b35-cc9ac222a05c",
    "height": "f980e9d2-e85c-483e-ae93-980623114e6f",
    "hgb_g_dl_": "8729e0b9-c1b4-416f-b47e-9d9b8855d023",
    "hiv_viral_load": "856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "hiv_viral_load_status": "163310AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "hpv_dna_result_received_date": "510f2a47-3761-4903-b7eb-8ea389cecfe9",
    "hpv_dna_screening_result": "8ecc6d15-26dd-4840-8667-a517a93bea5f",
    "hpv_subtype": "7bb81ac2-7a2a-4870-b965-fd3883d36f20",
    "lab_id": "051f22d7-24dc-423c-a13d-b5de1e2e8361",
    "lf_lam_result": "98ff157c-c736-4078-8acd-847a74accb64",
    "linked_to_cervical_cancer_screening": "a3998691-d9cc-492b-81f2-7bd28a6e413b",
    "method_of_family_planning": "374AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "mid_upper_arm_circumference": "1343AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "mother_enrolled_in_prevention_of_ma": "163532AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "next_follow_up_screening_date": "4ce065b6-aecb-46a3-b60b-41bc5dc8022f",
    "next_visit_date": "c596f199-4d76-4eca-b3c4-ffa631c0aee9",
    "nutritional_screening_result": "2c5b553b-58f8-4462-b14e-0a6a6628a790",
    "nutritional_status_of_adult": "ae4d72a4-ccf5-49ff-b395-6687c534b1a2",
    "nutritional_status_of_older_child_a": "cdcebe52-8acc-4eaa-ba6d-ceef4bde644f",
    "nutritional_supplements_provided": "a0f1f531-e082-448a-aed5-200621c7b274",
    "on_antiretroviral_therapy": "1149AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "on_family_planning": "8d5be308-7205-4a57-844e-968f62850e65",
    "operation_triple_zero_enrollment_da": "0fe4faee-0717-4dc0-be3d-1cd52923804a",
    "other_medications_med_1": "7616e1cd-0c39-4fc3-a9e6-14a69e72655a",
    "pain_level_coded_": "166000AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "patient_diagnosed_with_active_tuber": "164500AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "patient_wishes_to_get_pregnant": "160571AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "pre_test_counselling_for_cervical_c": "fc5ec0e6-8e56-4a23-8bf9-fbe464da12c7",
    "pregnancy_status": "5272AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "purpose_for_visit_cervical_screenin": "2c6f75a8-f35c-4671-939e-ebcc680c48a0",
    "ready_for_cervical_cancer_screening": "1b1dc36e-fe65-4f4b-8304-09fbd9c106ad",
    "reason_for_art_regimen_change": "ae280ece-d408-4e49-b472-0dba1c918d10",
    "reason_for_dsd_category_change": "2780c548-5118-4ae4-99da-c8557cb55f28",
    "reason_for_poor_treatment_adherence": "160582AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "reason_for_referral_cacx": "52106755-062c-4cd5-a627-2373f5a0cef0",
    "regimen": "6d7d0327-e1f8-4246-bfe5-be1e82d94b14",
    "regimen_change": "f5c27f2a-a2a3-4e91-91c9-488f4f4eb3b6",
    "regimen_substitution_type": "94747e61-6350-4382-817a-265f18501758",
    "regimen_switch_type": "8825ff0d-de38-4525-8130-86738c70c599",
    "routine_viral_load_test_indication": "9b8cef86-9093-4737-a641-3b8399618c85",
    "screening_test_result_tuberculosis": "160108AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "specimen_sent_to_lab": "161934AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "stages_of_disclosure": "685eaed5-b695-499e-96c5-d9ce95d8b7df",
    "taking_co_trimoxazole_preventive_th": "160434AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "targeted_viral_load_test_indication": "8f75ce27-29fa-4a67-bc8a-295c94323220",
    "tb_diagnostic_test_result": "c20140f7-d45d-4b44-a1b9-0534861a615d",
    "tb_prophylaxis_type": "54084c9e-bc87-4d95-89fc-eb9a2cffb592",
    "tb_prophylaxis_type_alternate_": "f18eded5-67a9-4f02-b131-9a6230e64d18",
    "tb_related_ois_opportunistic_illnes": "efe20a31-6781-40b7-a4d8-5fc347881e76",
    "tb_screening_date": "179497a0-6f07-469f-bb2e-9b85644a82af",
    "tb_treatment_status": "cc9215e5-454f-4cdf-a773-62b2f2f17fdb",
    "tpt_dispensed_dose_in_days_alternat": "cc80b9ac-2ed1-4fd5-969a-9e324e91e95e",
    "tpt_dispensed_dose_in_days_inh_": "ad542a8d-cd7c-4d70-8ef3-829b89c05009",
    "tpt_followup_6h_": "0166677a-5a8e-45fa-b3f6-3c5aa9f13d00",
    "tpt_followup_alternate_for_3hp": "c1af657e-bc31-46a2-9f5e-55a1c9ae7507",
    "tpt_followup_alternate_for_3hr": "43f17059-ad82-43d7-bb0b-fe5315abdd07",
    "tpt_side_effects": "87174948-2998-47b5-a551-64ea23ab9862",
    "treatment_end_date": "164384AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "treatment_of_pre_cancerous_lesions_": "6b97156c-3795-48d0-a15c-4f2590ffef54",
    "treatment_start_date": "163526AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "tuberculosis_drug_treatment_start_d": "1113AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "type_of_hiv_test": "5583b0a6-f390-446d-ab39-d98584ee330c",
    "via_done_": "fa346c0e-a1a0-4758-87a3-12f2dcb6c0e9",
    "via_screening_result": "ff6b60e4-7310-4ddc-98ce-a2910c32a7a0",
    "viral_load_received_": "f429af1a-8eba-48ab-a0f7-9d69652753e7",
    "viral_load_test_indication": "6bb5b796-60bc-406c-abd9-fb9362ed5e80",
    "viral_load_test_status": "2dc9ee04-4d12-4606-ae0f-86895bf14a44",
    "visit_type": "b3f60308-cda4-41f9-af08-b98d2c1562c7",
    "was_the_patient_screened_for_tuberc": "feebf47b-c11e-4fa7-bb4b-1a9fc444bcc9",
    "weight_for_age_status": "1854AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "weight_text_": "4ab93a3c-4373-4b9b-9268-5ff0641cc242",

    "cytology_result": "9e5c5bd8-276c-497b-9ea1-9a5c9f94faa7",
    "crag": "ac95e1ae-ee9a-4fdc-8fb6-d5fd640727f7",
    "creatinine_cr_mg_dl_": "5f39ac9e-7338-4a79-8b40-58dbef9debb0",
    "ast_u_l_": "33297367-9a74-4e2c-a474-923e61d45252",
    "antiretroviral_side_effects": "9d4cf346-a23e-4c8e-9ed0-4e6fc274cbe6",
    "alt_u_l_": "49296372-8a07-4182-8506-ded37e2bc667",
    "date_of_hiv_diagnosis": "160554AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
  }
},{
  "report_name": "Intake A",
  "flat_table_name": "mamba_flat_encounter_intake_a",
  "encounter_type_uuid": "05add044-67f8-48c9-928d-79002ab19efe",
  "table_columns": {
    "respondent_age": "1532AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "hiv_test_result": "2e770be1-7397-4684-bea6-6632c23b00d7",
    "respondent_gender": "1533AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "within_the_facility": "d2b461e5-dd7f-4d16-968e-354ac68cbd38",
    "outside_the_facility": "73d76014-365e-484c-bf5b-0959add4e3f3",
    "date_enrolled_in_care": "1ebc345c-6f09-43e1-a616-d7e52fff4c7d",
    "reason_for_patient_referral": "5f34e705-3574-4b44-b455-d815962026a1",
    "catchment_area_residency_status": "160637AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "challenges_with_regular_follow_up": "f93c0a7d-9578-4fb2-afd1-6a7f477a7e1a",
    "other_entry_point_from_outside_the_": "2c30c599-1e4f-46f9-8488-5ab57cdc8ac3",
    "other_entry_point_from_within_the_f": "a0368d36-686d-4b98-b60e-e62cb3d46c9b",
    "who_knows_your_child_s_hiv_positive": "de8937b6-6ebe-4302-a488-2198be481839"
  }
}]}';

CALL sp_mamba_flat_table_config_insert_helper_manual(@report_data); -- insert manually added config JSON data from config dir
CALL sp_mamba_flat_table_config_insert_helper_auto(); -- insert automatically generated config JSON data from db

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_update;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update the hash of the JSON data
UPDATE mamba_flat_table_config
SET table_json_data_hash = MD5(TRIM(table_json_data))
WHERE id > 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_flat_table_config_create();
CALL sp_mamba_flat_table_config_insert();
CALL sp_mamba_flat_table_config_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_incremental_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_incremental_create;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_incremental_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config_incremental_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config_incremental_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE IF NOT EXISTS mamba_flat_table_config_incremental
(
    id                   INT           NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    encounter_type_id    INT           NOT NULL UNIQUE,
    report_name          VARCHAR(100)  NOT NULL,
    table_json_data      JSON          NOT NULL,
    table_json_data_hash CHAR(32)      NULL,
    encounter_type_uuid  CHAR(38)      NOT NULL,
    incremental_record   INT DEFAULT 0 NOT NULL COMMENT 'Whether `table_json_data` has been modified or not',

    INDEX mamba_idx_encounter_type_id (encounter_type_id),
    INDEX mamba_idx_report_name (report_name),
    INDEX mamba_idx_table_json_data_hash (table_json_data_hash),
    INDEX mamba_idx_uuid (encounter_type_uuid),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_incremental_insert_helper_manual  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- manually extracts user given flat table config file json into the mamba_flat_table_config_incremental table
-- this data together with automatically extracted flat table data is inserted into the mamba_flat_table_config_incremental table
-- later it is processed by the 'fn_mamba_generate_report_array_from_automated_json_table' function
-- into the @report_data variable inside the compile-mysql.sh script

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_incremental_insert_helper_manual;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_incremental_insert_helper_manual(
    IN report_data MEDIUMTEXT CHARACTER SET UTF8MB4
)
BEGIN

    DECLARE report_count INT DEFAULT 0;
    DECLARE report_array_len INT;
    DECLARE report_enc_type_id INT DEFAULT NULL;
    DECLARE report_enc_type_uuid VARCHAR(50);
    DECLARE report_enc_name VARCHAR(500);

    SET session group_concat_max_len = 20000;

    SELECT JSON_EXTRACT(report_data, '$.flat_report_metadata') INTO @report_array;
    SELECT JSON_LENGTH(@report_array) INTO report_array_len;

    WHILE report_count < report_array_len
        DO

            SELECT JSON_EXTRACT(@report_array, CONCAT('$[', report_count, ']')) INTO @report_data_item;
            SELECT JSON_EXTRACT(@report_data_item, '$.report_name') INTO report_enc_name;
            SELECT JSON_EXTRACT(@report_data_item, '$.encounter_type_uuid') INTO report_enc_type_uuid;

            SET report_enc_type_uuid = JSON_UNQUOTE(report_enc_type_uuid);

            SET report_enc_type_id = (SELECT DISTINCT et.encounter_type_id
                                      FROM mamba_dim_encounter_type et
                                      WHERE et.uuid = report_enc_type_uuid
                                      LIMIT 1);

            IF report_enc_type_id IS NOT NULL THEN
                INSERT INTO mamba_flat_table_config_incremental
                (report_name,
                 encounter_type_id,
                 table_json_data,
                 encounter_type_uuid)
                VALUES (JSON_UNQUOTE(report_enc_name),
                        report_enc_type_id,
                        @report_data_item,
                        report_enc_type_uuid);
            END IF;

            SET report_count = report_count + 1;

        END WHILE;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_incremental_insert_helper_auto  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder
DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_incremental_insert_helper_auto;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_incremental_insert_helper_auto()
main_block:
BEGIN

    DECLARE encounter_type_name CHAR(50) CHARACTER SET UTF8MB4;
    DECLARE is_automatic_flattening TINYINT(1);

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_encounter_type_name CURSOR FOR
        SELECT DISTINCT et.name
        FROM mamba_source_db.obs o
                 INNER JOIN mamba_source_db.encounter e ON e.encounter_id = o.encounter_id
                 INNER JOIN mamba_dim_encounter_type et ON e.encounter_type = et.encounter_type_id
        WHERE et.encounter_type_id NOT IN (SELECT DISTINCT tc.encounter_type_id from mamba_flat_table_config_incremental tc)
          AND et.retired = 0;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SELECT DISTINCT(automatic_flattening_mode_switch)
    INTO is_automatic_flattening
    FROM _mamba_etl_user_settings;

    -- If auto-flattening is not switched on, do nothing
    IF is_automatic_flattening = 0 THEN
        LEAVE main_block;
    END IF;

    OPEN cursor_encounter_type_name;
    computations_loop:
    LOOP
        FETCH cursor_encounter_type_name INTO encounter_type_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        SET @insert_stmt = CONCAT(
                'INSERT INTO mamba_flat_table_config_incremental (report_name, encounter_type_id, table_json_data, encounter_type_uuid)
                    SELECT
                        name,
                        encounter_type_id,
                         CONCAT(''{'',
                            ''"report_name": "'', name, ''", '',
                            ''"flat_table_name": "'', table_name, ''", '',
                            ''"encounter_type_uuid": "'', uuid, ''", '',
                            ''"table_columns": '', json_obj, '' '',
                            ''}'') AS table_json_data,
                        encounter_type_uuid
                    FROM (
                        SELECT DISTINCT
                            et.name,
                            encounter_type_id,
                            et.auto_flat_table_name AS table_name,
                            et.uuid, ',
                '(
                SELECT DISTINCT CONCAT(''{'', GROUP_CONCAT(CONCAT(''"'', name, ''":"'', uuid, ''"'') SEPARATOR '','' ),''}'') x
                FROM (
                        SELECT
                            DISTINCT et.encounter_type_id,
                            c.auto_table_column_name AS name,
                            c.uuid
                        FROM mamba_source_db.obs o
                        INNER JOIN mamba_source_db.encounter e
                                  ON e.encounter_id = o.encounter_id
                        INNER JOIN mamba_dim_encounter_type et
                                  ON e.encounter_type = et.encounter_type_id
                        INNER JOIN mamba_dim_concept c
                                  ON o.concept_id = c.concept_id
                        WHERE et.name = ''', encounter_type_name, '''
                                    AND et.retired = 0
                                ) json_obj
                        ) json_obj,
                       et.uuid as encounter_type_uuid
                    FROM mamba_dim_encounter_type et
                    INNER JOIN mamba_source_db.encounter e
                        ON e.encounter_type = et.encounter_type_id
                    WHERE et.name = ''', encounter_type_name, '''
                ) X  ;   ');
        PREPARE inserttbl FROM @insert_stmt;
        EXECUTE inserttbl;
        DEALLOCATE PREPARE inserttbl;
    END LOOP computations_loop;
    CLOSE cursor_encounter_type_name;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN


SET @report_data = '{"flat_report_metadata":[{
  "encounter_type_uuid": "136b2ded-22a3-4831-a39a-088d35a50ef5",
  "flat_table_name": "mamba_flat_encounter_follow_up",
  "report_name": "Follow Up",
  "table_columns": {
    "adherence": "23d97715-589c-4dcf-bb86-70e26bba2269",
    "anitiretroviral_adherence_level": "b1a646d3-78ff-4dd5-823a-5bef7d69ff3d",
    "antiretroviral_art_dispensed_dose_i": "f3911009-1a8f-42ee-bdfc-1e343c2839aa",
    "are_there_any_ois_": "cd16e7e6-85cb-441a-a718-18d76957edac",
    "are_there_any_side_effects_": "6d9482a5-4686-4fa2-a35a-ea6c0daa5d1f",
    "are_there_any_side_effects_for_tpt_": "2705e6f9-960c-4d8c-be78-2929a8ee418d",
    "art_antiretroviral_start_date": "ae329187-6232-4142-aa91-22c85bc8e5b5",
    "assessed_for_pain": "7c67f18a-d9ff-4e4c-8c55-35ea70b9c697",
    "assessment_date": "78c8abfb-1989-444a-8750-947227f4bde8",
    "assessment_status": "e6e24c87-d3a0-4a38-bd04-560b13f92298",
    "biopsy_result": "df94b4c4-8a3a-46b2-be5b-e948403081a0",
    "biopsy_result_received_date": "473c914c-53d8-496c-a85c-32787a7d95b0",
    "biopsy_sample_collected_date": "5c93668e-6206-4cce-bdf9-7c6fb02991df",
    "bmi_for_age_children_between_age_5_": "eeeaf256-e133-42fa-b167-dbf9edf33e77",
    "bmi_text_": "e3c2efba-3ec9-4029-9bd9-716948995433",
    "cd4_": "730AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cd4_count": "5497AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cd4_done": "4868dd2d-4d56-4e72-8c89-8658a32a9072",
    "cervical_cancer_screening_method_st": "c842a287-f94c-48ee-a370-bd6540a0d1af",
    "cervical_cancer_screening_status": "01c546b4-e08a-4c0c-82ef-d387cab6bbbf",
    "client_sets_hiv_prevention_plan": "a3758771-f534-495e-9a3c-290acc79dda0",
    "colposcopy_exam_date": "eb135e8e-5e19-4d6e-ad71-c6bdab26f73d",
    "colposcopy_of_cervix_findings": "93bf2a3e-1675-44c9-b7ee-b8ba9cb32b22",
    "confirmed_cervical_cancer_cases_bas": "6b78badd-0b92-47f8-b16c-46559d5179b2",
    "cotrimoxazole_adherence_level": "af63fc87-379b-4fc4-8e68-936026da9f54",
    "cotrimoxazole_dispensed_dose_in_day": "629c6f5d-a10d-408a-9daa-d44383c8d653",
    "cotrimoxazole_prophylaxis_start_dat": "164361AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cotrimoxazole_prophylaxis_stop_date": "164362AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "current_functional_status": "2b7b8471-b19e-4d55-b2a4-4fe5b80f889a",
    "current_visit_observations": "7d175fa9-e64c-4923-ae6d-e35512be07a3",
    "current_who_hiv_stage": "5356AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "currently_breastfeeding_child": "5632AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "currently_taking_tuberculosis_proph": "166449AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cytology_done_": "ccf40a4b-072c-4648-ae34-731d4278d15d",
    "cytology_sample_collection_date": "3b5034de-ce0f-4017-80ab-17746ab3fe15",
    "date_active_tbrx_completed": "d4a98e9e-26b0-429b-82cb-e6a2197eeb05",
    "date_active_tbrx_dc": "93c114b1-ddcb-41eb-bca5-0c08a2a8c349",
    "date_client_arrived_in_the_referred": "88571a39-5caf-4260-b8d6-d0e28ca37410",
    "date_client_served_in_the_referred_": "080e8ad4-809e-4d51-a2a9-6eaac774ad38",
    "date_completed_tuberculosis_prophyl": "162279AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_counseling_given": "72a28ebe-77ba-4592-9291-ac91e46ea770",
    "date_cytology_result_received": "f0892f21-406c-446b-abd5-bb62f3ea2387",
    "date_discontinued_tuberculosis_prop": "162281AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_enrolled_for_cervical_cancer_s": "c7ecf767-325a-41c2-80a7-79c91762ab3e",
    "date_first_enhanced_adherence_couns": "70788495-b7d5-4484-b571-88383409c386",
    "date_hpv_test_was_done": "8b57d62c-c9a3-454a-b1af-929ca69603ce",
    "date_linked_to_cervical_cancer_scre": "2df6bd1b-c200-4363-8293-0d72ef24e8b7",
    "date_medically_eligible_for_antiret": "162227AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_of_event": "160753AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_of_last_menstrual_period_lmp_": "30d8d278-5ace-4d01-b2f1-8efc28372070",
    "date_of_reported_hiv_viral_load": "163281AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_patient_referred_out": "161561AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_second_enhanced_adherence_coun": "25f8b459-94b5-4ea9-8b64-9ccf3f8422e7",
    "date_started_on_tuberculosis_prophy": "162320AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_third_enhanced_adherence_couns": "95f843ff-07d4-4c46-8642-06e935b69be2",
    "date_viral_load_results_received": "beeede36-cae4-4f6e-b4b9-e39e37353a82",
    "date_visual_inspection_of_the_cervi": "f46c7ed3-65c3-451c-a8e1-4c615f795db1",
    "developmental_exam_findings": "1200AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "diagnosis_date": "159948AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "diagnostic_test": "002240c0-8672-4631-a32d-9bb9c34e4665",
    "dsd_category": "defeb4ff-d07b-4e4a-bbd6-d4281c1384a2",
    "dsd_category_changed_by_this_date": "eae5830f-ac48-48f0-bebd-35c500de8ff9",
    "eats_nutritious_foods": "161005AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "edema": "6175fb23-7046-4fc0-9cc9-e43f5d448cd8",
    "eligibility_status": "9ed5856a-a20a-44d2-bc8e-2acaa68cf11b",
    "eligible_for_tpt": "30b80afc-6358-404a-89c4-af5762faaeee",
    "enhanced_adherence_counseling_provi": "ebb4f716-7da3-4110-b9e2-6d6838140346",
    "enrolled_to_otz_operation_triple_zo": "d61f6440-5d4a-4389-a162-61c1ba13963e",
    "estimated_date_of_confinement": "5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "fluconazole_start_date": "5ac4300a-5e19-45c8-8692-31a57d6d5b8c",
    "fluconazole_started": "a1173486-b6c1-4d0a-886d-1126b6b558a1",
    "follow_up_date_followup_": "5c118396-52dc-4cac-8860-e6d8e4a7f296",
    "follow_up_status": "222f64a8-a603-4d2e-b70e-2d90b622bb04",
    "gene_xpert_result": "a10a7db7-5ec5-4d62-9b35-cc9ac222a05c",
    "height": "f980e9d2-e85c-483e-ae93-980623114e6f",
    "hgb_g_dl_": "8729e0b9-c1b4-416f-b47e-9d9b8855d023",
    "hiv_viral_load": "856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "hiv_viral_load_status": "163310AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "hpv_dna_result_received_date": "510f2a47-3761-4903-b7eb-8ea389cecfe9",
    "hpv_dna_screening_result": "8ecc6d15-26dd-4840-8667-a517a93bea5f",
    "hpv_subtype": "7bb81ac2-7a2a-4870-b965-fd3883d36f20",
    "lab_id": "051f22d7-24dc-423c-a13d-b5de1e2e8361",
    "lf_lam_result": "98ff157c-c736-4078-8acd-847a74accb64",
    "linked_to_cervical_cancer_screening": "a3998691-d9cc-492b-81f2-7bd28a6e413b",
    "method_of_family_planning": "374AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "mid_upper_arm_circumference": "1343AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "mother_enrolled_in_prevention_of_ma": "163532AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "next_follow_up_screening_date": "4ce065b6-aecb-46a3-b60b-41bc5dc8022f",
    "next_visit_date": "c596f199-4d76-4eca-b3c4-ffa631c0aee9",
    "nutritional_screening_result": "2c5b553b-58f8-4462-b14e-0a6a6628a790",
    "nutritional_status_of_adult": "ae4d72a4-ccf5-49ff-b395-6687c534b1a2",
    "nutritional_status_of_older_child_a": "cdcebe52-8acc-4eaa-ba6d-ceef4bde644f",
    "nutritional_supplements_provided": "a0f1f531-e082-448a-aed5-200621c7b274",
    "on_antiretroviral_therapy": "1149AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "on_family_planning": "8d5be308-7205-4a57-844e-968f62850e65",
    "operation_triple_zero_enrollment_da": "0fe4faee-0717-4dc0-be3d-1cd52923804a",
    "other_medications_med_1": "7616e1cd-0c39-4fc3-a9e6-14a69e72655a",
    "pain_level_coded_": "166000AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "patient_diagnosed_with_active_tuber": "164500AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "patient_wishes_to_get_pregnant": "160571AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "pre_test_counselling_for_cervical_c": "fc5ec0e6-8e56-4a23-8bf9-fbe464da12c7",
    "pregnancy_status": "5272AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "purpose_for_visit_cervical_screenin": "2c6f75a8-f35c-4671-939e-ebcc680c48a0",
    "ready_for_cervical_cancer_screening": "1b1dc36e-fe65-4f4b-8304-09fbd9c106ad",
    "reason_for_art_regimen_change": "ae280ece-d408-4e49-b472-0dba1c918d10",
    "reason_for_dsd_category_change": "2780c548-5118-4ae4-99da-c8557cb55f28",
    "reason_for_poor_treatment_adherence": "160582AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "reason_for_referral_cacx": "52106755-062c-4cd5-a627-2373f5a0cef0",
    "regimen": "6d7d0327-e1f8-4246-bfe5-be1e82d94b14",
    "regimen_change": "f5c27f2a-a2a3-4e91-91c9-488f4f4eb3b6",
    "regimen_substitution_type": "94747e61-6350-4382-817a-265f18501758",
    "regimen_switch_type": "8825ff0d-de38-4525-8130-86738c70c599",
    "routine_viral_load_test_indication": "9b8cef86-9093-4737-a641-3b8399618c85",
    "screening_test_result_tuberculosis": "160108AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "specimen_sent_to_lab": "161934AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "stages_of_disclosure": "685eaed5-b695-499e-96c5-d9ce95d8b7df",
    "taking_co_trimoxazole_preventive_th": "160434AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "targeted_viral_load_test_indication": "8f75ce27-29fa-4a67-bc8a-295c94323220",
    "tb_diagnostic_test_result": "c20140f7-d45d-4b44-a1b9-0534861a615d",
    "tb_prophylaxis_type": "54084c9e-bc87-4d95-89fc-eb9a2cffb592",
    "tb_prophylaxis_type_alternate_": "f18eded5-67a9-4f02-b131-9a6230e64d18",
    "tb_related_ois_opportunistic_illnes": "efe20a31-6781-40b7-a4d8-5fc347881e76",
    "tb_screening_date": "179497a0-6f07-469f-bb2e-9b85644a82af",
    "tb_treatment_status": "cc9215e5-454f-4cdf-a773-62b2f2f17fdb",
    "tpt_dispensed_dose_in_days_alternat": "cc80b9ac-2ed1-4fd5-969a-9e324e91e95e",
    "tpt_dispensed_dose_in_days_inh_": "ad542a8d-cd7c-4d70-8ef3-829b89c05009",
    "tpt_followup_6h_": "0166677a-5a8e-45fa-b3f6-3c5aa9f13d00",
    "tpt_followup_alternate_for_3hp": "c1af657e-bc31-46a2-9f5e-55a1c9ae7507",
    "tpt_followup_alternate_for_3hr": "43f17059-ad82-43d7-bb0b-fe5315abdd07",
    "tpt_side_effects": "87174948-2998-47b5-a551-64ea23ab9862",
    "treatment_end_date": "164384AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "treatment_of_pre_cancerous_lesions_": "6b97156c-3795-48d0-a15c-4f2590ffef54",
    "treatment_start_date": "163526AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "tuberculosis_drug_treatment_start_d": "1113AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "type_of_hiv_test": "5583b0a6-f390-446d-ab39-d98584ee330c",
    "via_done_": "fa346c0e-a1a0-4758-87a3-12f2dcb6c0e9",
    "via_screening_result": "ff6b60e4-7310-4ddc-98ce-a2910c32a7a0",
    "viral_load_received_": "f429af1a-8eba-48ab-a0f7-9d69652753e7",
    "viral_load_test_indication": "6bb5b796-60bc-406c-abd9-fb9362ed5e80",
    "viral_load_test_status": "2dc9ee04-4d12-4606-ae0f-86895bf14a44",
    "visit_type": "b3f60308-cda4-41f9-af08-b98d2c1562c7",
    "was_the_patient_screened_for_tuberc": "feebf47b-c11e-4fa7-bb4b-1a9fc444bcc9",
    "weight_for_age_status": "1854AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "weight_text_": "4ab93a3c-4373-4b9b-9268-5ff0641cc242",

    "cytology_result": "9e5c5bd8-276c-497b-9ea1-9a5c9f94faa7",
    "crag": "ac95e1ae-ee9a-4fdc-8fb6-d5fd640727f7",
    "creatinine_cr_mg_dl_": "5f39ac9e-7338-4a79-8b40-58dbef9debb0",
    "ast_u_l_": "33297367-9a74-4e2c-a474-923e61d45252",
    "antiretroviral_side_effects": "9d4cf346-a23e-4c8e-9ed0-4e6fc274cbe6",
    "alt_u_l_": "49296372-8a07-4182-8506-ded37e2bc667",
    "date_of_hiv_diagnosis": "160554AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
  }
},{
  "report_name": "Intake A",
  "flat_table_name": "mamba_flat_encounter_intake_a",
  "encounter_type_uuid": "05add044-67f8-48c9-928d-79002ab19efe",
  "table_columns": {
    "respondent_age": "1532AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "hiv_test_result": "2e770be1-7397-4684-bea6-6632c23b00d7",
    "respondent_gender": "1533AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "within_the_facility": "d2b461e5-dd7f-4d16-968e-354ac68cbd38",
    "outside_the_facility": "73d76014-365e-484c-bf5b-0959add4e3f3",
    "date_enrolled_in_care": "1ebc345c-6f09-43e1-a616-d7e52fff4c7d",
    "reason_for_patient_referral": "5f34e705-3574-4b44-b455-d815962026a1",
    "catchment_area_residency_status": "160637AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "challenges_with_regular_follow_up": "f93c0a7d-9578-4fb2-afd1-6a7f477a7e1a",
    "other_entry_point_from_outside_the_": "2c30c599-1e4f-46f9-8488-5ab57cdc8ac3",
    "other_entry_point_from_within_the_f": "a0368d36-686d-4b98-b60e-e62cb3d46c9b",
    "who_knows_your_child_s_hiv_positive": "de8937b6-6ebe-4302-a488-2198be481839"
  }
}]}';

CALL sp_mamba_flat_table_config_incremental_insert_helper_manual(@report_data); -- insert manually added config JSON data from config dir
CALL sp_mamba_flat_table_config_incremental_insert_helper_auto(); -- insert automatically generated config JSON data from db

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update the hash of the JSON data
UPDATE mamba_flat_table_config_incremental
SET table_json_data_hash = MD5(TRIM(table_json_data))
WHERE id > 0;

-- If a new encounter type has been added
INSERT INTO mamba_flat_table_config (report_name,
                                     encounter_type_id,
                                     table_json_data,
                                     encounter_type_uuid,
                                     table_json_data_hash,
                                     incremental_record)
SELECT tci.report_name,
       tci.encounter_type_id,
       tci.table_json_data,
       tci.encounter_type_uuid,
       tci.table_json_data_hash,
       1
FROM mamba_flat_table_config_incremental tci
WHERE tci.encounter_type_id NOT IN (SELECT encounter_type_id FROM mamba_flat_table_config);

-- If there is any change in either concepts or encounter types in terms of names or additional questions
UPDATE mamba_flat_table_config tc
    INNER JOIN mamba_flat_table_config_incremental tci ON tc.encounter_type_id = tci.encounter_type_id
SET tc.table_json_data      = tci.table_json_data,
    tc.table_json_data_hash = tci.table_json_data_hash,
    tc.report_name          = tci.report_name,
    tc.encounter_type_uuid  = tci.encounter_type_uuid,
    tc.incremental_record   = 1
WHERE tc.table_json_data_hash <> tci.table_json_data_hash
  AND tc.table_json_data_hash IS NOT NULL;

-- If an encounter type has been voided then delete it from dim_json
DELETE
FROM mamba_flat_table_config
WHERE encounter_type_id NOT IN (SELECT tci.encounter_type_id FROM mamba_flat_table_config_incremental tci);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_incremental_truncate  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_incremental_truncate;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_incremental_truncate()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config_incremental_truncate', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config_incremental_truncate', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_truncate_table('mamba_flat_table_config_incremental');
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_incremental;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_flat_table_config_incremental_create();
CALL sp_mamba_flat_table_config_incremental_truncate();
CALL sp_mamba_flat_table_config_incremental_insert();
CALL sp_mamba_flat_table_config_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_obs_group  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_obs_group;


~-~-
CREATE PROCEDURE sp_mamba_obs_group()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_obs_group', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_obs_group', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_obs_group_create();
CALL sp_mamba_obs_group_insert();
CALL sp_mamba_obs_group_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_obs_group_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_obs_group_create;


~-~-
CREATE PROCEDURE sp_mamba_obs_group_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_obs_group_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_obs_group_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_obs_group
(
    id                     INT          NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    obs_id                 INT          NOT NULL,
    obs_group_concept_id   INT          NOT NULL,
    obs_group_concept_name VARCHAR(255) NOT NULL, -- should be the concept name of the obs

    INDEX mamba_idx_obs_id (obs_id),
    INDEX mamba_idx_obs_group_concept_id (obs_group_concept_id),
    INDEX mamba_idx_obs_group_concept_name (obs_group_concept_name)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_obs_group_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_obs_group_insert;


~-~-
CREATE PROCEDURE sp_mamba_obs_group_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_obs_group_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_obs_group_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TEMPORARY TABLE mamba_temp_obs_group_ids
(
    obs_group_id INT NOT NULL,
    row_num      INT NOT NULL,

    INDEX mamba_idx_obs_group_id (obs_group_id),
    INDEX mamba_idx_visit_id (row_num)
)
    CHARSET = UTF8MB4;

INSERT INTO mamba_temp_obs_group_ids
SELECT obs_group_id,
       COUNT(*) AS row_num
FROM mamba_z_encounter_obs o
WHERE obs_group_id IS NOT NULL
GROUP BY obs_group_id, person_id, encounter_id;

INSERT INTO mamba_obs_group (obs_group_concept_id,
                             obs_group_concept_name,
                             obs_id)
SELECT DISTINCT o.obs_question_concept_id,
                LEFT(c.auto_table_column_name, 12) AS name,
                o.obs_id
FROM mamba_temp_obs_group_ids t
         INNER JOIN mamba_z_encounter_obs o
                    ON t.obs_group_id = o.obs_group_id
         INNER JOIN mamba_dim_concept c
                    ON o.obs_question_concept_id = c.concept_id
WHERE t.row_num > 1;

DROP TEMPORARY TABLE mamba_temp_obs_group_ids;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_obs_group_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_obs_group_update;


~-~-
CREATE PROCEDURE sp_mamba_obs_group_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_obs_group_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_obs_group_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_error_log_drop  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_error_log_drop;


~-~-
CREATE PROCEDURE sp_mamba_etl_error_log_drop()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_error_log_drop', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_error_log_drop', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

DROP TABLE IF EXISTS _mamba_etl_error_log;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_error_log_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_error_log_create;


~-~-
CREATE PROCEDURE sp_mamba_etl_error_log_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_error_log_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_error_log_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE _mamba_etl_error_log
(
    id             INT          NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'Primary Key',
    procedure_name VARCHAR(255) NOT NULL,
    error_message  VARCHAR(1000),
    error_code     INT,
    sql_state      VARCHAR(5),
    error_time     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_error_log_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_error_log_insert;


~-~-
CREATE PROCEDURE sp_mamba_etl_error_log_insert(
    IN procedure_name VARCHAR(255),
    IN error_message VARCHAR(1000),
    IN error_code INT,
    IN sql_state VARCHAR(5)
)
BEGIN
    INSERT INTO _mamba_etl_error_log (procedure_name, error_message, error_code, sql_state)
    VALUES (procedure_name, error_message, error_code, sql_state);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_error_log  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_error_log;


~-~-
CREATE PROCEDURE sp_mamba_etl_error_log()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_error_log', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_error_log', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_error_log_drop();
CALL sp_mamba_etl_error_log_create();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_user_settings_drop  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_user_settings_drop;


~-~-
CREATE PROCEDURE sp_mamba_etl_user_settings_drop()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_user_settings_drop', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_user_settings_drop', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

DROP TABLE IF EXISTS _mamba_etl_user_settings;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_user_settings_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_user_settings_create;


~-~-
CREATE PROCEDURE sp_mamba_etl_user_settings_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_user_settings_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_user_settings_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE _mamba_etl_user_settings
(
    id                               INT          NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY COMMENT 'Primary Key',
    openmrs_database                 VARCHAR(255) NOT NULL COMMENT 'Name of the OpenMRS (source) database',
    etl_database                     VARCHAR(255) NOT NULL COMMENT 'Name of the ETL (target) database',
    concepts_locale                  CHAR(4)      NOT NULL COMMENT 'Preferred Locale of the Concept names',
    table_partition_number           INT          NOT NULL COMMENT 'Number of columns at which to partition \'many columned\' Tables',
    incremental_mode_switch          TINYINT(1)   NOT NULL COMMENT 'If MambaETL should/not run in Incremental Mode',
    automatic_flattening_mode_switch TINYINT(1)   NOT NULL COMMENT 'If MambaETL should/not automatically flatten ALL encounter types',
    etl_interval_seconds             INT          NOT NULL COMMENT 'ETL Runs every 60 seconds',
    incremental_mode_switch_cascaded TINYINT(1)   NOT NULL DEFAULT 0 COMMENT 'This is a computed Incremental Mode (1 or 0) for the ETL that is cascaded down to the implementer scripts',
    last_etl_schedule_insert_id      INT          NOT NULL DEFAULT 1 COMMENT 'Insert ID of the last ETL that ran'

) CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_user_settings_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_user_settings_insert;


~-~-
CREATE PROCEDURE sp_mamba_etl_user_settings_insert(
    IN openmrs_database VARCHAR(256) CHARACTER SET UTF8MB4,
    IN etl_database VARCHAR(256) CHARACTER SET UTF8MB4,
    IN concepts_locale CHAR(4) CHARACTER SET UTF8MB4,
    IN table_partition_number INT,
    IN incremental_mode_switch TINYINT(1),
    IN automatic_flattening_mode_switch TINYINT(1),
    IN etl_interval_seconds INT
)
BEGIN

    SET @insert_stmt = CONCAT(
            'INSERT INTO _mamba_etl_user_settings (`openmrs_database`, `etl_database`, `concepts_locale`, `table_partition_number`, `incremental_mode_switch`, `automatic_flattening_mode_switch`, `etl_interval_seconds`) VALUES (''',
            openmrs_database, ''', ''', etl_database, ''', ''', concepts_locale, ''', ', table_partition_number, ', ', incremental_mode_switch, ', ', automatic_flattening_mode_switch, ', ', etl_interval_seconds, ');');

    PREPARE inserttbl FROM @insert_stmt;
    EXECUTE inserttbl;
    DEALLOCATE PREPARE inserttbl;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_user_settings  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_user_settings;


~-~-
CREATE PROCEDURE sp_mamba_etl_user_settings(
    IN openmrs_database VARCHAR(256) CHARACTER SET UTF8MB4,
    IN etl_database VARCHAR(256) CHARACTER SET UTF8MB4,
    IN concepts_locale CHAR(4) CHARACTER SET UTF8MB4,
    IN table_partition_number INT,
    IN incremental_mode_switch TINYINT(1),
    IN automatic_flattening_mode_switch TINYINT(1),
    IN etl_interval_seconds INT
)
BEGIN

    -- DECLARE openmrs_db VARCHAR(256)  DEFAULT IFNULL(openmrs_database, 'openmrs');

    CALL sp_mamba_etl_user_settings_drop();
    CALL sp_mamba_etl_user_settings_create();
    CALL sp_mamba_etl_user_settings_insert(openmrs_database,
                                           etl_database,
                                           concepts_locale,
                                           table_partition_number,
                                           incremental_mode_switch,
                                           automatic_flattening_mode_switch,
                                           etl_interval_seconds);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_all_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_all_create;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_all_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_incremental_columns_index_all_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_incremental_columns_index_all_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- This table will be used to index the columns that are used to determine if a record is new, changed, retired or voided
-- It will be used to speed up the incremental updates for each incremental Table indentified in the ETL process

CREATE TABLE IF NOT EXISTS mamba_etl_incremental_columns_index_all
(
    incremental_table_pkey INT        NOT NULL UNIQUE PRIMARY KEY,

    date_created           DATETIME   NOT NULL,
    date_changed           DATETIME   NULL,
    date_retired           DATETIME   NULL,
    date_voided            DATETIME   NULL,

    retired                TINYINT(1) NULL,
    voided                 TINYINT(1) NULL,

    INDEX mamba_idx_date_created (date_created),
    INDEX mamba_idx_date_changed (date_changed),
    INDEX mamba_idx_date_retired (date_retired),
    INDEX mamba_idx_date_voided (date_voided),
    INDEX mamba_idx_retired (retired),
    INDEX mamba_idx_voided (voided)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_all_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_all_insert;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_all_insert(
    IN openmrs_table VARCHAR(255)
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE incremental_column_name VARCHAR(255);
    DECLARE column_list VARCHAR(500) DEFAULT 'incremental_table_pkey, ';
    DECLARE select_list VARCHAR(500) DEFAULT '';
    DECLARE pkey_column VARCHAR(255);

    DECLARE column_cursor CURSOR FOR
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'mamba_etl_incremental_columns_index_all';

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Identify the primary key of the target table
    SELECT COLUMN_NAME
    INTO pkey_column
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mamba_source_db'
      AND TABLE_NAME = openmrs_table
      AND COLUMN_KEY = 'PRI'
    LIMIT 1;

    -- Add the primary key to the select list
    SET select_list = CONCAT(select_list, pkey_column, ', ');

    OPEN column_cursor;

    column_loop:
    LOOP
        FETCH column_cursor INTO incremental_column_name;
        IF done THEN
            LEAVE column_loop;
        END IF;

        -- Check if the column exists in openmrs_table
        IF EXISTS (SELECT 1
                   FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_SCHEMA = 'mamba_source_db'
                     AND TABLE_NAME = openmrs_table
                     AND COLUMN_NAME = incremental_column_name) THEN
            SET column_list = CONCAT(column_list, incremental_column_name, ', ');
            SET select_list = CONCAT(select_list, incremental_column_name, ', ');
        END IF;
    END LOOP column_loop;

    CLOSE column_cursor;

    -- Remove the trailing comma and space
    SET column_list = LEFT(column_list, CHAR_LENGTH(column_list) - 2);
    SET select_list = LEFT(select_list, CHAR_LENGTH(select_list) - 2);

    SET @insert_sql = CONCAT(
            'INSERT INTO mamba_etl_incremental_columns_index_all (', column_list, ') ',
            'SELECT ', select_list, ' FROM mamba_source_db.', openmrs_table
                      );

    PREPARE stmt FROM @insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_all_truncate  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_all_truncate;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_all_truncate()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_incremental_columns_index_all_truncate', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_incremental_columns_index_all_truncate', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

TRUNCATE TABLE mamba_etl_incremental_columns_index_all;
-- CALL sp_mamba_truncate_table('mamba_etl_incremental_columns_index_all');

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_all;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_all(
    IN target_table_name VARCHAR(255)
)
BEGIN

    CALL sp_mamba_etl_incremental_columns_index_all_create();
    CALL sp_mamba_etl_incremental_columns_index_all_truncate();
    CALL sp_mamba_etl_incremental_columns_index_all_insert(target_table_name);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_new_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_new_create;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_new_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_incremental_columns_index_new_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_incremental_columns_index_new_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- This Table will only contain Primary keys for only those records that are NEW (i.e. Newly Inserted)

CREATE TEMPORARY TABLE IF NOT EXISTS mamba_etl_incremental_columns_index_new
(
    incremental_table_pkey INT NOT NULL UNIQUE PRIMARY KEY
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_new_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_new_insert;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_new_insert(
    IN mamba_table_name VARCHAR(255)
)
BEGIN
    DECLARE incremental_start_time DATETIME;
    DECLARE pkey_column VARCHAR(255);

    -- Identify the primary key of the 'mamba_table_name'
    SELECT COLUMN_NAME
    INTO pkey_column
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = mamba_table_name
      AND COLUMN_KEY = 'PRI'
    LIMIT 1;

    SET incremental_start_time = (SELECT start_time
                                  FROM _mamba_etl_schedule sch
                                  WHERE end_time IS NOT NULL
                                    AND transaction_status = 'COMPLETED'
                                  ORDER BY id DESC
                                  LIMIT 1);

    -- Insert only records that are NOT in the mamba ETL table
    -- and were created after the last ETL run time (start_time)
    SET @insert_sql = CONCAT(
            'INSERT INTO mamba_etl_incremental_columns_index_new (incremental_table_pkey) ',
            'SELECT DISTINCT incremental_table_pkey ',
            'FROM mamba_etl_incremental_columns_index_all ',
            'WHERE date_created >= ?',
            ' AND incremental_table_pkey NOT IN (SELECT DISTINCT (', pkey_column, ') FROM ', mamba_table_name, ')');

    PREPARE stmt FROM @insert_sql;
    SET @inc_start_time = incremental_start_time;
    EXECUTE stmt USING @inc_start_time;
    DEALLOCATE PREPARE stmt;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_new_truncate  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_new_truncate;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_new_truncate()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_incremental_columns_index_new_truncate', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_incremental_columns_index_new_truncate', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

TRUNCATE TABLE mamba_etl_incremental_columns_index_new;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_new  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_new;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_new(
    IN mamba_table_name VARCHAR(255)
)
BEGIN

    CALL sp_mamba_etl_incremental_columns_index_new_create();
    CALL sp_mamba_etl_incremental_columns_index_new_truncate();
    CALL sp_mamba_etl_incremental_columns_index_new_insert(mamba_table_name);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_modified_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_modified_create;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_modified_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_incremental_columns_index_modified_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_incremental_columns_index_modified_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- This Table will only contain Primary keys for only those records that have been modified/updated (i.e. Retired, Voided, Changed)

CREATE TEMPORARY TABLE IF NOT EXISTS mamba_etl_incremental_columns_index_modified
(
    incremental_table_pkey INT NOT NULL UNIQUE PRIMARY KEY
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_modified_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_modified_insert;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_modified_insert(
    IN mamba_table_name VARCHAR(255)
)
BEGIN
    DECLARE incremental_start_time DATETIME;
    DECLARE pkey_column VARCHAR(255);

    -- Identify the primary key of the 'mamba_table_name'
    SELECT COLUMN_NAME
    INTO pkey_column
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = mamba_table_name
      AND COLUMN_KEY = 'PRI'
    LIMIT 1;

    SET incremental_start_time = (SELECT start_time
                                  FROM _mamba_etl_schedule sch
                                  WHERE end_time IS NOT NULL
                                    AND transaction_status = 'COMPLETED'
                                  ORDER BY id DESC
                                  LIMIT 1);

    -- Insert only records that are NOT in the mamba ETL table
    -- and were created after the last ETL run time (start_time)
    SET @insert_sql = CONCAT(
            'INSERT INTO mamba_etl_incremental_columns_index_modified (incremental_table_pkey) ',
            'SELECT DISTINCT incremental_table_pkey ',
            'FROM mamba_etl_incremental_columns_index_all ',
            'WHERE date_changed >= ?',
            ' OR (voided = 1 AND date_voided >= ?)',
            ' OR (retired = 1 AND date_retired >= ?)');

    PREPARE stmt FROM @insert_sql;
    SET @incremental_start_time = incremental_start_time;
    EXECUTE stmt USING @incremental_start_time, @incremental_start_time, @incremental_start_time;
    DEALLOCATE PREPARE stmt;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_modified_truncate  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_modified_truncate;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_modified_truncate()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_incremental_columns_index_modified_truncate', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_incremental_columns_index_modified_truncate', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

TRUNCATE TABLE mamba_etl_incremental_columns_index_modified;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_modified  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_modified;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_modified(
    IN mamba_table_name VARCHAR(255)
)
BEGIN

    CALL sp_mamba_etl_incremental_columns_index_modified_create();
    CALL sp_mamba_etl_incremental_columns_index_modified_truncate();
    CALL sp_mamba_etl_incremental_columns_index_modified_insert(mamba_table_name);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index(
    IN openmrs_table_name VARCHAR(255),
    IN mamba_table_name VARCHAR(255)
)
BEGIN

    CALL sp_mamba_etl_incremental_columns_index_all(openmrs_table_name);
    CALL sp_mamba_etl_incremental_columns_index_new(mamba_table_name);
    CALL sp_mamba_etl_incremental_columns_index_modified(mamba_table_name);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_table_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_table_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_table_insert(
    IN openmrs_table VARCHAR(255),
    IN mamba_table VARCHAR(255),
    IN is_incremental BOOLEAN
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE tbl_column_name VARCHAR(255);
    DECLARE column_list VARCHAR(500) DEFAULT '';
    DECLARE select_list VARCHAR(500) DEFAULT '';
    DECLARE pkey_column VARCHAR(255);
    DECLARE join_clause VARCHAR(500) DEFAULT '';

    DECLARE column_cursor CURSOR FOR
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = mamba_table;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Identify the primary key of the mamba_source_db table
    SELECT COLUMN_NAME
    INTO pkey_column
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mamba_source_db'
      AND TABLE_NAME = openmrs_table
      AND COLUMN_KEY = 'PRI'
    LIMIT 1;

    SET column_list = CONCAT(column_list, 'incremental_record', ', ');
    IF is_incremental THEN
        SET select_list = CONCAT(select_list, 1, ', ');
    ELSE
        SET select_list = CONCAT(select_list, 0, ', ');
    END IF;

    OPEN column_cursor;

    column_loop:
    LOOP
        FETCH column_cursor INTO tbl_column_name;
        IF done THEN
            LEAVE column_loop;
        END IF;

        -- Check if the column exists in openmrs_table
        IF EXISTS (SELECT 1
                   FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_SCHEMA = 'mamba_source_db'
                     AND TABLE_NAME = openmrs_table
                     AND COLUMN_NAME = tbl_column_name) THEN
            SET column_list = CONCAT(column_list, tbl_column_name, ', ');
            SET select_list = CONCAT(select_list, tbl_column_name, ', ');
        END IF;
    END LOOP column_loop;

    CLOSE column_cursor;

    -- Remove the trailing comma and space
    SET column_list = LEFT(column_list, CHAR_LENGTH(column_list) - 2);
    SET select_list = LEFT(select_list, CHAR_LENGTH(select_list) - 2);

    -- Set the join clause if it is an incremental insert
    IF is_incremental THEN
        SET join_clause = CONCAT(
                ' INNER JOIN mamba_etl_incremental_columns_index_new ic',
                ' ON tb.', pkey_column, ' = ic.incremental_table_pkey');
    END IF;

    SET @insert_sql = CONCAT(
            'INSERT INTO ', mamba_table, ' (', column_list, ') ',
            'SELECT ', select_list,
            ' FROM mamba_source_db.', openmrs_table, ' tb',
            join_clause, ';');

    PREPARE stmt FROM @insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_truncate_table  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_truncate_table;


~-~-
CREATE PROCEDURE sp_mamba_truncate_table(
    IN table_to_truncate VARCHAR(64) CHARACTER SET UTF8MB4
)
BEGIN
    IF EXISTS (SELECT 1
               FROM information_schema.tables
               WHERE table_schema = DATABASE()
                 AND table_name = table_to_truncate) THEN

        SET @sql = CONCAT('TRUNCATE TABLE ', table_to_truncate);
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_drop_table  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_drop_table;


~-~-
CREATE PROCEDURE sp_mamba_drop_table(
    IN table_to_drop VARCHAR(64) CHARACTER SET UTF8MB4
)
BEGIN

    SET @sql = CONCAT('DROP TABLE IF EXISTS ', table_to_drop);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_location_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_location_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_location_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_location_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_location
(
    location_id        INT           NOT NULL UNIQUE PRIMARY KEY,
    name               VARCHAR(255)  NOT NULL,
    description        VARCHAR(255)  NULL,
    city_village       VARCHAR(255)  NULL,
    state_province     VARCHAR(255)  NULL,
    postal_code        VARCHAR(50)   NULL,
    country            VARCHAR(50)   NULL,
    latitude           VARCHAR(50)   NULL,
    longitude          VARCHAR(50)   NULL,
    county_district    VARCHAR(255)  NULL,
    address1           VARCHAR(255)  NULL,
    address2           VARCHAR(255)  NULL,
    address3           VARCHAR(255)  NULL,
    address4           VARCHAR(255)  NULL,
    address5           VARCHAR(255)  NULL,
    address6           VARCHAR(255)  NULL,
    address7           VARCHAR(255)  NULL,
    address8           VARCHAR(255)  NULL,
    address9           VARCHAR(255)  NULL,
    address10          VARCHAR(255)  NULL,
    address11          VARCHAR(255)  NULL,
    address12          VARCHAR(255)  NULL,
    address13          VARCHAR(255)  NULL,
    address14          VARCHAR(255)  NULL,
    address15          VARCHAR(255)  NULL,
    date_created       DATETIME      NOT NULL,
    date_changed       DATETIME      NULL,
    date_retired       DATETIME      NULL,
    retired            TINYINT(1)    NULL,
    retire_reason      VARCHAR(255)  NULL,
    retired_by         INT           NULL,
    changed_by         INT           NULL,
    incremental_record INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_name (name),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_location_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_location_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_location_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_location_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('location', 'mamba_dim_location', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_location_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_location_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_location_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_location_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_location;


~-~-
CREATE PROCEDURE sp_mamba_dim_location()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_location', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_location', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_location_create();
CALL sp_mamba_dim_location_insert();
CALL sp_mamba_dim_location_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_location_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_location_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_location_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_location_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('location', 'mamba_dim_location');
CALL sp_mamba_dim_location_incremental_insert();
CALL sp_mamba_dim_location_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_location_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_location_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_location_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_location_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_mamba_dim_table_insert('location', 'mamba_dim_location', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_location_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_location_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_location_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_location_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE mamba_dim_location mdl
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON mdl.location_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.location l
    ON mdl.location_id = l.location_id
SET mdl.name               = l.name,
    mdl.description        = l.description,
    mdl.city_village       = l.city_village,
    mdl.state_province     = l.state_province,
    mdl.postal_code        = l.postal_code,
    mdl.country            = l.country,
    mdl.latitude           = l.latitude,
    mdl.longitude          = l.longitude,
    mdl.county_district    = l.county_district,
    mdl.address1           = l.address1,
    mdl.address2           = l.address2,
    mdl.address3           = l.address3,
    mdl.address4           = l.address4,
    mdl.address5           = l.address5,
    mdl.address6           = l.address6,
    mdl.address7           = l.address7,
    mdl.address8           = l.address8,
    mdl.address9           = l.address9,
    mdl.address10          = l.address10,
    mdl.address11          = l.address11,
    mdl.address12          = l.address12,
    mdl.address13          = l.address13,
    mdl.address14          = l.address14,
    mdl.address15          = l.address15,
    mdl.date_created       = l.date_created,
    mdl.changed_by         = l.changed_by,
    mdl.date_changed       = l.date_changed,
    mdl.retired            = l.retired,
    mdl.retired_by         = l.retired_by,
    mdl.date_retired       = l.date_retired,
    mdl.retire_reason      = l.retire_reason,
    mdl.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_type_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_type_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_patient_identifier_type
(
    patient_identifier_type_id INT           NOT NULL UNIQUE PRIMARY KEY,
    name                       VARCHAR(50)   NOT NULL,
    description                TEXT          NULL,
    uuid                       CHAR(38)      NOT NULL,
    date_created               DATETIME      NOT NULL,
    date_changed               DATETIME      NULL,
    date_retired               DATETIME      NULL,
    retired                    TINYINT(1)    NULL,
    retire_reason              VARCHAR(255)  NULL,
    retired_by                 INT           NULL,
    changed_by                 INT           NULL,
    incremental_record         INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_name (name),
    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_type_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_type_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('patient_identifier_type', 'mamba_dim_patient_identifier_type', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_type_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_type_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_type', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_type', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_patient_identifier_type_create();
CALL sp_mamba_dim_patient_identifier_type_insert();
CALL sp_mamba_dim_patient_identifier_type_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_type_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_type_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('patient_identifier_type', 'mamba_dim_patient_identifier_type');
CALL sp_mamba_dim_patient_identifier_type_incremental_insert();
CALL sp_mamba_dim_patient_identifier_type_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_type_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_type_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_mamba_dim_table_insert('patient_identifier_type', 'mamba_dim_patient_identifier_type', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_type_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_type_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE mamba_dim_patient_identifier_type mdpit
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON mdpit.patient_identifier_type_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.patient_identifier_type pit
    ON mdpit.patient_identifier_type_id = pit.patient_identifier_type_id
SET mdpit.name               = pit.name,
    mdpit.description        = pit.description,
    mdpit.uuid               = pit.uuid,
    mdpit.date_created       = pit.date_created,
    mdpit.date_changed       = pit.date_changed,
    mdpit.date_retired       = pit.date_retired,
    mdpit.retired            = pit.retired,
    mdpit.retire_reason      = pit.retire_reason,
    mdpit.retired_by         = pit.retired_by,
    mdpit.changed_by         = pit.changed_by,
    mdpit.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_datatype_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_datatype_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_datatype_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_concept_datatype
(
    concept_datatype_id INT           NOT NULL UNIQUE PRIMARY KEY,
    name                VARCHAR(255)  NOT NULL,
    hl7_abbreviation    VARCHAR(3)    NULL,
    description         VARCHAR(255)  NULL,
    date_created        DATETIME      NOT NULL,
    date_retired        DATETIME      NULL,
    retired             TINYINT(1)    NULL,
    retire_reason       VARCHAR(255)  NULL,
    retired_by          INT           NULL,
    incremental_record  INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_name (name),
    INDEX mamba_idx_retired (retired),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_datatype_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_datatype_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_datatype_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('concept_datatype', 'mamba_dim_concept_datatype', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_datatype()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_datatype', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_datatype', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_concept_datatype_create();
CALL sp_mamba_dim_concept_datatype_insert();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_datatype_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_datatype_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_datatype_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_mamba_dim_table_insert('concept_datatype', 'mamba_dim_concept_datatype', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_datatype_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_datatype_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_datatype_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE mamba_dim_concept_datatype mcd
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON mcd.concept_datatype_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.concept_datatype cd
    ON mcd.concept_datatype_id = cd.concept_datatype_id
SET mcd.name               = cd.name,
    mcd.hl7_abbreviation   = cd.hl7_abbreviation,
    mcd.description        = cd.description,
    mcd.date_created       = cd.date_created,
    mcd.date_retired       = cd.date_retired,
    mcd.retired            = cd.retired,
    mcd.retired_by         = cd.retired_by,
    mcd.retire_reason      = cd.retire_reason,
    mcd.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_datatype_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_datatype_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_datatype_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('concept_datatype', 'mamba_dim_concept_datatype');
CALL sp_mamba_dim_concept_datatype_incremental_insert();
CALL sp_mamba_dim_concept_datatype_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_concept
(
    concept_id             INT           NOT NULL UNIQUE PRIMARY KEY,
    uuid                   CHAR(38)      NOT NULL,
    datatype_id            INT           NOT NULL, -- make it a FK
    datatype               VARCHAR(100)  NULL,
    name                   VARCHAR(256)  NULL,
    auto_table_column_name VARCHAR(60)   NULL,
    date_created           DATETIME      NOT NULL,
    date_changed           DATETIME      NULL,
    date_retired           DATETIME      NULL,
    retired                TINYINT(1)    NULL,
    retire_reason          VARCHAR(255)  NULL,
    retired_by             INT           NULL,
    changed_by             INT           NULL,
    incremental_record     INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_datatype_id (datatype_id),
    INDEX mamba_idx_retired (retired),
    INDEX mamba_idx_date_created (date_created),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('concept', 'mamba_dim_concept', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update the Data Type
UPDATE mamba_dim_concept c
    INNER JOIN mamba_dim_concept_datatype dt
    ON c.datatype_id = dt.concept_datatype_id
SET c.datatype = dt.name
WHERE c.concept_id > 0;

CREATE TEMPORARY TABLE mamba_temp_computed_concept_name
(
    concept_id      INT          NOT NULL,
    computed_name   VARCHAR(255) NOT NULL,
    tbl_column_name VARCHAR(60)  NOT NULL,
    INDEX mamba_idx_concept_id (concept_id)
)
    CHARSET = UTF8MB4
SELECT c.concept_id,
       CASE
           WHEN TRIM(cn.name) IS NULL OR TRIM(cn.name) = '' THEN CONCAT('UNKNOWN_CONCEPT_NAME', '_', c.concept_id)
           WHEN c.retired = 1 THEN CONCAT(TRIM(cn.name), '_', 'RETIRED')
           ELSE TRIM(cn.name)
           END                                                         AS computed_name,
       TRIM(LOWER(LEFT(REPLACE(REPLACE(fn_mamba_remove_special_characters(
                                               CASE
                                                   WHEN TRIM(cn.name) IS NULL OR TRIM(cn.name) = ''
                                                       THEN CONCAT('UNKNOWN_CONCEPT_NAME', '_', c.concept_id)
                                                   WHEN c.retired = 1 THEN CONCAT(TRIM(cn.name), '_', 'RETIRED')
                                                   ELSE TRIM(cn.name)
                                                   END
                                       ), ' ', '_'), '__', '_'), 60))) AS tbl_column_name
FROM mamba_dim_concept c
         LEFT JOIN mamba_dim_concept_name cn ON c.concept_id = cn.concept_id;

UPDATE mamba_dim_concept c
    INNER JOIN mamba_temp_computed_concept_name tc
    ON c.concept_id = tc.concept_id
SET c.name                   = tc.computed_name,
    c.auto_table_column_name = IF(tc.tbl_column_name = '',
                                  CONCAT('UNKNOWN_CONCEPT_NAME', '_', c.concept_id),
                                  tc.tbl_column_name)
WHERE c.concept_id > 0;

DROP TEMPORARY TABLE IF EXISTS mamba_temp_computed_concept_name;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_cleanup;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_cleanup()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE current_id INT;
    DECLARE current_auto_table_column_name VARCHAR(60);
    DECLARE previous_auto_table_column_name VARCHAR(60) DEFAULT '';
    DECLARE counter INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT concept_id, auto_table_column_name
        FROM mamba_dim_concept
        ORDER BY auto_table_column_name;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    CREATE TEMPORARY TABLE IF NOT EXISTS mamba_dim_concept_temp
    (
        concept_id             INT,
        auto_table_column_name VARCHAR(60)
    )
        CHARSET = UTF8MB4;

    TRUNCATE TABLE mamba_dim_concept_temp;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO current_id, current_auto_table_column_name;

        IF done THEN
            LEAVE read_loop;
        END IF;

        IF current_auto_table_column_name IS NULL THEN
            SET current_auto_table_column_name = '';
        END IF;

        IF current_auto_table_column_name = previous_auto_table_column_name THEN

            SET counter = counter + 1;
            SET current_auto_table_column_name = CONCAT(
                    IF(CHAR_LENGTH(previous_auto_table_column_name) <= 57,
                       previous_auto_table_column_name,
                       LEFT(previous_auto_table_column_name, CHAR_LENGTH(previous_auto_table_column_name) - 3)
                    ),
                    '_',
                    counter);
        ELSE
            SET counter = 0;
            SET previous_auto_table_column_name = current_auto_table_column_name;
        END IF;

        INSERT INTO mamba_dim_concept_temp (concept_id, auto_table_column_name)
        VALUES (current_id, current_auto_table_column_name);

    END LOOP;

    CLOSE cur;

    UPDATE mamba_dim_concept c
        JOIN mamba_dim_concept_temp t
        ON c.concept_id = t.concept_id
    SET c.auto_table_column_name = t.auto_table_column_name
    WHERE c.concept_id > 0;

    DROP TEMPORARY TABLE IF EXISTS mamba_dim_concept_temp;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_concept_create();
CALL sp_mamba_dim_concept_insert();
CALL sp_mamba_dim_concept_update();
CALL sp_mamba_dim_concept_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_mamba_dim_table_insert('concept', 'mamba_dim_concept', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE mamba_dim_concept tc
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON tc.concept_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.concept sc
    ON tc.concept_id = sc.concept_id
SET tc.uuid               = sc.uuid,
    tc.datatype_id        = sc.datatype_id,
    tc.date_created       = sc.date_created,
    tc.date_changed       = sc.date_changed,
    tc.date_retired       = sc.date_retired,
    tc.changed_by         = sc.changed_by,
    tc.retired            = sc.retired,
    tc.retired_by         = sc.retired_by,
    tc.retire_reason      = sc.retire_reason,
    tc.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- Update the Data Type
UPDATE mamba_dim_concept c
    INNER JOIN mamba_dim_concept_datatype dt
    ON c.datatype_id = dt.concept_datatype_id
SET c.datatype = dt.name
WHERE c.incremental_record = 1;

-- Update the concept name and table column name
CREATE TEMPORARY TABLE mamba_temp_computed_concept_name
(
    concept_id      INT          NOT NULL,
    computed_name   VARCHAR(255) NOT NULL,
    tbl_column_name VARCHAR(60)  NOT NULL,
    INDEX mamba_idx_concept_id (concept_id)
)CHARSET = UTF8MB4 AS
SELECT c.concept_id,
       CASE
           WHEN TRIM(cn.name) IS NULL OR TRIM(cn.name) = '' THEN CONCAT('UNKNOWN_CONCEPT_NAME', '_', c.concept_id)
           WHEN c.retired = 1 THEN CONCAT(TRIM(cn.name), '_', 'RETIRED')
           ELSE TRIM(cn.name)
           END                                                         AS computed_name,
       TRIM(LOWER(LEFT(REPLACE(REPLACE(fn_mamba_remove_special_characters(
                                               CASE
                                                   WHEN TRIM(cn.name) IS NULL OR TRIM(cn.name) = ''
                                                       THEN CONCAT('UNKNOWN_CONCEPT_NAME', '_', c.concept_id)
                                                   WHEN c.retired = 1 THEN CONCAT(TRIM(cn.name), '_', 'RETIRED')
                                                   ELSE TRIM(cn.name)
                                                   END
                                       ), ' ', '_'), '__', '_'), 60))) AS tbl_column_name
FROM mamba_dim_concept c
         LEFT JOIN mamba_dim_concept_name cn ON c.concept_id = cn.concept_id;

UPDATE mamba_dim_concept c
    INNER JOIN mamba_temp_computed_concept_name tc
    ON c.concept_id = tc.concept_id
SET c.name                   = tc.computed_name,
    c.auto_table_column_name = IF(tc.tbl_column_name = '',
                                  CONCAT('UNKNOWN_CONCEPT_NAME', '_', c.concept_id),
                                  tc.tbl_column_name)
WHERE c.incremental_record = 1;

DROP TEMPORARY TABLE IF EXISTS mamba_temp_computed_concept_name;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_incremental_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_incremental_cleanup;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_incremental_cleanup()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE current_id INT;
    DECLARE current_auto_table_column_name VARCHAR(60);
    DECLARE previous_auto_table_column_name VARCHAR(60) DEFAULT '';
    DECLARE counter INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT concept_id, auto_table_column_name
        FROM mamba_dim_concept
        WHERE incremental_record = 1
        ORDER BY auto_table_column_name;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    CREATE TEMPORARY TABLE IF NOT EXISTS mamba_dim_concept_temp
    (
        concept_id             INT,
        auto_table_column_name VARCHAR(60)

    ) CHARSET = UTF8MB4;

    TRUNCATE TABLE mamba_dim_concept_temp;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO current_id, current_auto_table_column_name;

        IF done THEN
            LEAVE read_loop;
        END IF;

        IF current_auto_table_column_name IS NULL THEN
            SET current_auto_table_column_name = '';
        END IF;

        IF current_auto_table_column_name = previous_auto_table_column_name THEN

            SET counter = counter + 1;
            SET current_auto_table_column_name = CONCAT(
                    IF(CHAR_LENGTH(previous_auto_table_column_name) <= 57,
                       previous_auto_table_column_name,
                       LEFT(previous_auto_table_column_name, CHAR_LENGTH(previous_auto_table_column_name) - 3)
                    ),
                    '_',
                    counter);
        ELSE
            SET counter = 0;
            SET previous_auto_table_column_name = current_auto_table_column_name;
        END IF;

        INSERT INTO mamba_dim_concept_temp (concept_id, auto_table_column_name)
        VALUES (current_id, current_auto_table_column_name);

    END LOOP;

    CLOSE cur;

    UPDATE mamba_dim_concept c
        JOIN mamba_dim_concept_temp t
        ON c.concept_id = t.concept_id
    SET c.auto_table_column_name = t.auto_table_column_name
    WHERE incremental_record = 1;

    DROP TEMPORARY TABLE mamba_dim_concept_temp;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('concept', 'mamba_dim_concept');
CALL sp_mamba_dim_concept_incremental_insert();
CALL sp_mamba_dim_concept_incremental_update();
CALL sp_mamba_dim_concept_incremental_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_answer_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_answer_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_answer_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_concept_answer
(
    concept_answer_id  INT           NOT NULL UNIQUE PRIMARY KEY,
    concept_id         INT           NOT NULL,
    answer_concept     INT,
    answer_drug        INT,
    incremental_record INT DEFAULT 0 NOT NULL,

    INDEX mamba_idx_concept_answer (concept_answer_id),
    INDEX mamba_idx_concept_id (concept_id),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_answer_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_answer_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_answer_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('concept_answer', 'mamba_dim_concept_answer', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_answer()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_answer', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_answer', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_concept_answer_create();
CALL sp_mamba_dim_concept_answer_insert();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_answer_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_answer_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_answer_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new records
CALL sp_mamba_dim_table_insert('concept_answer', 'mamba_dim_concept_answer', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_answer_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_answer_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_answer_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_answer_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_answer_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_answer_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('concept_answer', 'mamba_dim_concept_answer');
CALL sp_mamba_dim_concept_answer_incremental_insert();
CALL sp_mamba_dim_concept_answer_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_name_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_name_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_name_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_concept_name
(
    concept_name_id    INT           NOT NULL UNIQUE PRIMARY KEY,
    concept_id         INT,
    name               VARCHAR(255)  NOT NULL,
    locale             VARCHAR(50)   NOT NULL,
    locale_preferred   TINYINT,
    concept_name_type  VARCHAR(255),
    voided             TINYINT,
    date_created       DATETIME      NOT NULL,
    date_changed       DATETIME      NULL,
    date_voided        DATETIME      NULL,
    changed_by         INT           NULL,
    voided_by          INT           NULL,
    void_reason        VARCHAR(255)  NULL,
    incremental_record INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_concept_id (concept_id),
    INDEX mamba_idx_concept_name_type (concept_name_type),
    INDEX mamba_idx_locale (locale),
    INDEX mamba_idx_locale_preferred (locale_preferred),
    INDEX mamba_idx_voided (voided),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_name_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_name_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_name_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

INSERT INTO mamba_dim_concept_name (concept_name_id,
                                    concept_id,
                                    name,
                                    locale,
                                    locale_preferred,
                                    voided,
                                    concept_name_type,
                                    date_created,
                                    date_changed,
                                    changed_by,
                                    voided_by,
                                    date_voided,
                                    void_reason)
SELECT cn.concept_name_id,
       cn.concept_id,
       cn.name,
       cn.locale,
       cn.locale_preferred,
       cn.voided,
       cn.concept_name_type,
       cn.date_created,
       cn.date_changed,
       cn.changed_by,
       cn.voided_by,
       cn.date_voided,
       cn.void_reason
FROM mamba_source_db.concept_name cn
WHERE cn.locale IN (SELECT DISTINCT(concepts_locale) FROM _mamba_etl_user_settings)
  AND IF(cn.locale_preferred = 1, cn.locale_preferred = 1, cn.concept_name_type = 'FULLY_SPECIFIED')
  AND cn.voided = 0;
-- Use locale preferred or Fully specified name

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_name_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_name_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_name_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_name()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_name', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_name', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_concept_name_create();
CALL sp_mamba_dim_concept_name_insert();
CALL sp_mamba_dim_concept_name_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_name_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_name_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_name_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
INSERT INTO mamba_dim_concept_name (concept_name_id,
                                    concept_id,
                                    name,
                                    locale,
                                    locale_preferred,
                                    voided,
                                    concept_name_type,
                                    date_created,
                                    date_changed,
                                    changed_by,
                                    voided_by,
                                    date_voided,
                                    void_reason,
                                    incremental_record)
SELECT cn.concept_name_id,
       cn.concept_id,
       cn.name,
       cn.locale,
       cn.locale_preferred,
       cn.voided,
       cn.concept_name_type,
       cn.date_created,
       cn.date_changed,
       cn.changed_by,
       cn.voided_by,
       cn.date_voided,
       cn.void_reason,
       1
FROM mamba_source_db.concept_name cn
         INNER JOIN mamba_etl_incremental_columns_index_new ic
                    ON cn.concept_name_id = ic.incremental_table_pkey
WHERE cn.locale IN (SELECT DISTINCT (concepts_locale) FROM _mamba_etl_user_settings)
  AND cn.locale_preferred = 1
  AND cn.voided = 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_name_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_name_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_name_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- Update only Modified Records
UPDATE mamba_dim_concept_name cn
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON cn.concept_name_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.concept_name cnm
    ON cn.concept_name_id = cnm.concept_name_id
SET cn.concept_id         = cnm.concept_id,
    cn.name               = cnm.name,
    cn.locale             = cnm.locale,
    cn.locale_preferred   = cnm.locale_preferred,
    cn.concept_name_type  = cnm.concept_name_type,
    cn.voided             = cnm.voided,
    cn.date_created       = cnm.date_created,
    cn.date_changed       = cnm.date_changed,
    cn.changed_by         = cnm.changed_by,
    cn.voided_by          = cnm.voided_by,
    cn.date_voided        = cnm.date_voided,
    cn.void_reason        = cnm.void_reason,
    cn.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_incremental_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_incremental_cleanup;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_name_incremental_cleanup()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_name_incremental_cleanup', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_name_incremental_cleanup', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- Delete any concept names that have become voided or not locale_preferred or not our locale we set (so we are consistent with the original INSERT statement)
-- We only need to keep the non-voided, locale we set & locale_preferred concept names
-- This is because when concept names are modified, the old name is voided and a new name is created but both have a date_changed value of the same date (donno why)

DELETE
FROM mamba_dim_concept_name
WHERE voided <> 0
   OR locale_preferred <> 1
   OR locale NOT IN (SELECT DISTINCT(concepts_locale) FROM _mamba_etl_user_settings);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_name_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_name_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_name_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('concept_name', 'mamba_dim_concept_name');
CALL sp_mamba_dim_concept_name_incremental_insert();
CALL sp_mamba_dim_concept_name_incremental_update();
CALL sp_mamba_dim_concept_name_incremental_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_type_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_type_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_encounter_type
(
    encounter_type_id    INT           NOT NULL UNIQUE PRIMARY KEY,
    uuid                 CHAR(38)      NOT NULL,
    name                 VARCHAR(50)   NOT NULL,
    auto_flat_table_name VARCHAR(60)   NULL,
    description          TEXT          NULL,
    retired              TINYINT(1)    NULL,
    date_created         DATETIME      NULL,
    date_changed         DATETIME      NULL,
    changed_by           INT           NULL,
    date_retired         DATETIME      NULL,
    retired_by           INT           NULL,
    retire_reason        VARCHAR(255)  NULL,
    incremental_record   INT DEFAULT 0 NOT NULL,

    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_retired (retired),
    INDEX mamba_idx_name (name),
    INDEX mamba_idx_auto_flat_table_name (auto_flat_table_name),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_type_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_type_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('encounter_type', 'mamba_dim_encounter_type', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_type_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_type_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

UPDATE mamba_dim_encounter_type et
SET et.auto_flat_table_name = LOWER(LEFT(
        REPLACE(REPLACE(fn_mamba_remove_special_characters(CONCAT('mamba_flat_encounter_', et.name)), ' ', '_'), '__',
                '_'), 60))
WHERE et.encounter_type_id > 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_cleanup;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type_cleanup()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE current_id INT;
    DECLARE current_auto_flat_table_name VARCHAR(60);
    DECLARE previous_auto_flat_table_name VARCHAR(60) DEFAULT '';
    DECLARE counter INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT encounter_type_id, auto_flat_table_name
        FROM mamba_dim_encounter_type
        ORDER BY auto_flat_table_name;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    CREATE TEMPORARY TABLE IF NOT EXISTS mamba_dim_encounter_type_temp
    (
        encounter_type_id    INT,
        auto_flat_table_name VARCHAR(60)
    )
        CHARSET = UTF8MB4;

    TRUNCATE TABLE mamba_dim_encounter_type_temp;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO current_id, current_auto_flat_table_name;

        IF done THEN
            LEAVE read_loop;
        END IF;

        IF current_auto_flat_table_name IS NULL THEN
            SET current_auto_flat_table_name = '';
        END IF;

        IF current_auto_flat_table_name = previous_auto_flat_table_name THEN

            SET counter = counter + 1;
            SET current_auto_flat_table_name = CONCAT(
                    IF(CHAR_LENGTH(previous_auto_flat_table_name) <= 57,
                       previous_auto_flat_table_name,
                       LEFT(previous_auto_flat_table_name, CHAR_LENGTH(previous_auto_flat_table_name) - 3)
                    ),
                    '_',
                    counter);
        ELSE
            SET counter = 0;
            SET previous_auto_flat_table_name = current_auto_flat_table_name;
        END IF;

        INSERT INTO mamba_dim_encounter_type_temp (encounter_type_id, auto_flat_table_name)
        VALUES (current_id, current_auto_flat_table_name);

    END LOOP;

    CLOSE cur;

    UPDATE mamba_dim_encounter_type c
        JOIN mamba_dim_encounter_type_temp t
        ON c.encounter_type_id = t.encounter_type_id
    SET c.auto_flat_table_name = t.auto_flat_table_name
    WHERE c.encounter_type_id > 0;

    DROP TEMPORARY TABLE mamba_dim_encounter_type_temp;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_type', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_type', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_encounter_type_create();
CALL sp_mamba_dim_encounter_type_insert();
CALL sp_mamba_dim_encounter_type_update();
CALL sp_mamba_dim_encounter_type_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_type_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_type_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('encounter_type', 'mamba_dim_encounter_type', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_type_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_type_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Encounter types
UPDATE mamba_dim_encounter_type et
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON et.encounter_type_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.encounter_type ent
    ON et.encounter_type_id = ent.encounter_type_id
SET et.uuid               = ent.uuid,
    et.name               = ent.name,
    et.description        = ent.description,
    et.retired            = ent.retired,
    et.date_created       = ent.date_created,
    et.date_changed       = ent.date_changed,
    et.changed_by         = ent.changed_by,
    et.date_retired       = ent.date_retired,
    et.retired_by         = ent.retired_by,
    et.retire_reason      = ent.retire_reason,
    et.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

UPDATE mamba_dim_encounter_type et
SET et.auto_flat_table_name = LOWER(LEFT(
        REPLACE(REPLACE(fn_mamba_remove_special_characters(CONCAT('mamba_flat_encounter_', et.name)), ' ', '_'), '__',
                '_'), 60))
WHERE et.incremental_record = 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_incremental_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_incremental_cleanup;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type_incremental_cleanup()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE current_id INT;
    DECLARE current_auto_flat_table_name VARCHAR(60);
    DECLARE previous_auto_flat_table_name VARCHAR(60) DEFAULT '';
    DECLARE counter INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT encounter_type_id, auto_flat_table_name
        FROM mamba_dim_encounter_type
        WHERE incremental_record = 1
        ORDER BY auto_flat_table_name;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    CREATE TEMPORARY TABLE IF NOT EXISTS mamba_dim_encounter_type_temp
    (
        encounter_type_id    INT,
        auto_flat_table_name VARCHAR(60)
    )
        CHARSET = UTF8MB4;

    TRUNCATE TABLE mamba_dim_encounter_type_temp;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO current_id, current_auto_flat_table_name;

        IF done THEN
            LEAVE read_loop;
        END IF;

        IF current_auto_flat_table_name IS NULL THEN
            SET current_auto_flat_table_name = '';
        END IF;

        IF current_auto_flat_table_name = previous_auto_flat_table_name THEN

            SET counter = counter + 1;
            SET current_auto_flat_table_name = CONCAT(
                    IF(CHAR_LENGTH(previous_auto_flat_table_name) <= 57,
                       previous_auto_flat_table_name,
                       LEFT(previous_auto_flat_table_name, CHAR_LENGTH(previous_auto_flat_table_name) - 3)
                    ),
                    '_',
                    counter);
        ELSE
            SET counter = 0;
            SET previous_auto_flat_table_name = current_auto_flat_table_name;
        END IF;

        INSERT INTO mamba_dim_encounter_type_temp (encounter_type_id, auto_flat_table_name)
        VALUES (current_id, current_auto_flat_table_name);

    END LOOP;

    CLOSE cur;

    UPDATE mamba_dim_encounter_type et
        JOIN mamba_dim_encounter_type_temp t
        ON et.encounter_type_id = t.encounter_type_id
    SET et.auto_flat_table_name = t.auto_flat_table_name
    WHERE et.incremental_record = 1;

    DROP TEMPORARY TABLE mamba_dim_encounter_type_temp;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_type_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_type_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('encounter_type', 'mamba_dim_encounter_type');
CALL sp_mamba_dim_encounter_type_incremental_insert();
CALL sp_mamba_dim_encounter_type_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_encounter
(
    encounter_id        INT           NOT NULL UNIQUE PRIMARY KEY,
    uuid                CHAR(38)      NOT NULL,
    encounter_type      INT           NOT NULL,
    encounter_type_uuid CHAR(38)      NULL,
    patient_id          INT           NOT NULL,
    visit_id            INT           NULL,
    encounter_datetime  DATETIME      NOT NULL,
    date_created        DATETIME      NOT NULL,
    date_changed        DATETIME      NULL,
    changed_by          INT           NULL,
    date_voided         DATETIME      NULL,
    voided              TINYINT(1)    NOT NULL,
    voided_by           INT           NULL,
    void_reason         VARCHAR(255)  NULL,
    incremental_record  INT DEFAULT 0 NOT NULL,

    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_encounter_id (encounter_id),
    INDEX mamba_idx_encounter_type (encounter_type),
    INDEX mamba_idx_encounter_type_uuid (encounter_type_uuid),
    INDEX mamba_idx_patient_id (patient_id),
    INDEX mamba_idx_visit_id (visit_id),
    INDEX mamba_idx_encounter_datetime (encounter_datetime),
    INDEX mamba_idx_voided (voided),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

INSERT INTO mamba_dim_encounter (encounter_id,
                                 uuid,
                                 encounter_type,
                                 encounter_type_uuid,
                                 patient_id,
                                 visit_id,
                                 encounter_datetime,
                                 date_created,
                                 date_changed,
                                 changed_by,
                                 date_voided,
                                 voided,
                                 voided_by,
                                 void_reason)
SELECT e.encounter_id,
       e.uuid,
       e.encounter_type,
       et.uuid,
       e.patient_id,
       e.visit_id,
       e.encounter_datetime,
       e.date_created,
       e.date_changed,
       e.changed_by,
       e.date_voided,
       e.voided,
       e.voided_by,
       e.void_reason
FROM mamba_source_db.encounter e
         INNER JOIN mamba_dim_encounter_type et
                    ON e.encounter_type = et.encounter_type_id
WHERE et.uuid
          IN (SELECT DISTINCT(md.encounter_type_uuid)
              FROM mamba_concept_metadata md);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_encounter_create();
CALL sp_mamba_dim_encounter_insert();
CALL sp_mamba_dim_encounter_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new records
INSERT INTO mamba_dim_encounter (encounter_id,
                                 uuid,
                                 encounter_type,
                                 encounter_type_uuid,
                                 patient_id,
                                 visit_id,
                                 encounter_datetime,
                                 date_created,
                                 date_changed,
                                 changed_by,
                                 date_voided,
                                 voided,
                                 voided_by,
                                 void_reason,
                                 incremental_record)
SELECT e.encounter_id,
       e.uuid,
       e.encounter_type,
       et.uuid,
       e.patient_id,
       e.visit_id,
       e.encounter_datetime,
       e.date_created,
       e.date_changed,
       e.changed_by,
       e.date_voided,
       e.voided,
       e.voided_by,
       e.void_reason,
       1
FROM mamba_source_db.encounter e
         INNER JOIN mamba_etl_incremental_columns_index_new ic
                    ON e.encounter_id = ic.incremental_table_pkey
         INNER JOIN mamba_dim_encounter_type et
                    ON e.encounter_type = et.encounter_type_id;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Encounters
UPDATE mamba_dim_encounter e
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON e.encounter_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.encounter enc
    ON e.encounter_id = enc.encounter_id
    INNER JOIN mamba_dim_encounter_type et
    ON e.encounter_type = et.encounter_type_id
SET e.encounter_id        = enc.encounter_id,
    e.uuid                = enc.uuid,
    e.encounter_type      = enc.encounter_type,
    e.encounter_type_uuid = et.uuid,
    e.patient_id          = enc.patient_id,
    e.visit_id            = enc.visit_id,
    e.encounter_datetime  = enc.encounter_datetime,
    e.date_created        = enc.date_created,
    e.date_changed        = enc.date_changed,
    e.changed_by          = enc.changed_by,
    e.date_voided         = enc.date_voided,
    e.voided              = enc.voided,
    e.voided_by           = enc.voided_by,
    e.void_reason         = enc.void_reason,
    e.incremental_record  = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('encounter', 'mamba_dim_encounter');
CALL sp_mamba_dim_encounter_incremental_insert();
CALL sp_mamba_dim_encounter_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_report_definition_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_report_definition_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_report_definition_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_report_definition_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_report_definition_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_report_definition
(
    id                            INT          NOT NULL AUTO_INCREMENT,
    report_id                     VARCHAR(255) NOT NULL UNIQUE,
    report_procedure_name         VARCHAR(255) NOT NULL UNIQUE, -- should be derived from report_id??
    report_columns_procedure_name VARCHAR(255) NOT NULL UNIQUE,
    sql_query                     TEXT         NOT NULL,
    table_name                    VARCHAR(255) NOT NULL,        -- name of the table (will contain columns) of this query
    report_name                   VARCHAR(255) NULL,
    result_column_names           TEXT         NULL,            -- comma-separated column names

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_report_definition_report_id_index
    ON mamba_dim_report_definition (report_id);


CREATE TABLE mamba_dim_report_definition_parameters
(
    id                 INT          NOT NULL AUTO_INCREMENT,
    report_id          VARCHAR(255) NOT NULL,
    parameter_name     VARCHAR(255) NOT NULL,
    parameter_type     VARCHAR(30)  NOT NULL,
    parameter_position INT          NOT NULL, -- takes order or declaration in JSON file

    PRIMARY KEY (id),
    FOREIGN KEY (`report_id`) REFERENCES `mamba_dim_report_definition` (`report_id`)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_report_definition_parameter_position_index
    ON mamba_dim_report_definition_parameters (parameter_position);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_report_definition_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_report_definition_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_report_definition_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_report_definition_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_report_definition_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
SET @report_definition_json = '{
  "report_definitions": [
    {
      "report_name": "MCH Mother HIV Status",
      "report_id": "mother_hiv_status",
      "report_sql": {
        "sql_query": "SELECT pm.hiv_test_result AS hiv_test_result FROM mamba_flat_encounter_pmtct_anc pm INNER JOIN mamba_dim_person p ON pm.client_id = p.person_id WHERE p.uuid = person_uuid AND pm.ptracker_id = ptracker_id",
        "query_params": [
          {
            "name": "ptracker_id",
            "type": "VARCHAR(255)"
          },
          {
            "name": "person_uuid",
            "type": "VARCHAR(255)"
          }
        ]
      }
    },
    {
      "report_name": "MCH Total Deliveries",
      "report_id": "total_deliveries",
      "report_sql": {
        "sql_query": "SELECT COUNT(*) AS total_deliveries FROM mamba_dim_encounter e inner join mamba_dim_encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''6dc5308d-27c9-4d49-b16f-2c5e3c759757'' AND DATE(e.encounter_datetime) > CONCAT(YEAR(CURDATE()), ''-01-01 00:00:00'')",
        "query_params": []
      }
    },
    {
      "report_name": "MCH HIV-Exposed Infants",
      "report_id": "total_hiv_exposed_infants",
      "report_sql": {
        "sql_query": "SELECT COUNT(DISTINCT ei.infant_client_id) AS total_hiv_exposed_infants FROM mamba_fact_pmtct_exposedinfants ei INNER JOIN mamba_dim_person p ON ei.infant_client_id = p.person_id WHERE ei.encounter_datetime BETWEEN DATE_FORMAT(NOW(), ''%Y-01-01'') AND NOW() AND birthdate BETWEEN DATE_FORMAT(NOW(), ''%Y-01-01'') AND NOW()",
        "query_params": []
      }
    },
    {
      "report_name": "MCH Total Pregnant women",
      "report_id": "total_pregnant_women",
      "report_sql": {
        "sql_query": "SELECT COUNT(DISTINCT pw.client_id) AS total_pregnant_women FROM mamba_fact_pmtct_pregnant_women pw WHERE visit_type like ''New%'' AND encounter_datetime BETWEEN DATE_FORMAT(NOW(), ''%Y-01-01'') AND NOW() AND DATE_ADD(date_of_last_menstrual_period, INTERVAL 40 WEEK) > NOW()",
        "query_params": []
      }
    },
    {
      "report_name": "TB Total Active DR Cases ",
      "report_id": "total_active_dr_cases",
      "report_sql": {
        "sql_query": "SELECT COUNT(DISTINCT e.patient_id) AS total_active_dr FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id, max(value_datetime)date_enrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''9a199b59-b185-485b-b9b3-a9754e65ae57'' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid =''161552AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') group by e.patient_id ) ed ON ed.patient_id = e.patient_id LEFT JOIN ( SELECT DISTINCT e.patient_id, max(value_datetime)date_disenrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''9a199b59-b185-485b-b9b3-a9754e65ae57'' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid =''159431AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') group by e.patient_id ) dd ON dd.patient_id = e.patient_id WHERE et.uuid = ''9a199b59-b185-485b-b9b3-a9754e65ae57'' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid =''163775AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') AND o.value_coded = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid =''160052AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') AND ( dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled)",
        "query_params": []
      }
    },
    {
      "report_name": "TB Total Active DS Cases ",
      "report_id": "total_active_ds_cases",
      "report_sql": {
        "sql_query": "SELECT COUNT(DISTINCT e.patient_id) AS total_active_ds FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id, max(value_datetime)date_enrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''9a199b59-b185-485b-b9b3-a9754e65ae57'' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid =''161552AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') group by e.patient_id ) ed ON ed.patient_id = e.patient_id LEFT JOIN ( SELECT DISTINCT e.patient_id, max(value_datetime)date_disenrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''9a199b59-b185-485b-b9b3-a9754e65ae57'' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid =''159431AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') group by e.patient_id ) dd ON dd.patient_id = e.patient_id WHERE et.uuid = ''9a199b59-b185-485b-b9b3-a9754e65ae57'' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid =''163775AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') AND o.value_coded = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid =''160541AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') AND ( dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled)",
        "query_params": []
      }
    },
    {
      "report_name": "MNCH Mother Status",
      "report_id": "mother_status",
      "report_sql": {
        "sql_query": "SELECT cn.name as mother_status FROM mamba_source_db.obs o INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.concept_name cn on o.value_coded = cn.concept_id WHERE p.uuid = person_uuid AND c.uuid = ''1856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'' AND o.encounter_id = (SELECT e.encounter_id FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''6dc5308d-27c9-4d49-b16f-2c5e3c759757'' AND c.uuid = ''1856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'' AND p.uuid = person_uuid ORDER BY encounter_datetime desc  LIMIT 1) AND cn.voided = 0 AND cn.locale_preferred = 1",
        "query_params": [
          {
            "name": "person_uuid",
            "type": "VARCHAR(255)"
          }
        ]
      }
    },
    {
      "report_name": "MNCH Estimated Delivery Date",
      "report_id": "estimated_date_of_delivery",
      "report_sql": {
        "sql_query": "(SELECT CASE WHEN o.value_datetime >=curdate() THEN DATE_FORMAT(o.value_datetime, ''%d %m %Y'') ELSE '''' END AS estimated_date_of_delivery FROM mamba_source_db.obs o INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id  WHERE p.uuid = person_uuid AND c.uuid = ''5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'' AND o.encounter_id = (SELECT e.encounter_id FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''677d1a80-dbbe-4399-be34-aa7f54f11405'' AND c.uuid = ''5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'' AND p.uuid = person_uuid ORDER BY encounter_datetime desc  LIMIT 1) ORDER BY o.value_datetime DESC  LIMIT 1)",
        "query_params": [
          {
            "name": "person_uuid",
            "type": "VARCHAR(255)"
          }
        ]
      }
    },
    {
      "report_name": "MNCH Next Appointment",
      "report_id": "next_appointment_date",
      "report_sql": {
        "sql_query": "(SELECT DATE_FORMAT(o.value_datetime, ''%d %m %Y'') as next_appointment FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT e.encounter_id FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''677d1a80-dbbe-4399-be34-aa7f54f11405'' AND c.uuid = ''5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'' AND p.uuid = person_uuid) a ON o.encounter_id = a.encounter_id LEFT JOIN (SELECT e.encounter_id FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id  INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''4362fd2d-1866-4ea0-84ef-5e5da9627440'' AND c.uuid = ''5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'' AND p.uuid = person_uuid)b  on o.encounter_id = b.encounter_id WHERE et.uuid = ''677d1a80-dbbe-4399-be34-aa7f54f11405'' AND c.uuid = ''5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'' AND p.uuid = person_uuid ORDER BY  encounter_datetime DESC LIMIT 1)",
        "query_params": [
          {
            "name": "person_uuid",
            "type": "VARCHAR(255)"
          }
        ]
      }
    },
    {
      "report_name": "MNCH Number of ANC Visits",
      "report_id": "no_of_anc_visits",
      "report_sql": {
        "sql_query": "SELECT COUNT(DISTINCT e.encounter_id)no_of_anc_visits FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''677d1a80-dbbe-4399-be34-aa7f54f11405'' AND c.uuid = ''5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'' AND o.value_datetime >= curdate() AND p.uuid = person_uuid",
        "query_params": [
          {
            "name": "person_uuid",
            "type": "VARCHAR(255)"
          }
        ]
      }
    },
    {
      "report_name": "TPT Active clients",
      "report_id": "total_active_tpt",
      "report_sql": {
        "sql_query": "SELECT DISTINCT COUNT(DISTINCT e.patient_id) AS total_active_tpt FROM mamba_source_db.obs o  INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id,max(value_datetime) date_enrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id  WHERE et.uuid = ''dc6ce80c-83f8-4ace-a638-21df78542551'' AND o.concept_id =  (SELECT concept_id FROM mamba_source_db.concept WHERE uuid = ''162320AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') GROUP BY e.patient_id) ed ON ed.patient_id = e.patient_id LEFT JOIN(SELECT DISTINCT e.patient_id, max(value_datetime) date_disenrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''dc6ce80c-83f8-4ace-a638-21df78542551'' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid = ''163284AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') GROUP BY e.patient_id) dd ON dd.patient_id = e.patient_id WHERE et.uuid = ''dc6ce80c-83f8-4ace-a638-21df78542551'' AND (dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled)",
        "query_params": []
      }
    }
  ]
}';
CALL sp_mamba_extract_report_definition_metadata(@report_definition_json, 'mamba_dim_report_definition');
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_report_definition_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_report_definition_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_report_definition_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_report_definition_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_report_definition_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_report_definition  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_report_definition;


~-~-
CREATE PROCEDURE sp_mamba_dim_report_definition()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_report_definition', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_report_definition', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_report_definition_create();
CALL sp_mamba_dim_report_definition_insert();
CALL sp_mamba_dim_report_definition_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_person
(
    person_id           INT           NOT NULL UNIQUE PRIMARY KEY,
    birthdate           DATE          NULL,
    birthdate_estimated TINYINT(1)    NOT NULL,
    age                 INT           NULL,
    dead                TINYINT(1)    NOT NULL,
    death_date          DATETIME      NULL,
    deathdate_estimated TINYINT       NOT NULL,
    gender              VARCHAR(50)   NULL,
    person_name_short   VARCHAR(255)  NULL,
    person_name_long    TEXT          NULL,
    uuid                CHAR(38)      NOT NULL,
    date_created        DATETIME      NOT NULL,
    date_changed        DATETIME      NULL,
    changed_by          INT           NULL,
    date_voided         DATETIME      NULL,
    voided              TINYINT(1)    NOT NULL,
    voided_by           INT           NULL,
    void_reason         VARCHAR(255)  NULL,
    incremental_record  INT DEFAULT 0 NOT NULL,

    INDEX mamba_idx_person_id (person_id),
    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_incremental_record (incremental_record)

) CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('person', 'mamba_dim_person', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

UPDATE mamba_dim_person psn
    INNER JOIN mamba_dim_person_name pn
    on psn.person_id = pn.person_id
SET age               = fn_mamba_age_calculator(psn.birthdate, psn.death_date),
    person_name_short = CONCAT_WS(' ', pn.prefix, pn.given_name, pn.middle_name, pn.family_name),
    person_name_long  = CONCAT_WS(' ', pn.prefix, pn.given_name, pn.middle_name, pn.family_name_prefix, pn.family_name,
                                  pn.family_name2,
                                  pn.family_name_suffix, pn.degree)
WHERE pn.preferred = 1
  AND pn.voided = 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person;


~-~-
CREATE PROCEDURE sp_mamba_dim_person()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_person_create();
CALL sp_mamba_dim_person_insert();
CALL sp_mamba_dim_person_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('person', 'mamba_dim_person', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Persons
UPDATE mamba_dim_person p
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON p.person_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.person psn
    ON p.person_id = psn.person_id
SET p.birthdate           = psn.birthdate,
    p.birthdate_estimated = psn.birthdate_estimated,
    p.dead                = psn.dead,
    p.death_date          = psn.death_date,
    p.deathdate_estimated = psn.deathdate_estimated,
    p.gender              = psn.gender,
    p.uuid                = psn.uuid,
    p.date_created        = psn.date_created,
    p.date_changed        = psn.date_changed,
    p.changed_by          = psn.changed_by,
    p.date_voided         = psn.date_voided,
    p.voided              = psn.voided,
    p.voided_by           = psn.voided_by,
    p.void_reason         = psn.void_reason,
    p.incremental_record  = 1
WHERE im.incremental_table_pkey > 1;

UPDATE mamba_dim_person psn
    INNER JOIN mamba_dim_person_name pn
    on psn.person_id = pn.person_id
SET age               = fn_mamba_age_calculator(psn.birthdate, psn.death_date),
    person_name_short = CONCAT_WS(' ', pn.prefix, pn.given_name, pn.middle_name, pn.family_name),
    person_name_long  = CONCAT_WS(' ', pn.prefix, pn.given_name, pn.middle_name, pn.family_name_prefix, pn.family_name,
                                  pn.family_name2,
                                  pn.family_name_suffix, pn.degree)
WHERE psn.incremental_record = 1
  AND pn.preferred = 1
  AND pn.voided = 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('person', 'mamba_dim_person');
CALL sp_mamba_dim_person_incremental_insert();
CALL sp_mamba_dim_person_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_person_attribute
(
    person_attribute_id      INT           NOT NULL UNIQUE PRIMARY KEY,
    person_attribute_type_id INT           NOT NULL,
    person_id                INT           NOT NULL,
    uuid                     CHAR(38)      NOT NULL,
    value                    NVARCHAR(50)  NOT NULL,
    voided                   TINYINT,
    date_created             DATETIME      NOT NULL,
    date_changed             DATETIME      NULL,
    date_voided              DATETIME      NULL,
    changed_by               INT           NULL,
    voided_by                INT           NULL,
    void_reason              VARCHAR(255)  NULL,
    incremental_record       INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_person_attribute_type_id (person_attribute_type_id),
    INDEX mamba_idx_person_id (person_id),
    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_voided (voided),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('person_attribute', 'mamba_dim_person_attribute', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_person_attribute_create();
CALL sp_mamba_dim_person_attribute_insert();
CALL sp_mamba_dim_person_attribute_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('person_attribute', 'mamba_dim_person_attribute', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Persons
UPDATE mamba_dim_person_attribute mpa
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON mpa.person_attribute_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.person_attribute pa
    ON mpa.person_attribute_id = pa.person_attribute_id
SET mpa.person_attribute_id = pa.person_attribute_id,
    mpa.person_id           = pa.person_id,
    mpa.uuid                = pa.uuid,
    mpa.value               = pa.value,
    mpa.date_created        = pa.date_created,
    mpa.date_changed        = pa.date_changed,
    mpa.date_voided         = pa.date_voided,
    mpa.changed_by          = pa.changed_by,
    mpa.voided              = pa.voided,
    mpa.voided_by           = pa.voided_by,
    mpa.void_reason         = pa.void_reason,
    mpa.incremental_record  = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('person_attribute', 'mamba_dim_person_attribute');
CALL sp_mamba_dim_person_attribute_incremental_insert();
CALL sp_mamba_dim_person_attribute_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_type_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_type_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_type_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_type_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_type_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_person_attribute_type
(
    person_attribute_type_id INT           NOT NULL UNIQUE PRIMARY KEY,
    name                     NVARCHAR(50)  NOT NULL,
    description              TEXT          NULL,
    searchable               TINYINT(1)    NOT NULL,
    uuid                     NVARCHAR(50)  NOT NULL,
    date_created             DATETIME      NOT NULL,
    date_changed             DATETIME      NULL,
    date_retired             DATETIME      NULL,
    retired                  TINYINT(1)    NULL,
    retire_reason            VARCHAR(255)  NULL,
    retired_by               INT           NULL,
    changed_by               INT           NULL,
    incremental_record       INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_name (name),
    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_retired (retired),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_type_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_type_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_type_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_type_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_type_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('person_attribute_type', 'mamba_dim_person_attribute_type', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_type_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_type_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_type_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_type_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_type_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_type  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_type;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_type()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_type', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_type', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_person_attribute_type_create();
CALL sp_mamba_dim_person_attribute_type_insert();
CALL sp_mamba_dim_person_attribute_type_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_type_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_type_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_type_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_type_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_type_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_mamba_dim_table_insert('person_attribute_type', 'mamba_dim_person_attribute_type', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_type_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_type_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_type_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_type_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_type_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE mamba_dim_person_attribute_type mpat
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON mpat.person_attribute_type_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.person_attribute_type pat
    ON mpat.person_attribute_type_id = pat.person_attribute_type_id
SET mpat.name               = pat.name,
    mpat.description        = pat.description,
    mpat.searchable         = pat.searchable,
    mpat.uuid               = pat.uuid,
    mpat.date_created       = pat.date_created,
    mpat.date_changed       = pat.date_changed,
    mpat.date_retired       = pat.date_retired,
    mpat.changed_by         = pat.changed_by,
    mpat.retired            = pat.retired,
    mpat.retired_by         = pat.retired_by,
    mpat.retire_reason      = pat.retire_reason,
    mpat.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_type_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_type_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_type_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_type_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_type_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('person_attribute_type', 'mamba_dim_person_attribute_type');
CALL sp_mamba_dim_person_attribute_type_incremental_insert();
CALL sp_mamba_dim_person_attribute_type_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_patient_identifier
(
    patient_identifier_id INT           NOT NULL UNIQUE PRIMARY KEY,
    patient_id            INT           NOT NULL,
    identifier            VARCHAR(50)   NOT NULL,
    identifier_type       INT           NOT NULL,
    preferred             TINYINT       NOT NULL,
    location_id           INT           NULL,
    patient_program_id    INT           NULL,
    uuid                  CHAR(38)      NOT NULL,
    date_created          DATETIME      NOT NULL,
    date_changed          DATETIME      NULL,
    date_voided           DATETIME      NULL,
    changed_by            INT           NULL,
    voided                TINYINT,
    voided_by             INT           NULL,
    void_reason           VARCHAR(255)  NULL,
    incremental_record    INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_patient_id (patient_id),
    INDEX mamba_idx_identifier (identifier),
    INDEX mamba_idx_identifier_type (identifier_type),
    INDEX mamba_idx_preferred (preferred),
    INDEX mamba_idx_voided (voided),
    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('patient_identifier', 'mamba_dim_patient_identifier', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_patient_identifier_create();
CALL sp_mamba_dim_patient_identifier_insert();
CALL sp_mamba_dim_patient_identifier_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_mamba_dim_table_insert('patient_identifier', 'mamba_dim_patient_identifier', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE mamba_dim_patient_identifier mpi
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON mpi.patient_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.patient_identifier pi
    ON mpi.patient_id = pi.patient_id
SET mpi.patient_id         = pi.patient_id,
    mpi.identifier         = pi.identifier,
    mpi.identifier_type    = pi.identifier_type,
    mpi.preferred          = pi.preferred,
    mpi.location_id        = pi.location_id,
    mpi.patient_program_id = pi.patient_program_id,
    mpi.uuid               = pi.uuid,
    mpi.voided             = pi.voided,
    mpi.date_created       = pi.date_created,
    mpi.date_changed       = pi.date_changed,
    mpi.date_voided        = pi.date_voided,
    mpi.changed_by         = pi.changed_by,
    mpi.voided_by          = pi.voided_by,
    mpi.void_reason        = pi.void_reason,
    mpi.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('patient_identifier', 'mamba_dim_patient_identifier');
CALL sp_mamba_dim_patient_identifier_incremental_insert();
CALL sp_mamba_dim_patient_identifier_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_name_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_name_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_name_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_person_name
(
    person_name_id     INT           NOT NULL UNIQUE PRIMARY KEY,
    person_id          INT           NOT NULL,
    preferred          TINYINT       NOT NULL,
    prefix             VARCHAR(50)   NULL,
    given_name         VARCHAR(50)   NULL,
    middle_name        VARCHAR(50)   NULL,
    family_name_prefix VARCHAR(50)   NULL,
    family_name        VARCHAR(50)   NULL,
    family_name2       VARCHAR(50)   NULL,
    family_name_suffix VARCHAR(50)   NULL,
    degree             VARCHAR(50)   NULL,
    date_created       DATETIME      NOT NULL,
    date_changed       DATETIME      NULL,
    date_voided        DATETIME      NULL,
    changed_by         INT           NULL,
    voided             TINYINT(1)    NOT NULL,
    voided_by          INT           NULL,
    void_reason        VARCHAR(255)  NULL,
    incremental_record INT DEFAULT 0 NOT NULL,

    INDEX mamba_idx_person_id (person_id),
    INDEX mamba_idx_voided (voided),
    INDEX mamba_idx_preferred (preferred),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_name_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_name_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_name_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('person_name', 'mamba_dim_person_name', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_name()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_name', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_name', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_person_name_create();
CALL sp_mamba_dim_person_name_insert();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_name_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_name_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_name_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('person_name', 'mamba_dim_person_name', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_name_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_name_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_name_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Encounters
UPDATE mamba_dim_person_name dpn
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON dpn.person_name_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.person_name pn
    ON dpn.person_name_id = pn.person_name_id
SET dpn.person_name_id     = pn.person_name_id,
    dpn.person_id          = pn.person_id,
    dpn.preferred          = pn.preferred,
    dpn.prefix             = pn.prefix,
    dpn.given_name         = pn.given_name,
    dpn.middle_name        = pn.middle_name,
    dpn.family_name_prefix = pn.family_name_prefix,
    dpn.family_name        = pn.family_name,
    dpn.family_name2       = pn.family_name2,
    dpn.family_name_suffix = pn.family_name_suffix,
    dpn.degree             = pn.degree,
    dpn.date_created       = pn.date_created,
    dpn.date_changed       = pn.date_changed,
    dpn.changed_by         = pn.changed_by,
    dpn.date_voided        = pn.date_voided,
    dpn.voided             = pn.voided,
    dpn.voided_by          = pn.voided_by,
    dpn.void_reason        = pn.void_reason,
    dpn.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_name_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_name_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_name_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('person_name', 'mamba_dim_person_name');
CALL sp_mamba_dim_person_name_incremental_insert();
CALL sp_mamba_dim_person_name_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_address_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_address_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_address_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_person_address
(
    person_address_id  INT           NOT NULL UNIQUE PRIMARY KEY,
    person_id          INT           NULL,
    preferred          TINYINT       NOT NULL,
    address1           VARCHAR(255)  NULL,
    address2           VARCHAR(255)  NULL,
    address3           VARCHAR(255)  NULL,
    address4           VARCHAR(255)  NULL,
    address5           VARCHAR(255)  NULL,
    address6           VARCHAR(255)  NULL,
    address7           VARCHAR(255)  NULL,
    address8           VARCHAR(255)  NULL,
    address9           VARCHAR(255)  NULL,
    address10          VARCHAR(255)  NULL,
    address11          VARCHAR(255)  NULL,
    address12          VARCHAR(255)  NULL,
    address13          VARCHAR(255)  NULL,
    address14          VARCHAR(255)  NULL,
    address15          VARCHAR(255)  NULL,
    city_village       VARCHAR(255)  NULL,
    county_district    VARCHAR(255)  NULL,
    state_province     VARCHAR(255)  NULL,
    postal_code        VARCHAR(50)   NULL,
    country            VARCHAR(50)   NULL,
    latitude           VARCHAR(50)   NULL,
    longitude          VARCHAR(50)   NULL,
    date_created       DATETIME      NOT NULL,
    date_changed       DATETIME      NULL,
    date_voided        DATETIME      NULL,
    changed_by         INT           NULL,
    voided             TINYINT,
    voided_by          INT           NULL,
    void_reason        VARCHAR(255)  NULL,
    incremental_record INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_person_id (person_id),
    INDEX mamba_idx_preferred (preferred),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_address_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_address_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_address_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('person_address', 'mamba_dim_person_address', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_address()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_address', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_address', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_person_address_create();
CALL sp_mamba_dim_person_address_insert();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_address_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_address_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_address_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_mamba_dim_table_insert('person_address', 'mamba_dim_person_address', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_address_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_address_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_address_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE mamba_dim_person_address mpa
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON mpa.person_address_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.person_address pa
    ON mpa.person_address_id = pa.person_address_id
SET mpa.person_id          = pa.person_id,
    mpa.preferred          = pa.preferred,
    mpa.address1           = pa.address1,
    mpa.address2           = pa.address2,
    mpa.address3           = pa.address3,
    mpa.address4           = pa.address4,
    mpa.address5           = pa.address5,
    mpa.address6           = pa.address6,
    mpa.address7           = pa.address7,
    mpa.address8           = pa.address8,
    mpa.address9           = pa.address9,
    mpa.address10          = pa.address10,
    mpa.address11          = pa.address11,
    mpa.address12          = pa.address12,
    mpa.address13          = pa.address13,
    mpa.address14          = pa.address14,
    mpa.address15          = pa.address15,
    mpa.city_village       = pa.city_village,
    mpa.county_district    = pa.county_district,
    mpa.state_province     = pa.state_province,
    mpa.postal_code        = pa.postal_code,
    mpa.country            = pa.country,
    mpa.latitude           = pa.latitude,
    mpa.longitude          = pa.longitude,
    mpa.date_created       = pa.date_created,
    mpa.date_changed       = pa.date_changed,
    mpa.date_voided        = pa.date_voided,
    mpa.changed_by         = pa.changed_by,
    mpa.voided             = pa.voided,
    mpa.voided_by          = pa.voided_by,
    mpa.void_reason        = pa.void_reason,
    mpa.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_address_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_address_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_address_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('person_address', 'mamba_dim_person_address');
CALL sp_mamba_dim_person_address_incremental_insert();
CALL sp_mamba_dim_person_address_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_user_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_user_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_user_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_user_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CREATE TABLE mamba_dim_users
(
    user_id            INT           NOT NULL UNIQUE PRIMARY KEY,
    system_id          VARCHAR(50)   NOT NULL,
    username           VARCHAR(50)   NULL,
    creator            INT           NOT NULL,
    person_id          INT           NOT NULL,
    uuid               CHAR(38)      NOT NULL,
    email              VARCHAR(255)  NULL,
    retired            TINYINT(1)    NULL,
    date_created       DATETIME      NULL,
    date_changed       DATETIME      NULL,
    changed_by         INT           NULL,
    date_retired       DATETIME      NULL,
    retired_by         INT           NULL,
    retire_reason      VARCHAR(255)  NULL,
    incremental_record INT DEFAULT 0 NOT NULL,

    INDEX mamba_idx_system_id (system_id),
    INDEX mamba_idx_username (username),
    INDEX mamba_idx_retired (retired),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_user_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_user_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_user_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_user_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('users', 'mamba_dim_users', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_user_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_user_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_user_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_user_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_user;


~-~-
CREATE PROCEDURE sp_mamba_dim_user()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_user', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_user', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
    CALL sp_mamba_dim_user_create();
    CALL sp_mamba_dim_user_insert();
    CALL sp_mamba_dim_user_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_user_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_user_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_user_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_user_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_mamba_dim_table_insert('users', 'mamba_dim_users', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_user_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_user_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_user_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_user_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Users
UPDATE mamba_dim_users u
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON u.user_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.users us
    ON u.user_id = us.user_id
SET u.system_id          = us.system_id,
    u.username           = us.username,
    u.creator            = us.creator,
    u.person_id          = us.person_id,
    u.uuid               = us.uuid,
    u.email              = us.email,
    u.retired            = us.retired,
    u.date_created       = us.date_created,
    u.date_changed       = us.date_changed,
    u.changed_by         = us.changed_by,
    u.date_retired       = us.date_retired,
    u.retired_by         = us.retired_by,
    u.retire_reason      = us.retire_reason,
    u.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_user_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_user_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_user_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_user_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_etl_incremental_columns_index('users', 'mamba_dim_users');
CALL sp_mamba_dim_user_incremental_insert();
CALL sp_mamba_dim_user_incremental_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_relationship_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_relationship_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_relationship_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_relationship
(
    relationship_id    INT           NOT NULL UNIQUE PRIMARY KEY,
    person_a           INT           NOT NULL,
    relationship       INT           NOT NULL,
    person_b           INT           NOT NULL,
    start_date         DATETIME      NULL,
    end_date           DATETIME      NULL,
    creator            INT           NOT NULL,
    uuid               CHAR(38)      NOT NULL,
    date_created       DATETIME      NOT NULL,
    date_changed       DATETIME      NULL,
    changed_by         INT           NULL,
    date_voided        DATETIME      NULL,
    voided             TINYINT(1)    NOT NULL,
    voided_by          INT           NULL,
    void_reason        VARCHAR(255)  NULL,
    incremental_record INT DEFAULT 0 NOT NULL,

    INDEX mamba_idx_person_a (person_a),
    INDEX mamba_idx_person_b (person_b),
    INDEX mamba_idx_relationship (relationship),
    INDEX mamba_idx_incremental_record (incremental_record)

) CHARSET = UTF8MB3;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_relationship_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_relationship_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_relationship_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('relationship', 'mamba_dim_relationship', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_relationship_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_relationship_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_relationship_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship;


~-~-
CREATE PROCEDURE sp_mamba_dim_relationship()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_relationship', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_relationship', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_relationship_create();
CALL sp_mamba_dim_relationship_insert();
CALL sp_mamba_dim_relationship_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_relationship_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_relationship_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_relationship_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('relationship', 'mamba_dim_relationship', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_relationship_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_relationship_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_relationship_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only modified records
UPDATE mamba_dim_relationship r
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON r.relationship_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.relationship rel
    ON r.relationship_id = rel.relationship_id
SET r.relationship       = rel.relationship,
    r.person_a           = rel.person_a,
    r.relationship       = rel.relationship,
    r.person_b           = rel.person_b,
    r.start_date         = rel.start_date,
    r.end_date           = rel.end_date,
    r.creator            = rel.creator,
    r.uuid               = rel.uuid,
    r.date_created       = rel.date_created,
    r.date_changed       = rel.date_changed,
    r.changed_by         = rel.changed_by,
    r.voided             = rel.voided,
    r.voided_by          = rel.voided_by,
    r.date_voided        = rel.date_voided,
    r.incremental_record = 1
WHERE im.incremental_table_pkey > 1;


-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_relationship_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_relationship_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_relationship_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('relationship', 'mamba_dim_relationship');
CALL sp_mamba_dim_relationship_incremental_insert();
CALL sp_mamba_dim_relationship_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_orders_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_orders_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_orders_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_orders
(
    order_id               INT           NOT NULL UNIQUE PRIMARY KEY,
    uuid                   CHAR(38)      NOT NULL,
    order_type_id          INT           NOT NULL,
    concept_id             INT           NOT NULL,
    patient_id             INT           NOT NULL,
    encounter_id           INT           NOT NULL, -- links with encounter table
    accession_number       VARCHAR(255)  NULL,
    order_number           VARCHAR(50)   NOT NULL,
    orderer                INT           NOT NULL,
    instructions           TEXT          NULL,
    date_activated         DATETIME      NULL,
    auto_expire_date       DATETIME      NULL,
    date_stopped           DATETIME      NULL,
    order_reason           INT           NULL,
    order_reason_non_coded VARCHAR(255)  NULL,
    urgency                VARCHAR(50)   NOT NULL,
    previous_order_id      INT           NULL,
    order_action           VARCHAR(50)   NOT NULL,
    comment_to_fulfiller   VARCHAR(1024) NULL,
    care_setting           INT           NOT NULL,
    scheduled_date         DATETIME      NULL,
    order_group_id         INT           NULL,
    sort_weight            DOUBLE        NULL,
    fulfiller_comment      VARCHAR(1024) NULL,
    fulfiller_status       VARCHAR(50)   NULL,
    date_created           DATETIME      NOT NULL,
    creator                INT           NULL,
    voided                 TINYINT(1)    NOT NULL,
    voided_by              INT           NULL,
    date_voided            DATETIME      NULL,
    void_reason            VARCHAR(255)  NULL,
    incremental_record     INT DEFAULT 0 NOT NULL,

    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_order_type_id (order_type_id),
    INDEX mamba_idx_concept_id (concept_id),
    INDEX mamba_idx_patient_id (patient_id),
    INDEX mamba_idx_encounter_id (encounter_id),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_orders_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_orders_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_orders_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('orders', 'mamba_dim_orders', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_orders_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_orders_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_orders_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders;


~-~-
CREATE PROCEDURE sp_mamba_dim_orders()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_orders', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_orders', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_orders_create();
CALL sp_mamba_dim_orders_insert();
CALL sp_mamba_dim_orders_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_orders_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_orders_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_orders_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('orders', 'mamba_dim_orders', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_orders_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_orders_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_orders_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Encounters
UPDATE mamba_dim_orders do
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON do.order_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.orders o
    ON do.order_id = o.order_id
SET do.order_id               = o.order_id,
    do.uuid                   = o.uuid,
    do.order_type_id          = o.order_type_id,
    do.concept_id             = o.concept_id,
    do.patient_id             = o.patient_id,
    do.encounter_id           = o.encounter_id,
    do.accession_number       = o.accession_number,
    do.order_number           = o.order_number,
    do.orderer                = o.orderer,
    do.instructions           = o.instructions,
    do.date_activated         = o.date_activated,
    do.auto_expire_date       = o.auto_expire_date,
    do.date_stopped           = o.date_stopped,
    do.order_reason           = o.order_reason,
    do.order_reason_non_coded = o.order_reason_non_coded,
    do.urgency                = o.urgency,
    do.previous_order_id      = o.previous_order_id,
    do.order_action           = o.order_action,
    do.comment_to_fulfiller   = o.comment_to_fulfiller,
    do.care_setting           = o.care_setting,
    do.scheduled_date         = o.scheduled_date,
    do.order_group_id         = o.order_group_id,
    do.sort_weight            = o.sort_weight,
    do.fulfiller_comment      = o.fulfiller_comment,
    do.fulfiller_status       = o.fulfiller_status,
    do.date_created           = o.date_created,
    do.creator                = o.creator,
    do.voided                 = o.voided,
    do.voided_by              = o.voided_by,
    do.date_voided            = o.date_voided,
    do.void_reason            = o.void_reason,
    do.incremental_record     = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_orders_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_orders_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_orders_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('orders', 'mamba_dim_orders');
CALL sp_mamba_dim_orders_incremental_insert();
CALL sp_mamba_dim_orders_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_agegroup_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_agegroup_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_agegroup_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_agegroup_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_agegroup_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_agegroup
(
    id              INT         NOT NULL AUTO_INCREMENT,
    age             INT         NULL,
    datim_agegroup  VARCHAR(50) NULL,
    datim_age_val   INT         NULL,
    normal_agegroup VARCHAR(50) NULL,
    normal_age_val   INT        NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_agegroup_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_agegroup_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_agegroup_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_agegroup_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_agegroup_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_load_agegroup();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_agegroup_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_agegroup_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_agegroup_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_agegroup_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_agegroup_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- update age_value b
UPDATE mamba_dim_agegroup a
SET datim_age_val =
    CASE
        WHEN a.datim_agegroup = '<1' THEN 1
        WHEN a.datim_agegroup = '1-4' THEN 2
        WHEN a.datim_agegroup = '5-9' THEN 3
        WHEN a.datim_agegroup = '10-14' THEN 4
        WHEN a.datim_agegroup = '15-19' THEN 5
        WHEN a.datim_agegroup = '20-24' THEN 6
        WHEN a.datim_agegroup = '25-29' THEN 7
        WHEN a.datim_agegroup = '30-34' THEN 8
        WHEN a.datim_agegroup = '35-39' THEN 9
        WHEN a.datim_agegroup = '40-44' THEN 10
        WHEN a.datim_agegroup = '45-49' THEN 11
        WHEN a.datim_agegroup = '50-54' THEN 12
        WHEN a.datim_agegroup = '55-59' THEN 13
        WHEN a.datim_agegroup = '60-64' THEN 14
        WHEN a.datim_agegroup = '65+' THEN 15
    END
WHERE a.datim_agegroup IS NOT NULL;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_agegroup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_agegroup;


~-~-
CREATE PROCEDURE sp_mamba_dim_agegroup()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_agegroup', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_agegroup', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_agegroup_create();
CALL sp_mamba_dim_agegroup_insert();
CALL sp_mamba_dim_agegroup_update();
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_create;


~-~-
CREATE PROCEDURE sp_mamba_z_encounter_obs_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_z_encounter_obs_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_z_encounter_obs_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_z_encounter_obs
(
    obs_id                  INT           NOT NULL UNIQUE PRIMARY KEY,
    encounter_id            INT           NULL,
    visit_id                INT           NULL,
    person_id               INT           NOT NULL,
    order_id                INT           NULL,
    encounter_datetime      DATETIME      NOT NULL,
    obs_datetime            DATETIME      NOT NULL,
    location_id             INT           NULL,
    obs_group_id            INT           NULL,
    obs_question_concept_id INT DEFAULT 0 NOT NULL,
    obs_value_text          TEXT          NULL,
    obs_value_numeric       DOUBLE        NULL,
    obs_value_boolean       BOOLEAN       NULL,
    obs_value_coded         INT           NULL,
    obs_value_datetime      DATETIME      NULL,
    obs_value_complex       VARCHAR(1000) NULL,
    obs_value_drug          INT           NULL,
    obs_question_uuid       CHAR(38),
    obs_answer_uuid         CHAR(38),
    obs_value_coded_uuid    CHAR(38),
    encounter_type_uuid     CHAR(38),
    status                  VARCHAR(16)   NOT NULL,
    previous_version        INT           NULL,
    date_created            DATETIME      NOT NULL,
    date_voided             DATETIME      NULL,
    voided                  TINYINT(1)    NOT NULL,
    voided_by               INT           NULL,
    void_reason             VARCHAR(255)  NULL,
    incremental_record      INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_encounter_id (encounter_id),
    INDEX mamba_idx_visit_id (visit_id),
    INDEX mamba_idx_person_id (person_id),
    INDEX mamba_idx_encounter_datetime (encounter_datetime),
    INDEX mamba_idx_encounter_type_uuid (encounter_type_uuid),
    INDEX mamba_idx_obs_question_concept_id (obs_question_concept_id),
    INDEX mamba_idx_obs_value_coded (obs_value_coded),
    INDEX mamba_idx_obs_value_coded_uuid (obs_value_coded_uuid),
    INDEX mamba_idx_obs_question_uuid (obs_question_uuid),
    INDEX mamba_idx_status (status),
    INDEX mamba_idx_voided (voided),
    INDEX mamba_idx_date_voided (date_voided),
    INDEX mamba_idx_order_id (order_id),
    INDEX mamba_idx_previous_version (previous_version),
    INDEX mamba_idx_obs_group_id (obs_group_id),
    INDEX mamba_idx_incremental_record (incremental_record),
    INDEX idx_encounter_person_datetime (encounter_id, person_id, encounter_datetime)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_insert;


~-~-
CREATE PROCEDURE sp_mamba_z_encounter_obs_insert()
BEGIN
    DECLARE total_records INT;
    DECLARE batch_size INT DEFAULT 1000000; -- 1 million batches
    DECLARE mamba_offset INT DEFAULT 0;

    SELECT COUNT(*)
    INTO total_records
    FROM mamba_source_db.obs o
             INNER JOIN mamba_dim_encounter e ON o.encounter_id = e.encounter_id
             INNER JOIN (SELECT DISTINCT concept_id, concept_uuid
                         FROM mamba_concept_metadata) md ON o.concept_id = md.concept_id
    WHERE o.encounter_id IS NOT NULL;

    WHILE mamba_offset < total_records
        DO
            SET @sql = CONCAT('INSERT INTO mamba_z_encounter_obs (obs_id,
                                       encounter_id,
                                       visit_id,
                                       person_id,
                                       order_id,
                                       encounter_datetime,
                                       obs_datetime,
                                       location_id,
                                       obs_group_id,
                                       obs_question_concept_id,
                                       obs_value_text,
                                       obs_value_numeric,
                                       obs_value_coded,
                                       obs_value_datetime,
                                       obs_value_complex,
                                       obs_value_drug,
                                       obs_question_uuid,
                                       obs_answer_uuid,
                                       obs_value_coded_uuid,
                                       encounter_type_uuid,
                                       status,
                                       previous_version,
                                       date_created,
                                       date_voided,
                                       voided,
                                       voided_by,
                                       void_reason)
            SELECT o.obs_id,
                   o.encounter_id,
                   e.visit_id,
                   o.person_id,
                   o.order_id,
                   e.encounter_datetime,
                   o.obs_datetime,
                   o.location_id,
                   o.obs_group_id,
                   o.concept_id     AS obs_question_concept_id,
                   o.value_text     AS obs_value_text,
                   o.value_numeric  AS obs_value_numeric,
                   o.value_coded    AS obs_value_coded,
                   o.value_datetime AS obs_value_datetime,
                   o.value_complex  AS obs_value_complex,
                   o.value_drug     AS obs_value_drug,
                   md.concept_uuid  AS obs_question_uuid,
                   NULL             AS obs_answer_uuid,
                   NULL             AS obs_value_coded_uuid,
                   e.encounter_type_uuid,
                   o.status,
                   o.previous_version,
                   o.date_created,
                   o.date_voided,
                   o.voided,
                   o.voided_by,
                   o.void_reason
            FROM mamba_source_db.obs o
                     INNER JOIN mamba_dim_encounter e ON o.encounter_id = e.encounter_id
                     INNER JOIN (SELECT DISTINCT concept_id, concept_uuid
                                 FROM mamba_concept_metadata) md ON o.concept_id = md.concept_id
            WHERE o.encounter_id IS NOT NULL
            ORDER BY o.obs_id ASC -- Use a unique column for ordering to avoid the duplicates error because of using mamba_offset
            LIMIT ', batch_size, ' OFFSET ', mamba_offset);

            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;

            SET mamba_offset = mamba_offset + batch_size;
        END WHILE;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_update;

DELIMITER
//

~-~-
CREATE PROCEDURE sp_mamba_z_encounter_obs_update()
BEGIN
    DECLARE
total_records INT;
    DECLARE
batch_size INT DEFAULT 100000; -- 1 million batches
    DECLARE
mamba_offset INT DEFAULT 0;

SELECT COUNT(*)
INTO total_records
FROM mamba_z_encounter_obs;
CREATE
TEMPORARY TABLE mamba_temp_value_coded_values
        CHARSET = UTF8MB4 AS
SELECT m.concept_id AS concept_id,
       m.uuid       AS concept_uuid,
       m.name       AS concept_name
FROM mamba_dim_concept m
WHERE concept_id in (SELECT DISTINCT obs_value_coded
                     FROM mamba_z_encounter_obs
                     WHERE obs_value_coded IS NOT NULL);
CREATE INDEX mamba_idx_concept_id ON mamba_temp_value_coded_values (concept_id);

-- update obs_value_coded (UUIDs & Concept value names)
WHILE
mamba_offset < total_records
        DO
            -- Perform the batch update
UPDATE mamba_z_encounter_obs z
    JOIN (SELECT encounter_id FROM mamba_z_encounter_obs GROUP BY encounter_id ORDER BY encounter_id LIMIT batch_size OFFSET mamba_offset ) AS filter
ON filter.encounter_id=z.encounter_id
    INNER JOIN mamba_temp_value_coded_values mtv
    ON z.obs_value_coded = mtv.concept_id
    SET z.obs_value_text = mtv.concept_name, z.obs_value_coded_uuid = mtv.concept_uuid
WHERE z.obs_value_coded IS NOT NULL;
COMMIT;
SET
mamba_offset = mamba_offset + batch_size;
END WHILE;
-- update column obs_value_boolean (Concept values)
UPDATE mamba_z_encounter_obs z
SET obs_value_boolean =
        CASE
            WHEN obs_value_text IN ('FALSE', 'No') THEN 0
            WHEN obs_value_text IN ('TRUE', 'Yes') THEN 1
            ELSE NULL
            END
WHERE z.obs_value_coded IS NOT NULL
  AND obs_question_concept_id in
      (SELECT DISTINCT concept_id
       FROM mamba_dim_concept c
       WHERE c.datatype = 'Boolean');

DROP
TEMPORARY TABLE IF EXISTS mamba_temp_value_coded_values;
END
//


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs;


~-~-
CREATE PROCEDURE sp_mamba_z_encounter_obs()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_z_encounter_obs', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_z_encounter_obs', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_z_encounter_obs_create();
CALL sp_mamba_z_encounter_obs_insert();
CALL sp_mamba_z_encounter_obs_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_z_encounter_obs_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_z_encounter_obs_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_z_encounter_obs_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert into mamba_z_encounter_obs
INSERT INTO mamba_z_encounter_obs (obs_id,
                                   encounter_id,
                                   visit_id,
                                   person_id,
                                   order_id,
                                   encounter_datetime,
                                   obs_datetime,
                                   location_id,
                                   obs_group_id,
                                   obs_question_concept_id,
                                   obs_value_text,
                                   obs_value_numeric,
                                   obs_value_coded,
                                   obs_value_datetime,
                                   obs_value_complex,
                                   obs_value_drug,
                                   obs_question_uuid,
                                   obs_answer_uuid,
                                   obs_value_coded_uuid,
                                   encounter_type_uuid,
                                   status,
                                   previous_version,
                                   date_created,
                                   date_voided,
                                   voided,
                                   voided_by,
                                   void_reason,
                                   incremental_record)
SELECT o.obs_id,
       o.encounter_id,
       e.visit_id,
       o.person_id,
       o.order_id,
       e.encounter_datetime,
       o.obs_datetime,
       o.location_id,
       o.obs_group_id,
       o.concept_id     AS obs_question_concept_id,
       o.value_text     AS obs_value_text,
       o.value_numeric  AS obs_value_numeric,
       o.value_coded    AS obs_value_coded,
       o.value_datetime AS obs_value_datetime,
       o.value_complex  AS obs_value_complex,
       o.value_drug     AS obs_value_drug,
       md.concept_uuid  AS obs_question_uuid,
       NULL             AS obs_answer_uuid,
       NULL             AS obs_value_coded_uuid,
       e.encounter_type_uuid,
       o.status,
       o.previous_version,
       o.date_created,
       o.date_voided,
       o.voided,
       o.voided_by,
       o.void_reason,
       1
FROM mamba_source_db.obs o
         INNER JOIN mamba_etl_incremental_columns_index_new ic ON o.obs_id = ic.incremental_table_pkey
         INNER JOIN mamba_dim_encounter e ON o.encounter_id = e.encounter_id
         INNER JOIN (SELECT DISTINCT concept_id, concept_uuid
                     FROM mamba_concept_metadata) md ON o.concept_id = md.concept_id
WHERE o.encounter_id IS NOT NULL;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_z_encounter_obs_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_z_encounter_obs_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_z_encounter_obs_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records

-- Update voided Obs (FINAL & AMENDED pair obs are incremental 1 though we shall not consider them in incremental flattening)
UPDATE mamba_z_encounter_obs z
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON z.obs_id = im.incremental_table_pkey
    INNER JOIN mamba_source_db.obs o
    ON z.obs_id = o.obs_id
SET z.encounter_id            = o.encounter_id,
    z.person_id               = o.person_id,
    z.order_id                = o.order_id,
    z.obs_datetime            = o.obs_datetime,
    z.location_id             = o.location_id,
    z.obs_group_id            = o.obs_group_id,
    z.obs_question_concept_id = o.concept_id,
    z.obs_value_text          = o.value_text,
    z.obs_value_numeric       = o.value_numeric,
    z.obs_value_coded         = o.value_coded,
    z.obs_value_datetime      = o.value_datetime,
    z.obs_value_complex       = o.value_complex,
    z.obs_value_drug          = o.value_drug,
    -- z.encounter_type_uuid     = o.encounter_type_uuid,
    z.status                  = o.status,
    z.previous_version        = o.previous_version,
    -- z.row_num            = o.row_num,
    z.date_created            = o.date_created,
    z.voided                  = o.voided,
    z.voided_by               = o.voided_by,
    z.date_voided             = o.date_voided,
    z.void_reason             = o.void_reason,
    z.incremental_record      = 1
WHERE im.incremental_table_pkey > 1;

-- update obs_value_coded (UUIDs & Concept value names) for only NEW Obs (not voided)
UPDATE mamba_z_encounter_obs z
    INNER JOIN mamba_dim_concept c
    ON z.obs_value_coded = c.concept_id
SET z.obs_value_text       = c.name,
    z.obs_value_coded_uuid = c.uuid
WHERE z.incremental_record = 1
  AND z.obs_value_coded IS NOT NULL;

-- update column obs_value_boolean (Concept values) for only NEW Obs (not voided)
UPDATE mamba_z_encounter_obs z
SET obs_value_boolean =
        CASE
            WHEN obs_value_text IN ('FALSE', 'No') THEN 0
            WHEN obs_value_text IN ('TRUE', 'Yes') THEN 1
            ELSE NULL
            END
WHERE z.incremental_record = 1
  AND z.obs_value_coded IS NOT NULL
  AND obs_question_concept_id in
      (SELECT DISTINCT concept_id
       FROM mamba_dim_concept c
       WHERE c.datatype = 'Boolean');

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_incremental;


~-~-
CREATE PROCEDURE sp_mamba_z_encounter_obs_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_z_encounter_obs_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_z_encounter_obs_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('obs', 'mamba_z_encounter_obs');
CALL sp_mamba_z_encounter_obs_incremental_insert();
CALL sp_mamba_z_encounter_obs_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_drop_and_flatten  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_drop_and_flatten;


~-~-
CREATE PROCEDURE sp_mamba_data_processing_drop_and_flatten()

BEGIN

    CALL sp_mamba_system_drop_all_tables();

    CALL sp_mamba_dim_agegroup;

    CALL sp_mamba_dim_location;

    CALL sp_mamba_dim_patient_identifier_type;

    CALL sp_mamba_dim_concept_datatype;

    CALL sp_mamba_dim_concept_name;

    CALL sp_mamba_dim_concept;

    CALL sp_mamba_dim_concept_answer;

    CALL sp_mamba_dim_encounter_type;

    CALL sp_mamba_flat_table_config;

    CALL sp_mamba_concept_metadata;

    CALL sp_mamba_dim_report_definition;

    CALL sp_mamba_dim_encounter;

    CALL sp_mamba_dim_person_name;

    CALL sp_mamba_dim_person;

    CALL sp_mamba_dim_person_attribute_type;

    CALL sp_mamba_dim_person_attribute;

    CALL sp_mamba_dim_person_address;

    CALL sp_mamba_dim_user;

    CALL sp_mamba_dim_relationship;

    CALL sp_mamba_dim_patient_identifier;

    CALL sp_mamba_dim_orders;

    CALL sp_mamba_z_encounter_obs;

    CALL sp_mamba_obs_group;

    CALL sp_mamba_flat_encounter_table_create_all;

    CALL sp_mamba_flat_encounter_table_insert_all;

    CALL sp_mamba_flat_encounter_obs_group_table_create_all;

    CALL sp_mamba_flat_encounter_obs_group_table_insert_all;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_increment_and_flatten  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_increment_and_flatten;


~-~-
CREATE PROCEDURE sp_mamba_data_processing_increment_and_flatten()

BEGIN

    CALL sp_mamba_dim_location_incremental;

    CALL sp_mamba_dim_patient_identifier_type_incremental;

    CALL sp_mamba_dim_concept_datatype_incremental;

    CALL sp_mamba_dim_concept_name_incremental;

    CALL sp_mamba_dim_concept_incremental;

    CALL sp_mamba_dim_concept_answer_incremental;

    CALL sp_mamba_dim_encounter_type_incremental;

    CALL sp_mamba_flat_table_config_incremental;

    CALL sp_mamba_concept_metadata_incremental;

    CALL sp_mamba_dim_encounter_incremental;

    CALL sp_mamba_dim_person_name_incremental;

    CALL sp_mamba_dim_person_incremental;

    CALL sp_mamba_dim_person_attribute_type_incremental;

    CALL sp_mamba_dim_person_attribute_incremental;

    CALL sp_mamba_dim_person_address_incremental;

    CALL sp_mamba_dim_user_incremental;

    CALL sp_mamba_dim_relationship_incremental;

    CALL sp_mamba_dim_patient_identifier_incremental;

    CALL sp_mamba_dim_orders_incremental;

    -- incremental inserts into the mamba_z_encounter_obs table only
    CALL sp_mamba_z_encounter_obs_incremental;

    CALL sp_mamba_flat_table_incremental_create_all;

    -- create and insert into flat tables whose columns or table names have been modified/added (determined by json_data hash)
    CALL sp_mamba_flat_table_incremental_insert_all;

    -- insert from mamba_z_encounter_obs into the flat table OBS that are either MODIFIED or CREATED/NEW
    -- (Deletes and inserts an entire Encounter (by id) if one of the obs is modified)
    CALL sp_mamba_flat_table_incremental_update_encounter;

    CALL sp_mamba_reset_incremental_update_flag_all;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_data_processing_derived_art_follow_up  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_data_processing_derived_art_follow_up;


~-~-
CREATE PROCEDURE sp_data_processing_derived_art_follow_up()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_data_processing_derived_art_follow_up', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_data_processing_derived_art_follow_up', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_dim_client_art_follow_up();
CALL sp_fact_encounter_art_follow_up();
CALL sp_fact_encounter_art_first_follow_up();
CALL sp_fact_encounter_art_latest_follow_up();
CALL sp_fact_encounter_tx_new();


-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_data_processing_derived_transfer_in  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_data_processing_derived_transfer_in;


~-~-
CREATE PROCEDURE sp_data_processing_derived_transfer_in()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_data_processing_derived_transfer_in', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_data_processing_derived_transfer_in', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_dim_client_transfer_in();
CALL sp_fact_encounter_transfer_in();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_data_processing_derived_transfer_out  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_data_processing_derived_transfer_out;


~-~-
CREATE PROCEDURE sp_data_processing_derived_transfer_out()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_data_processing_derived_transfer_out', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_data_processing_derived_transfer_out', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_fact_encounter_transfer_out();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_drop_all_derived_tables  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Needed for now till incremental is full implemented. We will just drop all tables and recreate them
DROP PROCEDURE IF EXISTS sp_mamba_drop_all_derived_tables;
~-~-
CREATE PROCEDURE sp_mamba_drop_all_derived_tables()
BEGIN
    DROP TABLE IF EXISTS mamba_fact_tx_new;
    DROP TABLE IF EXISTS mamba_fact_tx_curr;
    DROP TABLE IF EXISTS mamba_fact_art_latest_follow_up;
    DROP TABLE IF EXISTS mamba_fact_art_first_follow_up;
    DROP TABLE IF EXISTS mamba_fact_art_follow_up;
    DROP TABLE IF EXISTS mamba_fact_client_tranfer_in;
    DROP TABLE IF EXISTS mamba_fact_client_tranfer_out;
    DROP TABLE IF EXISTS mamba_dim_client_art_follow_up;
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_etl  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_data_processing_etl;

~-~-
CREATE PROCEDURE sp_mamba_data_processing_etl(IN etl_incremental_mode INT)

BEGIN
    -- add base folder SP here if any --

    -- Call the implementer ETL process
    CALL sp_mamba_drop_all_derived_tables();
    CALL sp_data_processing_derived_art_follow_up();
--     CALL sp_data_processing_derived_transfer_in();
--     CALL sp_data_processing_derived_transfer_out();

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_dim_client_art_follow_up  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_dim_client_art_follow_up;


~-~-
CREATE PROCEDURE sp_dim_client_art_follow_up()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_dim_client_art_follow_up', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_dim_client_art_follow_up', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_dim_client_art_follow_up_create();
CALL sp_dim_client_art_follow_up_insert();
CALL sp_dim_client_art_follow_up_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_dim_client_art_follow_up_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_dim_client_art_follow_up_create;


~-~-
CREATE PROCEDURE sp_dim_client_art_follow_up_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_dim_client_art_follow_up_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_dim_client_art_follow_up_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_dim_client_art_follow_up
(
    id                INT AUTO_INCREMENT,
    client_id         INT           NOT NULL,
    patient_name      NVARCHAR(255) NULL,
    mrn               NVARCHAR(50)  NULL,
    uan               NVARCHAR(50)  NULL,
    current_age       INT,
    mobile_no         NVARCHAR(50),
    date_of_birth     DATE          NULL,
    sex               NVARCHAR(50)  NULL,
    state_province    NVARCHAR(255) NULL,
    county_district   NVARCHAR(255) NULL,
    city_village      NVARCHAR(255) NULL,
    coarse_age_group  NVARCHAR(255) NULL,
    fine_age_group    NVARCHAR(255) NULL,
    PRIMARY KEY (id)
);
CREATE INDEX mamba_dim_client_art_follow_up_client_id_index ON mamba_dim_client_art_follow_up (client_id);
CREATE INDEX mamba_dim_client_art_follow_up_mrn_index ON mamba_dim_client_art_follow_up (mrn);
CREATE INDEX mamba_dim_client_art_follow_up_uan_index ON mamba_dim_client_art_follow_up (uan);
-- CREATE INDEX mamba_dim_client_care_and_treatment_enrollment_date_index ON mamba_dim_client_care_and_treatment (enrollment_date);
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_dim_client_art_follow_up_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_dim_client_art_follow_up_insert;


~-~-
CREATE PROCEDURE sp_dim_client_art_follow_up_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_dim_client_art_follow_up_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_dim_client_art_follow_up_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
INSERT INTO mamba_dim_client_art_follow_up (client_id,
                                                 patient_name,
                                                 mrn,
                                                 uan,
                                                 current_age,
                                                 mobile_no,
                                                 date_of_birth,
                                                 sex,
                                                 state_province,
                                                 county_district,
                                                 city_village,
                                                 coarse_age_group,
                                                 fine_age_group)
SELECT person.person_id,
       person.person_name_long,
       (SELECT pid.identifier
        FROM mamba_dim_patient_identifier pid
        WHERE pid.patient_id = person.person_id
          AND pid.identifier_type = 5
        LIMIT 1)                                                                AS 'MRN',
       (SELECT pid.identifier
        FROM mamba_dim_patient_identifier pid
        WHERE pid.patient_id = person.person_id
          AND pid.identifier_type = 6
        LIMIT 1)                                                                AS 'UAN',
       fn_mamba_age_calculator(person.birthdate, CURDATE())                     AS current_age,
       (select p_attr.value as mobile_no
        from mamba_dim_person_attribute p_attr
        where person.person_id = p_attr.person_id
          and p_attr.person_attribute_type_id = 9
        LIMIT 1),
       person.birthdate,
       CASE
           WHEN person.gender = 'F' THEN 'FEMALE'
           WHEN person.gender = 'M' THEN 'MALE'
           END                                                                  AS gender,
       p_add.state_province,
       p_add.county_district,
       p_add.city_village,
       (SELECT normal_agegroup from mamba_dim_agegroup where age = current_age)  as coarse_age_group,
       (SELECT  datim_agegroup from mamba_dim_agegroup where age = current_age) as fine_age_group

FROM mamba_dim_person person
         LEFT JOIN mamba_dim_person_address p_add ON person.person_id = p_add.person_id;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_dim_client_art_follow_up_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_dim_client_art_follow_up_update;


~-~-
CREATE PROCEDURE sp_dim_client_art_follow_up_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_dim_client_art_follow_up_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_dim_client_art_follow_up_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_art_follow_up  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_follow_up;


~-~-
CREATE PROCEDURE sp_fact_encounter_art_follow_up()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_art_follow_up', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_art_follow_up', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_fact_encounter_art_follow_up_create();
CALL sp_fact_encounter_art_follow_up_insert();
CALL sp_fact_encounter_art_follow_up_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_art_follow_up_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_follow_up_create;


~-~-
CREATE PROCEDURE sp_fact_encounter_art_follow_up_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_art_follow_up_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_art_follow_up_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_fact_art_follow_up
(
    id                          INT AUTO_INCREMENT,
    encounter_datetime         DATE,
    encounter_id                INT,
    client_id                   INT NULL,
    weight_in_kg                DOUBLE,
    cd4_count                   DOUBLE,
    current_who_hiv_stage       NVARCHAR(255),
    nutritional_status          NVARCHAR(255),
    tb_screening_result         NVARCHAR(255),
    enrollment_date             DATE,
    hiv_confirmed_date          DATE,
    art_start_date              DATE,
    days_difference             INT,
    followup_date               DATE,
    regimen                     NVARCHAR(255),
    arv_dose_days               NVARCHAR(255),
    pregnancy_status            NVARCHAR(255),
    breast_feeding_status       NVARCHAR(255),
    follow_up_status            NVARCHAR(255),
    ti                          NVARCHAR(255),
    treatment_end_date          DATE,
    next_visit_date             DATE,
    hiv_viral_load_count              INT,
    hiv_viral_load_status       NVARCHAR(255),
    viral_load_test_status      NVARCHAR(255),
    on_antiretroviral_therapy   NVARCHAR(255),
    viral_load_test_indication      NVARCHAR(255),
    antiretroviral_side_effects    NVARCHAR(255),
    anitiretroviral_adherence_level NVARCHAR(255),
    date_of_reported_hiv_viral_load NVARCHAR(255),
    date_viral_load_results_received NVARCHAR(255),
    routine_viral_load_test_indication NVARCHAR(255),
    targeted_viral_load_test_indication NVARCHAR(255),
    dsd_category                        NVARCHAR(255),
    tpt_start_date                      DATE,
    tpt_completed_date                  DATE,
    tpt_discontinued_date               DATE,
    tuberculosis_treatment_end_date     DATE,
    PRIMARY KEY (id)
);
CREATE INDEX mamba_fact_art_follow_up_art_start_date_index ON mamba_fact_art_follow_up (art_start_date);
CREATE INDEX mamba_fact_art_follow_up_client_id_index ON mamba_fact_art_follow_up (client_id);
CREATE INDEX mamba_fact_art_follow_up_followup_date_index ON mamba_fact_art_follow_up (followup_date);
CREATE INDEX mamba_fact_art_follow_up_regimen_index ON mamba_fact_art_follow_up (regimen);
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_art_follow_up_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_follow_up_insert;


~-~-
CREATE PROCEDURE sp_fact_encounter_art_follow_up_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_art_follow_up_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_art_follow_up_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
INSERT INTO mamba_fact_art_follow_up (client_id,
                                      encounter_id,
                                      encounter_datetime,
                                      weight_in_kg,
                                      cd4_count,
                                      current_who_hiv_stage,
                                      nutritional_status,
                                      tb_screening_result,
                                      enrollment_date,
                                      hiv_confirmed_date,
                                      art_start_date,
                                      days_difference,
                                      followup_date,
                                      regimen,
                                      arv_dose_days,
                                      pregnancy_status,
                                      breast_feeding_status,
                                      follow_up_status,
                                      ti,
                                      treatment_end_date,
                                      next_visit_date,
                                      hiv_viral_load_count,
                                      hiv_viral_load_status,
                                      viral_load_test_status,
                                      on_antiretroviral_therapy,
                                      viral_load_test_indication,
                                      antiretroviral_side_effects,
                                      anitiretroviral_adherence_level,
                                      date_of_reported_hiv_viral_load,
                                      date_viral_load_results_received,
                                      routine_viral_load_test_indication,
                                      targeted_viral_load_test_indication,
                                      dsd_category,
                                      tpt_start_date,
                                      tpt_completed_date,
                                      tpt_discontinued_date,
                                      tuberculosis_treatment_end_date)
SELECT follow_up.client_id,
       follow_up.encounter_id,
       follow_up.encounter_datetime,
       weight_text_,
       cd4_count,
       current_who_hiv_stage,
       nutritional_status_of_adult,
       tb_diagnostic_test_result,
       date_enrolled_in_care,
       date_of_hiv_diagnosis,
       art_antiretroviral_start_date,
       DATEDIFF(art_antiretroviral_start_date,date_of_hiv_diagnosis) as days_difference,
       follow_up_date_followup_,
       regimen,
       antiretroviral_art_dispensed_dose_i,
       pregnancy_status,
       currently_breastfeeding_child,
       follow_up_status,
       'why_eligible_for_hiv_test_',
       treatment_end_date,
       next_visit_date,
       hiv_viral_load,
       hiv_viral_load_status,
       viral_load_test_status,
       on_antiretroviral_therapy,
       viral_load_test_indication,
       antiretroviral_side_effects,
       anitiretroviral_adherence_level,
       date_of_reported_hiv_viral_load,
       date_viral_load_results_received,
       routine_viral_load_test_indication,
       targeted_viral_load_test_indication,
       dsd_category,
       date_started_on_tuberculosis_prophy,
       date_completed_tuberculosis_prophyl,
       date_discontinued_tuberculosis_prop,
       treatment_end_date
FROM mamba_flat_encounter_follow_up follow_up
         JOIN mamba_flat_encounter_follow_up_1 enc_follow_up_1
              on enc_follow_up_1.encounter_id = follow_up.encounter_id
         JOIN mamba_flat_encounter_intake_a int_a on follow_up.client_id = int_a.client_id
where art_antiretroviral_start_date is not null;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_art_follow_up_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_follow_up_update;


~-~-
CREATE PROCEDURE sp_fact_encounter_art_follow_up_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_art_follow_up_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_art_follow_up_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_art_first_follow_up  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_first_follow_up;


~-~-
CREATE PROCEDURE sp_fact_encounter_art_first_follow_up()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_art_first_follow_up', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_art_first_follow_up', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_fact_encounter_art_first_follow_up_create();
CALL sp_fact_encounter_art_first_follow_up_insert();
CALL sp_fact_encounter_art_first_follow_up_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_art_first_follow_up_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_first_follow_up_create;


~-~-
CREATE PROCEDURE sp_fact_encounter_art_first_follow_up_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_art_first_follow_up_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_art_first_follow_up_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_fact_art_first_follow_up
(
    id                          INT AUTO_INCREMENT,
    encounter_datetime         DATE,
    encounter_id                INT,
    client_id                   INT NULL,
    weight_in_kg                DOUBLE,
    cd4_count                   DOUBLE,
    current_who_hiv_stage       NVARCHAR(255),
    nutritional_status          NVARCHAR(255),
    tb_screening_result         NVARCHAR(255),
    enrollment_date             DATE,
    hiv_confirmed_date          DATE,
    art_start_date              DATE,
    days_difference             INT,
    followup_date               DATE,
    regimen                     NVARCHAR(255),
    arv_dose_days               NVARCHAR(255),
    pregnancy_status            NVARCHAR(255),
    breast_feeding_status       NVARCHAR(255),
    follow_up_status            NVARCHAR(255),
    ti                          NVARCHAR(255),
    treatment_end_date          DATE,
    next_visit_date             DATE,
    hiv_viral_load_count              INT,
    hiv_viral_load_status       NVARCHAR(255),
    viral_load_test_status      NVARCHAR(255),
    on_antiretroviral_therapy   NVARCHAR(255),
    viral_load_test_indication      NVARCHAR(255),
    antiretroviral_side_effects    NVARCHAR(255),
    anitiretroviral_adherence_level NVARCHAR(255),
    date_of_reported_hiv_viral_load NVARCHAR(255),
    date_viral_load_results_received NVARCHAR(255),
    routine_viral_load_test_indication NVARCHAR(255),
    targeted_viral_load_test_indication NVARCHAR(255),
    dsd_category                        NVARCHAR(255),
    tpt_start_date                      DATE,
    tpt_completed_date                  DATE,
    tpt_discontinued_date               DATE,
    tuberculosis_treatment_end_date     DATE,
    PRIMARY KEY (id)
);
CREATE INDEX mamba_fact_art_first_follow_up_art_start_date_index ON mamba_fact_art_first_follow_up (art_start_date);
CREATE INDEX mamba_fact_art_first_latest_follow_up_client_id_index ON mamba_fact_art_first_follow_up (client_id);
CREATE INDEX mamba_fact_art_first_follow_up_followup_date_index ON mamba_fact_art_first_follow_up (followup_date);
CREATE INDEX mamba_fact_art_first_follow_up_regimen_index ON mamba_fact_art_first_follow_up (regimen);
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_art_first_follow_up_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_first_follow_up_insert;


~-~-
CREATE PROCEDURE sp_fact_encounter_art_first_follow_up_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_art_first_follow_up_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_art_first_follow_up_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
INSERT INTO mamba_fact_art_first_follow_up (client_id,
                                      encounter_id,
                                      encounter_datetime,
                                      weight_in_kg,
                                      cd4_count,
                                      current_who_hiv_stage,
                                      nutritional_status,
                                      tb_screening_result,
                                      enrollment_date,
                                      hiv_confirmed_date,
                                      art_start_date,
                                      days_difference,
                                      followup_date,
                                      regimen,
                                      arv_dose_days,
                                      pregnancy_status,
                                      breast_feeding_status,
                                      follow_up_status,
                                      ti,
                                      treatment_end_date,
                                      next_visit_date,
                                      hiv_viral_load_count,
                                      hiv_viral_load_status,
                                      viral_load_test_status,
                                      on_antiretroviral_therapy,
                                      viral_load_test_indication,
                                      antiretroviral_side_effects,
                                      anitiretroviral_adherence_level,
                                      date_of_reported_hiv_viral_load,
                                      date_viral_load_results_received,
                                      routine_viral_load_test_indication,
                                      targeted_viral_load_test_indication,
                                      dsd_category,
                                      tpt_start_date,
                                      tpt_completed_date,
                                      tpt_discontinued_date,
                                      tuberculosis_treatment_end_date)
WITH FirstFollowUp AS (
    SELECT
        client_id,
        encounter_id,
        encounter_datetime,
        weight_in_kg,
        cd4_count,
        current_who_hiv_stage,
        nutritional_status,
        tb_screening_result,
        enrollment_date,
        hiv_confirmed_date,
        art_start_date,
        days_difference,
        followup_date,
        regimen,
        arv_dose_days,
        pregnancy_status,
        breast_feeding_status,
        follow_up_status,
        ti,
        treatment_end_date,
        next_visit_date,
        hiv_viral_load_count,
        hiv_viral_load_status,
        viral_load_test_status,
        on_antiretroviral_therapy,
        viral_load_test_indication,
        antiretroviral_side_effects,
        anitiretroviral_adherence_level,
        date_of_reported_hiv_viral_load,
        date_viral_load_results_received,
        routine_viral_load_test_indication,
        targeted_viral_load_test_indication,
        dsd_category,
        tpt_start_date,
        tpt_completed_date,
        tpt_discontinued_date,
        tuberculosis_treatment_end_date,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY followup_date) AS rn
    FROM mamba_fact_art_follow_up
)
SELECT
    client_id,
    encounter_id,
    encounter_datetime,
    weight_in_kg,
    cd4_count,
    current_who_hiv_stage,
    nutritional_status,
    tb_screening_result,
    enrollment_date,
    hiv_confirmed_date,
    art_start_date,
    days_difference,
    followup_date,
    regimen,
    arv_dose_days,
    pregnancy_status,
    breast_feeding_status,
    follow_up_status,
    ti,
    treatment_end_date,
    next_visit_date,
    hiv_viral_load_count,
    hiv_viral_load_status,
    viral_load_test_status,
    on_antiretroviral_therapy,
    viral_load_test_indication,
    antiretroviral_side_effects,
    anitiretroviral_adherence_level,
    date_of_reported_hiv_viral_load,
    date_viral_load_results_received,
    routine_viral_load_test_indication,
    targeted_viral_load_test_indication,
    dsd_category,
    tpt_start_date,
    tpt_completed_date,
    tpt_discontinued_date,
    tuberculosis_treatment_end_date
FROM FirstFollowUp
WHERE rn = 1;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_art_first_follow_up_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_first_follow_up_update;


~-~-
CREATE PROCEDURE sp_fact_encounter_art_first_follow_up_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_art_first_follow_up_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_art_first_follow_up_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_art_latest_follow_up  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_latest_follow_up;


~-~-
CREATE PROCEDURE sp_fact_encounter_art_latest_follow_up()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_art_latest_follow_up', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_art_latest_follow_up', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_fact_encounter_art_latest_follow_up_create();
CALL sp_fact_encounter_art_latest_follow_up_insert();
CALL sp_fact_encounter_art_latest_follow_up_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_art_latest_follow_up_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_latest_follow_up_create;


~-~-
CREATE PROCEDURE sp_fact_encounter_art_latest_follow_up_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_art_latest_follow_up_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_art_latest_follow_up_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_fact_art_latest_follow_up
(
    id                          INT AUTO_INCREMENT,
    encounter_datetime         DATE,
    encounter_id                INT,
    client_id                   INT NULL,
    weight_in_kg                DOUBLE,
    cd4_count                   DOUBLE,
    current_who_hiv_stage       NVARCHAR(255),
    nutritional_status          NVARCHAR(255),
    tb_screening_result         NVARCHAR(255),
    enrollment_date             DATE,
    hiv_confirmed_date          DATE,
    art_start_date              DATE,
    days_difference             INT,
    followup_date               DATE,
    regimen                     NVARCHAR(255),
    arv_dose_days               NVARCHAR(255),
    pregnancy_status            NVARCHAR(255),
    breast_feeding_status       NVARCHAR(255),
    follow_up_status            NVARCHAR(255),
    ti                          NVARCHAR(255),
    treatment_end_date          DATE,
    next_visit_date             DATE,
    hiv_viral_load_count              INT,
    hiv_viral_load_status       NVARCHAR(255),
    viral_load_test_status      NVARCHAR(255),
    on_antiretroviral_therapy   NVARCHAR(255),
    viral_load_test_indication      NVARCHAR(255),
    antiretroviral_side_effects    NVARCHAR(255),
    anitiretroviral_adherence_level NVARCHAR(255),
    date_of_reported_hiv_viral_load NVARCHAR(255),
    date_viral_load_results_received NVARCHAR(255),
    routine_viral_load_test_indication NVARCHAR(255),
    targeted_viral_load_test_indication NVARCHAR(255),
    dsd_category                        NVARCHAR(255),
    tpt_start_date                      DATE,
    tpt_completed_date                  DATE,
    tpt_discontinued_date               DATE,
    tuberculosis_treatment_end_date     DATE,
    PRIMARY KEY (id)
);
CREATE INDEX mamba_fact_art_latest_follow_up_art_start_date_index ON mamba_fact_art_latest_follow_up (art_start_date);
CREATE INDEX mamba_fact_art_latest_latest_follow_up_client_id_index ON mamba_fact_art_latest_follow_up (client_id);
CREATE INDEX mamba_fact_art_latest_follow_up_followup_date_index ON mamba_fact_art_latest_follow_up (followup_date);
CREATE INDEX mamba_fact_art_latest_follow_up_regimen_index ON mamba_fact_art_latest_follow_up (regimen);
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_art_latest_follow_up_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_latest_follow_up_insert;


~-~-
CREATE PROCEDURE sp_fact_encounter_art_latest_follow_up_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_art_latest_follow_up_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_art_latest_follow_up_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
INSERT INTO mamba_fact_art_latest_follow_up (client_id,
                                      encounter_id,
                                      encounter_datetime,
                                      weight_in_kg,
                                      cd4_count,
                                      current_who_hiv_stage,
                                      nutritional_status,
                                      tb_screening_result,
                                      enrollment_date,
                                      hiv_confirmed_date,
                                      art_start_date,
                                      days_difference,
                                      followup_date,
                                      regimen,
                                      arv_dose_days,
                                      pregnancy_status,
                                      breast_feeding_status,
                                      follow_up_status,
                                      ti,
                                      treatment_end_date,
                                      next_visit_date,
                                      hiv_viral_load_count,
                                      hiv_viral_load_status,
                                      viral_load_test_status,
                                      on_antiretroviral_therapy,
                                      viral_load_test_indication,
                                      antiretroviral_side_effects,
                                      anitiretroviral_adherence_level,
                                      date_of_reported_hiv_viral_load,
                                      date_viral_load_results_received,
                                      routine_viral_load_test_indication,
                                      targeted_viral_load_test_indication,
                                      dsd_category,
                                      tpt_start_date,
                                      tpt_completed_date,
                                      tpt_discontinued_date,
                                      tuberculosis_treatment_end_date)
WITH RankedFollowups AS (
    SELECT
        client_id,
        encounter_id,
        encounter_datetime,
        weight_in_kg,
        cd4_count,
        current_who_hiv_stage,
        nutritional_status,
        tb_screening_result,
        enrollment_date,
        hiv_confirmed_date,
        art_start_date,
        days_difference,
        followup_date,
        regimen,
        arv_dose_days,
        pregnancy_status,
        breast_feeding_status,
        follow_up_status,
        ti,
        treatment_end_date,
        next_visit_date,
        hiv_viral_load_count,
        hiv_viral_load_status,
        viral_load_test_status,
        on_antiretroviral_therapy,
        viral_load_test_indication,
        antiretroviral_side_effects,
        anitiretroviral_adherence_level,
        date_of_reported_hiv_viral_load,
        date_viral_load_results_received,
        routine_viral_load_test_indication,
        targeted_viral_load_test_indication,
        dsd_category,
        tpt_start_date,
        tpt_completed_date,
        tpt_discontinued_date,
        tuberculosis_treatment_end_date,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY followup_date DESC) AS rn
    FROM mamba_fact_art_follow_up
)
SELECT
    client_id,
    encounter_id,
    encounter_datetime,
    weight_in_kg,
    cd4_count,
    current_who_hiv_stage,
    nutritional_status,
    tb_screening_result,
    enrollment_date,
    hiv_confirmed_date,
    art_start_date,
    days_difference,
    followup_date,
    regimen,
    arv_dose_days,
    pregnancy_status,
    breast_feeding_status,
    follow_up_status,
    ti,
    treatment_end_date,
    next_visit_date,
    hiv_viral_load_count,
    hiv_viral_load_status,
    viral_load_test_status,
    on_antiretroviral_therapy,
    viral_load_test_indication,
    antiretroviral_side_effects,
    anitiretroviral_adherence_level,
    date_of_reported_hiv_viral_load,
    date_viral_load_results_received,
    routine_viral_load_test_indication,
    targeted_viral_load_test_indication,
    dsd_category,
    tpt_start_date,
    tpt_completed_date,
    tpt_discontinued_date,
    tuberculosis_treatment_end_date
FROM RankedFollowups
WHERE rn = 1;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_art_latest_follow_up_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_art_latest_follow_up_update;


~-~-
CREATE PROCEDURE sp_fact_encounter_art_latest_follow_up_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_art_latest_follow_up_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_art_latest_follow_up_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_tx_new  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_tx_new;


~-~-
CREATE PROCEDURE sp_fact_encounter_tx_new()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_tx_new', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_tx_new', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_fact_encounter_tx_new_create();
CALL sp_fact_encounter_tx_new_insert();
CALL sp_fact_encounter_tx_new_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_tx_new_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_tx_new_create;


~-~-
CREATE PROCEDURE sp_fact_encounter_tx_new_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_tx_new_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_tx_new_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_fact_tx_new
(
    id                     INT AUTO_INCREMENT,
    client_id              INT NULL,
    weight_in_kg           DOUBLE,
    cd4_count              DOUBLE,
    current_who_hiv_stage  NVARCHAR(255),
    nutritional_status     NVARCHAR(255),
    tb_screening_result    NVARCHAR(255),
    enrollment_date        DATE,
    hiv_confirmed_date     DATE,
    art_start_date         DATE,
    days_difference        INT,
    followup_date          DATE,
    regimen                NVARCHAR(255),
    arv_dose_days          NVARCHAR(255),
    pregnancy_status       NVARCHAR(255),
    breast_feeding_status  NVARCHAR(255),
    follow_up_status       NVARCHAR(255),
    ti                     NVARCHAR(255),
    treatment_end_date     DATE,
    next_visit_date        DATE,
    latest_followup_date   DATE,
    latest_followup_status NVARCHAR(255),
    latest_regimen         NVARCHAR(255),
    latest_arv_dose_days   NVARCHAR(255),
    PRIMARY KEY (id)
);
CREATE INDEX mamba_fact_mamba_fact_tx_new_art_start_date_index ON mamba_fact_tx_new (art_start_date);
CREATE INDEX mamba_fact_mamba_fact_tx_new_client_id_index ON mamba_fact_tx_new (client_id);
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_tx_new_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_tx_new_insert;


~-~-
CREATE PROCEDURE sp_fact_encounter_tx_new_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_tx_new_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_tx_new_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
INSERT INTO mamba_fact_tx_new (client_id,
                               weight_in_kg,
                               cd4_count,
                               current_who_hiv_stage,
                               nutritional_status,
                               tb_screening_result,
                               enrollment_date,
                               hiv_confirmed_date,
                               art_start_date,
                               days_difference,
                               followup_date,
                               regimen,
                               arv_dose_days,
                               pregnancy_status,
                               breast_feeding_status,
                               follow_up_status,
                               ti,
                               treatment_end_date,
                               next_visit_date,
                               latest_followup_date,
                               latest_followup_status,
                               latest_regimen,
                               latest_arv_dose_days)
SELECT
    first_follow_up.client_id,
    MAX(first_follow_up.weight_in_kg) AS weight_in_kg,
    MAX(first_follow_up.cd4_count) AS cd4_count,
    MAX(first_follow_up.current_who_hiv_stage) AS current_who_hiv_stage,
    MAX(first_follow_up.nutritional_status) AS nutritional_status,
    MAX(first_follow_up.tb_screening_result) AS tb_screening_result,
    MAX(first_follow_up.enrollment_date) AS enrollment_date,
    MAX(first_follow_up.hiv_confirmed_date) AS hiv_confirmed_date,
    MAX(first_follow_up.art_start_date) AS art_start_date,
    MAX(first_follow_up.days_difference) AS days_difference,
    MAX(first_follow_up.followup_date) AS followup_date,
    MAX(first_follow_up.regimen) AS regimen,
    MAX(first_follow_up.arv_dose_days) AS arv_dose_days,
    MAX(first_follow_up.pregnancy_status) AS pregnancy_status,
    MAX(first_follow_up.breast_feeding_status) AS breast_feeding_status,
    MAX(first_follow_up.follow_up_status) AS follow_up_status,
    MAX(first_follow_up.ti) AS ti,
    MAX(latest_follow_up.treatment_end_date) AS treatment_end_date,
    MAX(latest_follow_up.next_visit_date) AS next_visit_date,
    MAX(latest_follow_up.followup_date) AS latest_followup_date,
    MAX(latest_follow_up.follow_up_status) AS latest_followup_status,
    MAX(latest_follow_up.regimen) AS latest_regimen,
    MAX(latest_follow_up.arv_dose_days) AS latest_arv_dose_days
FROM
    mamba_fact_art_first_follow_up first_follow_up
        JOIN
    mamba_fact_art_latest_follow_up latest_follow_up
    ON
        first_follow_up.client_id = latest_follow_up.client_id
GROUP BY
    first_follow_up.client_id;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_tx_new_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_tx_new_update;


~-~-
CREATE PROCEDURE sp_fact_encounter_tx_new_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_tx_new_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_tx_new_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_tx_new_query  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_fact_encounter_art_follow_up_tx_new_query;

~-~-
CREATE PROCEDURE sp_fact_encounter_art_follow_up_tx_new_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
SELECT dim_follow_up.patient_name,
       dim_follow_up.mrn,
       dim_follow_up.uan,
       dim_follow_up.current_age,
       dim_follow_up.sex,
       dim_follow_up.mobile_no,
       fact_tx_new.weight_in_kg,
       fact_tx_new.cd4_count,
       fact_tx_new.current_who_hiv_stage,
       fact_tx_new.nutritional_status,
       fact_tx_new.tb_screening_result,
       fact_tx_new.enrollment_date,
       fact_tx_new.hiv_confirmed_date,
       fact_tx_new.art_start_date,
       fact_tx_new.days_difference,
       fact_tx_new.followup_date,
       fact_tx_new.regimen,
       fact_tx_new.arv_dose_days,
       fact_tx_new.pregnancy_status,
       fact_tx_new.breast_feeding_status,
       fact_tx_new.follow_up_status,
       fact_tx_new.ti,
       fact_tx_new.treatment_end_date,
       fact_tx_new.next_visit_date,
       fact_tx_new.latest_followup_date,
       fact_tx_new.latest_followup_status,
       fact_tx_new.latest_regimen,
       fact_tx_new.latest_arv_dose_days
FROM mamba_fact_tx_new fact_tx_new
         INNER JOIN mamba_dim_client_art_follow_up as dim_follow_up
                    on fact_tx_new.client_id = dim_follow_up.client_id

WHERE fact_tx_new.art_start_date BETWEEN  CAST(REPORT_START_DATE as DATE) AND CAST(REPORT_END_DATE as DATE)
  and dim_follow_up.mrn is not null and fact_tx_new.follow_up_status in ('Alive', 'Restart');
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_dim_tx_new_datim_query  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_dim_tx_new_datim_query;

-- e.g. CALL sp_dim_tx_new_datim_query('2020-01-01', '2020-01-01', 0, 'unknown');
~-~-
CREATE PROCEDURE sp_dim_tx_new_datim_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN IS_COURSE_AGE_GROUP BOOLEAN, -- expected values: 0 for fine age group, 1 for coarse age group
    IN CD4_COUNT_GROUPAGE VARCHAR(255) -- expected values are: 'all', '>200', '<200', 'unknown'
)
BEGIN

    DECLARE cd4_count_condition VARCHAR(255);
    DECLARE age_group_cols VARCHAR(5000);

    SET session group_concat_max_len = 20000;

    IF CD4_COUNT_GROUPAGE = 'all' THEN
        SET cd4_count_condition = '1=1';
    ELSEIF CD4_COUNT_GROUPAGE = '>200' THEN
        SET cd4_count_condition = 'fact_art.cd4_count > 200';
    ELSEIF CD4_COUNT_GROUPAGE = '<200' THEN
        SET cd4_count_condition = 'fact_art.cd4_count < 200';
    ELSEIF CD4_COUNT_GROUPAGE = 'unknown' THEN
        SET cd4_count_condition = 'fact_art.cd4_count IS NULL';
    ELSE
        SET cd4_count_condition = '1=1';
    END IF;

    IF IS_COURSE_AGE_GROUP THEN
        SELECT GROUP_CONCAT(DISTINCT
                            CONCAT(
                                    'MAX(CASE WHEN coarse_age_group = ''',
                                    coarse_age_group,
                                    ''' THEN count ELSE 0 END) AS `',
                                    coarse_age_group, '`'
                            )
                            ORDER BY CAST(SUBSTRING_INDEX(coarse_age_group, '-', 1) AS UNSIGNED)
               )
        INTO age_group_cols
        FROM mamba_dim_client_art_follow_up;
    ELSE
        SELECT GROUP_CONCAT(DISTINCT
                            CONCAT(
                                    'MAX(CASE WHEN fine_age_group = ''',
                                    fine_age_group,
                                    ''' THEN count ELSE 0 END) AS `',
                                    fine_age_group, '`'
                            )
               )
        INTO age_group_cols
        FROM mamba_dim_client_art_follow_up;
    END IF;

    SET @sql = CONCAT('
        SELECT
          sex,
          ', age_group_cols, '
        FROM (
          SELECT
            sex,
            ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), ',
            COUNT(*) AS count
          FROM mamba_dim_client_art_follow_up dim_art
                INNER JOIN analysis_db.mamba_fact_tx_new fact_art
                           ON dim_art.client_id = fact_art.client_id
                WHERE fact_art.art_start_date BETWEEN ? AND ?
                  AND ', cd4_count_condition, '
                  AND dim_art.mrn is not null and fact_art.follow_up_status in (''Alive'', ''Restart'')
          GROUP BY sex, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''MALE'' AS sex UNION SELECT ''FEMALE'') AS genders
        USING (sex)
        GROUP BY sex
        ');

    PREPARE stmt FROM @sql;
    SET @start_date = REPORT_START_DATE;
    SET @end_date = REPORT_END_DATE;
    EXECUTE stmt USING @start_date, @end_date;
    DEALLOCATE PREPARE stmt;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_tx_curr_query  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_fact_encounter_art_follow_up_tx_curr_query;

~-~-
CREATE PROCEDURE sp_fact_encounter_art_follow_up_tx_curr_query(IN REPORT_END_DATE DATE)
BEGIN
    WITH LatestFollowUp AS (
        SELECT
            client_id,
            hiv_confirmed_date,
            art_start_date,
            followup_date,
            enrollment_date,
            weight_in_kg,
            pregnancy_status,
            regimen,
            arv_dose_days,
            follow_up_status,
            anitiretroviral_adherence_level,
            next_visit_date,
            dsd_category,
            tpt_start_date,
            tpt_completed_date,
            tpt_discontinued_date,
            tuberculosis_treatment_end_date,
            date_viral_load_results_received,
            viral_load_test_status,
            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY followup_date DESC) AS rn_desc
        FROM mamba_fact_art_follow_up
        WHERE followup_date <= CAST(REPORT_END_DATE AS DATE)
          AND art_start_date <= CAST(REPORT_END_DATE AS DATE)
          AND regimen IS NOT NULL
    )
    SELECT
        lf.client_id,
        dim.patient_name,
        dim.mrn,
        dim.uan,
        dim.current_age,
        fn_mamba_age_calculator(dim.date_of_birth, lf.enrollment_date) AS age_at_enrollment,
        dim.sex,
        dim.mobile_no,
        lf.hiv_confirmed_date,
        lf.art_start_date,
        lf.followup_date,
        lf.weight_in_kg,
        lf.pregnancy_status,
        lf.regimen,
        lf.arv_dose_days,
        lf.follow_up_status,
        lf.anitiretroviral_adherence_level,
        lf.next_visit_date,
        lf.dsd_category,
        lf.tpt_start_date,
        lf.tpt_completed_date,
        lf.tpt_discontinued_date,
        lf.tuberculosis_treatment_end_date,
        lf.date_viral_load_results_received,
        lf.viral_load_test_status,
        NOW()
    FROM LatestFollowUp lf
             JOIN mamba_dim_client_art_follow_up dim ON lf.client_id = dim.client_id
    WHERE lf.rn_desc = 1
      AND lf.follow_up_status = 'Alive';
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_tx_curr_analytics_query  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_fact_encounter_tx_curr_analytics_query;

-- e.g. CALL sp_fact_encounter_tx_curr_analytics_query('2020-01-01', '2020-01-01');
~-~-
CREATE PROCEDURE sp_fact_encounter_tx_curr_analytics_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN
WITH LatestFollowUp AS (
    SELECT
        client_id,
        hiv_confirmed_date,
        art_start_date,
        followup_date,
        enrollment_date,
        weight_in_kg,
        pregnancy_status,
        regimen,
        arv_dose_days,
        treatment_end_date,
        follow_up_status,
        anitiretroviral_adherence_level,
        next_visit_date,
        dsd_category,
        tpt_start_date,
        tpt_completed_date,
        tpt_discontinued_date,
        tuberculosis_treatment_end_date,
        date_viral_load_results_received,
        viral_load_test_status,
        status,
        ROW_NUMBER() OVER (
            PARTITION BY client_id
            ORDER BY
                followup_date DESC
        ) AS rn_desc
    FROM
        (
            SELECT
                client_id,
                hiv_confirmed_date,
                art_start_date,
                followup_date,
                enrollment_date,
                weight_in_kg,
                pregnancy_status,
                regimen,
                arv_dose_days,
                treatment_end_date,
                follow_up_status,
                anitiretroviral_adherence_level,
                next_visit_date,
                dsd_category,
                tpt_start_date,
                tpt_completed_date,
                tpt_discontinued_date,
                tuberculosis_treatment_end_date,
                date_viral_load_results_received,
                viral_load_test_status,
                'current' as status
            FROM
                mamba_fact_art_follow_up
            WHERE
                (
                    followup_date <= REPORT_END_DATE
                    AND art_start_date <= REPORT_END_DATE
                    AND treatment_end_date >= REPORT_END_DATE
                )
                AND regimen IS NOT NULL
            UNION
            ALL
            SELECT
                client_id,
                hiv_confirmed_date,
                art_start_date,
                followup_date,
                enrollment_date,
                weight_in_kg,
                pregnancy_status,
                regimen,
                arv_dose_days,
                treatment_end_date,
                follow_up_status,
                anitiretroviral_adherence_level,
                next_visit_date,
                dsd_category,
                tpt_start_date,
                tpt_completed_date,
                tpt_discontinued_date,
                tuberculosis_treatment_end_date,
                date_viral_load_results_received,
                viral_load_test_status,
                'previous' as status
            FROM
                mamba_fact_art_follow_up
            WHERE
                (
                    followup_date <= REPORT_START_DATE
                    AND art_start_date <= REPORT_START_DATE
                    AND treatment_end_date >= REPORT_START_DATE
                )
                AND regimen IS NOT NULL
        ) AS combined
),
StatusSummary AS (
    SELECT
        client_id,
        MAX(status) AS max_status,
        COUNT(DISTINCT status) AS status_count
    FROM
        LatestFollowUp
    GROUP BY
        client_id
)
SELECT
    lf.client_id,
    dim.patient_name,
    dim.mrn,
    dim.uan,
    dim.current_age,
    fn_mamba_age_calculator(dim.date_of_birth, lf.enrollment_date) AS age_at_enrollment,
    dim.sex,
    dim.mobile_no,
    lf.hiv_confirmed_date,
    lf.art_start_date,
    lf.followup_date,
    lf.weight_in_kg,
    lf.pregnancy_status,
    lf.regimen,
    lf.arv_dose_days,
    lf.follow_up_status,
    lf.anitiretroviral_adherence_level,
    lf.next_visit_date,
    lf.dsd_category,
    lf.tpt_start_date,
    lf.tpt_completed_date,
    lf.tpt_discontinued_date,
    lf.tuberculosis_treatment_end_date,
    lf.date_viral_load_results_received,
    lf.viral_load_test_status,
    CASE
        WHEN ss.status_count = 2 THEN 'STILL ON CARE'
        WHEN ss.max_status = 'current' THEN 'New'
        WHEN ss.max_status = 'previous' THEN 'Previous'
    END AS status,
    NOW()
FROM
    LatestFollowUp lf
    JOIN StatusSummary ss ON lf.client_id = ss.client_id
    JOIN mamba_dim_client_art_follow_up dim ON lf.client_id = dim.client_id
WHERE
    lf.rn_desc = 1
    AND (
        lf.follow_up_status = 'Alive'
        OR lf.follow_up_status = 'Restart');
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_data_processing_derived_art_follow_up  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_data_processing_derived_art_follow_up;


~-~-
CREATE PROCEDURE sp_data_processing_derived_art_follow_up()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_data_processing_derived_art_follow_up', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_data_processing_derived_art_follow_up', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_dim_client_art_follow_up();
CALL sp_fact_encounter_art_follow_up();
CALL sp_fact_encounter_art_first_follow_up();
CALL sp_fact_encounter_art_latest_follow_up();
CALL sp_fact_encounter_tx_new();


-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_vl_eligibility_query  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_fact_vl_eligibility_query;

~-~-
CREATE PROCEDURE  sp_fact_vl_eligibility_query(
    IN REPORT_END_DATE DATE
)
BEGIN

    DECLARE end_date_str VARCHAR(20);

    SET end_date_str = DATE_FORMAT(REPORT_END_DATE, '%Y-%m-%d');

    WITH Follow_up AS (select follow_up_date_followup_            as follow_up_date,
                              follow_up.client_id                 as patient_id,
                              follow_up.encounter_id              as encounter_id,
                              date_of_reported_hiv_viral_load     as viral_load_sent_date,
                              date_viral_load_results_received    as viral_load_performed_date,
                              regimen_change,
                              follow_up_status,
                              art_antiretroviral_start_date       as art_start_date,
                              viral_load_test_status              as viral_load_status,
                              hiv_viral_load                      as viral_load_count,
                              weight_text_                          as weight,
                              diagnosis_date               as date_hiv_confirmed,
                              antiretroviral_art_dispensed_dose_i as arv_dispensed_dose_days,
                              regimen,
                              next_visit_date                   as next_visit_date,
                              routine_viral_load_test_indication  as routine_viral_load,
                              date_of_last_menstrual_period_lmp_  as lmp,
                              currently_breastfeeding_child       as BreastFeeding,
                              pregnancy_status                    as IsPregnant,
                              treatment_end_date,
                              mrn,
                              uan
                       from mamba_flat_encounter_follow_up follow_up
                                inner join mamba_flat_encounter_follow_up_1 follow_up_1
                                           on follow_up.encounter_id = follow_up_1.encounter_id
                                left join mamba_dim_client_art_follow_up dim_client
                                          on follow_up.client_id = dim_client.client_id
                       WHERE follow_up_1.follow_up_date_followup_ <= REPORT_END_DATE),
         VLSentdate AS (SELECT Follow_up.patient_id, MAX(viral_load_sent_date) AS VL_Sent_Date
                        FROM Follow_up
                        where
                            viral_load_sent_date is not null
                        GROUP BY Follow_up.patient_id),
         SwitchSubdate AS (SELECT Follow_up.patient_id, MAX(follow_up_date) AS SwitchDate
                           FROM Follow_up
                           WHERE (regimen_change is not null and regimen_change = 'Regimen switch type')
                           GROUP BY Follow_up.patient_id),
         VLPerfdate1 AS (SELECT Follow_up.patient_id, MAX(viral_load_performed_date) AS viral_load_perform_date
                         FROM Follow_up
                         WHERE follow_up_status IS NOT NULL
                           AND art_start_date is not null
                           and viral_load_performed_date is not null
                         GROUP BY Follow_up.patient_id) ,
         VLPerfdate2 AS (SELECT MAX(encounter_id) as encounter_id
                         FROM Follow_up
                                  INNER JOIN VLPerfdate1 on Follow_up.patient_id = VLPerfdate1.patient_id
                             AND Follow_up.viral_load_performed_date = VLPerfdate1.viral_load_perform_date
                         GROUP BY Follow_up.patient_id, Follow_up.viral_load_performed_date),
         VLPerfdate3 AS (SELECT
                             Follow_up.encounter_id,
                             Follow_up.patient_id,
                             viral_load_performed_date,
                             Follow_up.viral_load_status,
                             IF(Follow_up.viral_load_count REGEXP '^[0-9]+(\.[0-9]+)?$' > 0,
                                CAST(viral_load_count AS DECIMAL(12, 2)), NULL) AS viral_load_count,
                             CASE
                                 WHEN VLSentdate.VL_Sent_Date IS NOT NULL AND
                                      VLSentdate.VL_Sent_Date != '1900-01-01 00:00:00.000'
                                     THEN VLSentdate.VL_Sent_Date
                                 WHEN Follow_up.viral_load_performed_date IS NOT NULL
                                     AND Follow_up.viral_load_performed_date != '1900-01-01 00:00:00.000'
                                     THEN Follow_up.viral_load_performed_date
                                 ELSE '1900-01-01 00:00:00.000'
                                 END AS viral_load_ref_date
                         FROM Follow_up
                                  INNER JOIN VLPerfdate2 ON Follow_up.encounter_id = VLPerfdate2.encounter_id
                                  LEFT JOIN VLSentdate ON Follow_up.patient_id = VLSentdate.patient_id),
         temp1 AS (SELECT patient_id, MAX(follow_up_date) AS FollowupDate
                   FROM Follow_up
                   WHERE follow_up_status IS NOT NULL
                     and art_start_date
                   GROUP BY patient_id),
         temp2 AS (SELECT MAX(Follow_up.encounter_id) as encounter_id
                   FROM Follow_up
                            INNER JOIN temp1 on Follow_up.patient_id = temp1.patient_id AND
                                                Follow_up.follow_up_date = temp1.FollowupDate
                   GROUP BY Follow_up.patient_id, Follow_up.follow_up_date),
         temp3 AS (SELECT dim_clinet.mrn,-- reg.PatientId as mrn,
                          dim_clinet.patient_name,-- reg.fullname,
                          dim_clinet.mobile_no,-- address.MobilePhoneNumber,
                          'PhoneNumber',-- address.PhoneNumber,
                          timestampdiff(YEAR, dim_clinet.date_of_birth, curdate()) AS age,-- dbo.fn_Age(reg.DateOfBirth, CURDATE()) AS age,
                          dim_clinet.sex,-- reg.Sex,
                          Follow_up.encounter_id,-- f_case.Id,
                          Follow_up.patient_id,-- f_case.PatientId,
                          dim_clinet.uan,-- f_case.UniqueArtNumber,
                          weight,-- f_case.[Weight],
                          date_hiv_confirmed,-- f_case.date_hiv_confirmed,
                          art_start_date,-- f_case.art_start_date,
                          follow_up_date,-- f_case.FollowUpDate,
                          IsPregnant, -- f_case.IsPregnant,
                          arv_dispensed_dose_days,-- f_case.ARVDispendsedDose,
                          regimen,-- f_case.art_dose,
                          next_visit_date,-- f_case.next_visit_date,
                          follow_up_status,-- f_case.follow_up_status,
                          treatment_end_date, -- 'f_case.art_dose_End',
                          vlperfdate.viral_load_performed_date,-- vlperfdate.viral_load_perform_date,
                          vlperfdate.viral_load_status,-- vlperfdate.viral_load_status,
                          vlperfdate.viral_load_count,-- vlperfdate.viral_load_count,
                          vlsentdate.VL_Sent_Date,-- vlsentdate.VL_Sent_Date,
                          vlperfdate.viral_load_ref_date,-- vlperfdate.viral_load_ref_date,
                          sub_switch_date.SwitchDate                               as date_regimen_change,
                          Follow_up.follow_up_date                                 as FollowUpDate,
                          CASE
                              WHEN
                                  ((Follow_up.follow_up_status = 'Restart medication'))
                                  THEN DATE_ADD(Follow_up.follow_up_date, INTERVAL 91 DAY)
                              WHEN
                                  (
                                      (routine_viral_load is not null) -- Routine viral load test indication 'Routine viral load test indication'
                                      )
                                  THEN DATE_ADD(Follow_up.follow_up_date, INTERVAL 91 DAY)
                              WHEN
                                  (
                                      (routine_viral_load is null)
                                      )
                                  THEN DATE_ADD(Follow_up.follow_up_date, INTERVAL 181 DAY)
                              WHEN (
                                  (SwitchDate IS not NULL and SwitchDate != '1900-01-01 00:00:00.000')
                                  )
                                  THEN date_add(SwitchDate, INTERVAL 181 DAY)

                              WHEN (
                                  (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) <= 12)
                                      AND (Follow_up.IsPregnant is null OR Follow_up.IsPregnant = 'No')
                                      AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date IS NULL OR
                                           vlperfdate.viral_load_ref_date = '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_count IS NULL))
                                  THEN DATE_ADD(Follow_up.art_start_date, INTERVAL 181 DAY)

                              WHEN ((TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) <= 12)
                                  AND (Follow_up.IsPregnant is null OR Follow_up.IsPregnant = 'No')
                                  AND (Follow_up.BreastFeeding is null OR Follow_up.BreastFeeding = 'No')
                                  AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000' or
                                       (SwitchDate is not null and SwitchDate < vlperfdate.viral_load_ref_date))
                                  AND (vlperfdate.viral_load_ref_date IS NOT NULL AND
                                       vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                  AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51))
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 181 DAY)

                              WHEN ((TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 12)
                                  AND (Follow_up.IsPregnant is null OR Follow_up.IsPregnant = 'No')
                                  AND (Follow_up.BreastFeeding is null OR Follow_up.BreastFeeding = 'No')
                                  AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000' or
                                       (SwitchDate is not null and SwitchDate <= vlperfdate.viral_load_ref_date))
                                  AND (vlperfdate.viral_load_ref_date > '1900-01-01' and
                                       vlperfdate.viral_load_ref_date IS NOT NULL)
                                  AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51))
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 365 DAY)

                              WHEN ((TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 12)
                                  AND (Follow_up.IsPregnant is null OR Follow_up.IsPregnant = 'No')
                                  AND (Follow_up.BreastFeeding is null OR Follow_up.BreastFeeding = 'No')
                                  AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000')
                                  AND (vlperfdate.viral_load_ref_date = '1900-01-01' or
                                       vlperfdate.viral_load_ref_date IS NULL)
                                  AND (vlperfdate.viral_load_count IS NULL))
                                  THEN REPORT_END_DATE

                              WHEN (
                                  (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) <= 3)
                                      AND (Follow_up.IsPregnant = 'Yes')
                                      AND (Follow_up.BreastFeeding is null or Follow_up.BreastFeeding = 'No')
                                      AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000')
                                      AND
                                  (vlperfdate.viral_load_ref_date = '1900-01-01' or vlperfdate.viral_load_ref_date IS NULL)
                                      AND (vlperfdate.viral_load_count IS NULL)
                                  )
                                  THEN DATE_ADD(Follow_up.art_start_date, INTERVAL 91 DAY)

                              WHEN (
                                  (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 3)
                                      AND
                                  (TIMESTAMPDIFF(WEEK, REPORT_END_DATE, DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY)) < 34 and
                                   Follow_up.lmp is not null)
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51)
                                      AND (Abs(TIMESTAMPDIFF(MONTH, vlperfdate.viral_load_ref_date, REPORT_END_DATE)) >= 3)
                                      AND (Follow_up.IsPregnant = 'Yes')
                                  )
                                  THEN REPORT_END_DATE

                              WHEN (
                                  (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 3)
                                      AND (Follow_up.lmp is not null)
                                      AND (TIMESTAMPDIFF(WEEK, REPORT_END_DATE, DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY)) < 34)
                                      AND (vlperfdate.viral_load_ref_date IS NULL or
                                           vlperfdate.viral_load_ref_date = '1900-01-01 00:00:00.000')
                                      AND (Follow_up.IsPregnant = 'Yes')
                                  )
                                  THEN REPORT_END_DATE

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 3)
                                      AND (Follow_up.lmp is null or Follow_up.lmp = '1900-01-01 00:00:00.000')
                                      AND
                                  (TIMESTAMPDIFF(WEEK, REPORT_END_DATE, DATE_ADD(Follow_up.follow_up_date, INTERVAL 280 DAY)) <
                                   34)
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51)
                                      AND (TIMESTAMPDIFF(MONTH, vlperfdate.viral_load_ref_date, REPORT_END_DATE) >= 3)
                                  )
                                  THEN REPORT_END_DATE


                              WHEN (
                                  (Follow_up.lmp is not null and Follow_up.lmp != '1900-01-01 00:00:00.000')
                                      AND (TIMESTAMPDIFF(WEEK, REPORT_END_DATE, DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY)) < 34)
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (Follow_up.IsPregnant = 'Yes')
                                  )
                                  THEN DATE_ADD(Follow_up.lmp, INTERVAL 239 DAY)

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (Follow_up.lmp is not null and Follow_up.lmp != '1900-01-01 00:00:00.000')
                                      AND (DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY) <= REPORT_END_DATE)
                                      AND (vlperfdate.viral_load_ref_date IS NULL OR
                                           vlperfdate.viral_load_ref_date = '1900-01-01 00:00:00.000' or
                                           vlperfdate.viral_load_ref_date < DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY))
                                  )
                                  THEN DATE_ADD(Follow_up.lmp, INTERVAL 371 DAY)

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (Follow_up.lmp is null)
                                      AND (vlperfdate.viral_load_ref_date IS NULL or
                                           vlperfdate.viral_load_ref_date < Follow_up.follow_up_date)
                                  )
                                  THEN DATE_ADD(Follow_up.follow_up_date, INTERVAL 371 DAY)

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (Follow_up.lmp is not null and Follow_up.lmp != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date > (Follow_up.lmp))
                                  )
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 181 DAY)

                              WHEN (
                                  (Follow_up.BreastFeeding = 'Yes')
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date < Follow_up.follow_up_date and
                                           TIMESTAMPDIFF(DAY, vlperfdate.viral_load_ref_date, REPORT_END_DATE) > 180)
                                  )
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 181 DAY)

                              WHEN (
                                  (vlperfdate.viral_load_ref_date IS not NULL and
                                   vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND
                                  (vlperfdate.viral_load_count IS not NULL and vlperfdate.viral_load_count <= 1000 and
                                   vlperfdate.viral_load_count >= 51)
                                      AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000' or
                                           (SwitchDate is not null and SwitchDate < vlperfdate.viral_load_ref_date))
                                  )
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 91 DAY)

                              WHEN (
                                  (vlperfdate.viral_load_ref_date IS not NULL and
                                   vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_count IS not NULL and vlperfdate.viral_load_count > 1000)
                                      AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000' or
                                           (SwitchDate is not null and SwitchDate < vlperfdate.viral_load_ref_date))
                                  )
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 91 DAY)

                              WHEN (
                                  (vlperfdate.viral_load_ref_date IS NULL or
                                   vlperfdate.viral_load_ref_date = '1900-01-01 00:00:00.000')
                                      AND (SwitchDate IS not NULL and SwitchDate != '1900-01-01 00:00:00.000')
                                  )
                                  THEN DATE_ADD(SwitchDate, INTERVAL 181 DAY)

                              WHEN (
                                  (vlperfdate.viral_load_ref_date IS not NULL and
                                   vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (SwitchDate IS not NULL and SwitchDate != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date <= SwitchDate)
                                  )
                                  THEN DATE_ADD(SwitchDate, INTERVAL 181 DAY)

                              When (
                                  (Follow_up.BreastFeeding = 'Yes')
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (TIMESTAMPDIFF(DAY, vlperfdate.viral_load_ref_date, REPORT_END_DATE) >= 90)
                                  )
                                  THEN REPORT_END_DATE

                              When (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (TIMESTAMPDIFF(DAY, vlperfdate.viral_load_ref_date, REPORT_END_DATE) >= 90)
                                  )
                                  THEN REPORT_END_DATE

                              ELSE REPORT_END_DATE End                                   AS eligiblityDate,

                          CASE
                              WHEN (
                                  (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) <= 12)
                                      AND (Follow_up.IsPregnant is null OR Follow_up.IsPregnant = 'No')
                                      AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date IS NULL OR
                                           vlperfdate.viral_load_ref_date = '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_count IS NULL))
                                  THEN '1st VL after 6 months'

                              WHEN ((TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) <= 12)
                                  AND (Follow_up.IsPregnant is null OR Follow_up.IsPregnant = 'No')
                                  AND (Follow_up.BreastFeeding is null OR Follow_up.BreastFeeding = 'No')
                                  AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000' or
                                       (SwitchDate is not null and SwitchDate < vlperfdate.viral_load_ref_date))
                                  AND (vlperfdate.viral_load_ref_date IS NOT NULL AND
                                       vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                  AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51))
                                  THEN '2nd VL at 12 months'

                              WHEN ((TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 12)
                                  AND (Follow_up.IsPregnant is null OR Follow_up.IsPregnant = 'No')
                                  AND (Follow_up.BreastFeeding is null OR Follow_up.BreastFeeding = 'No')
                                  AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000' or
                                       (SwitchDate is not null and SwitchDate <= vlperfdate.viral_load_ref_date))
                                  AND (vlperfdate.viral_load_ref_date > '1900-01-01' or
                                       vlperfdate.viral_load_ref_date IS NOT NULL)
                                  AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51))
                                  THEN 'Annual VL 1'

                              WHEN ((TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 12)
                                  AND (Follow_up.IsPregnant is null OR Follow_up.IsPregnant = 'No')
                                  AND (Follow_up.BreastFeeding is null OR Follow_up.BreastFeeding = 'No')
                                  AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000')
                                  AND (vlperfdate.viral_load_ref_date = '1900-01-01' or
                                       vlperfdate.viral_load_ref_date IS NULL)
                                  AND (vlperfdate.viral_load_count IS NULL))
                                  THEN 'Annual VL 2'

                              WHEN (
                                  (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) <= 3)
                                      AND (Follow_up.IsPregnant = 'Yes')
                                      AND (Follow_up.BreastFeeding is null or Follow_up.BreastFeeding = 'No')
                                      AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000')
                                      AND
                                  (vlperfdate.viral_load_ref_date = '1900-01-01' or vlperfdate.viral_load_ref_date IS NULL)
                                      AND (vlperfdate.viral_load_count IS NULL)
                                  )
                                  THEN '1st VL at 3 months(Pregnant)'

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 3)
                                      AND (Follow_up.lmp is not null)
                                      AND (TIMESTAMPDIFF(WEEK, REPORT_END_DATE, DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY)) < 34)
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51)
                                      AND (TIMESTAMPDIFF(MONTH, vlperfdate.viral_load_ref_date, REPORT_END_DATE) >= 3)
                                  )
                                  THEN 'VL at 1st ANC 1'

                              WHEN (
                                  (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 3)
                                      and (Follow_up.lmp is not null)
                                      AND (TIMESTAMPDIFF(WEEK, REPORT_END_DATE, DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY)) < 34)
                                      AND (vlperfdate.viral_load_ref_date IS NULL or
                                           vlperfdate.viral_load_ref_date = '1900-01-01 00:00:00.000')
                                      AND (Follow_up.IsPregnant = 'Yes')
                                  )
                                  THEN 'VL at 1st ANC 2'

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 3)
                                      AND (Follow_up.lmp is null or Follow_up.lmp = '1900-01-01 00:00:00.000')
                                      AND
                                  (TIMESTAMPDIFF(WEEK, REPORT_END_DATE, DATE_ADD(Follow_up.follow_up_date, INTERVAL 280 DAY)) <
                                   34)
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51)
                                      AND (TIMESTAMPDIFF(MONTH, vlperfdate.viral_load_ref_date, REPORT_END_DATE) >= 3)
                                  )
                                  THEN 'VL at 1st ANC 3'

                              WHEN (
                                  (Follow_up.lmp is not null and Follow_up.lmp != '1900-01-01 00:00:00.000')
                                      AND (TIMESTAMPDIFF(WEEK, REPORT_END_DATE, DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY)) < 34)
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (Follow_up.IsPregnant = 'Yes')
                                  )
                                  THEN 'VL at 34-36 weeks gestation'

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (Follow_up.lmp is not null and Follow_up.lmp != '1900-01-01 00:00:00.000')
                                      AND (DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY) <= REPORT_END_DATE)
                                      AND (vlperfdate.viral_load_ref_date IS NULL OR
                                           vlperfdate.viral_load_ref_date = '1900-01-01 00:00:00.000' or
                                           vlperfdate.viral_load_ref_date < DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY))
                                  )
                                  THEN '3 months after delivery 1'

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (Follow_up.lmp is null)
                                      AND (vlperfdate.viral_load_ref_date IS NULL or
                                           vlperfdate.viral_load_ref_date < Follow_up.follow_up_date)
                                  )
                                  THEN '3 months after delivery 2'

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (Follow_up.lmp is not null and Follow_up.lmp != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date > (Follow_up.lmp))
                                  )
                                  THEN '6 months after 1st VL at PNC'

                              WHEN (
                                  (Follow_up.BreastFeeding = 'Yes')
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date < Follow_up.follow_up_date and
                                           TIMESTAMPDIFF(DAY, vlperfdate.viral_load_ref_date, REPORT_END_DATE) > 180)
                                  )
                                  THEN 'Every 6 months untill MTCT ends'

                              WHEN (
                                  (vlperfdate.viral_load_ref_date IS not NULL and
                                   vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND
                                  (vlperfdate.viral_load_count IS not NULL and vlperfdate.viral_load_count <= 1000 and
                                   vlperfdate.viral_load_count >= 51)
                                      AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000' or
                                           (SwitchDate is not null and SwitchDate < vlperfdate.viral_load_ref_date))
                                  )
                                  THEN 'Repeat VL test'

                              WHEN (
                                  (vlperfdate.viral_load_ref_date IS not NULL and
                                   vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_count IS not NULL and vlperfdate.viral_load_count > 1000)
                                      AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000' or
                                           (SwitchDate is not null and SwitchDate < vlperfdate.viral_load_ref_date))
                                  )
                                  THEN 'Confirmatory VL'

                              WHEN (
                                  (vlperfdate.viral_load_ref_date IS NULL or
                                   vlperfdate.viral_load_ref_date = '1900-01-01 00:00:00.000')
                                      AND (SwitchDate IS not NULL and SwitchDate != '1900-01-01 00:00:00.000')
                                  )
                                  THEN '1st VL at 6 months post regimen change/switch 1'

                              WHEN (
                                  (vlperfdate.viral_load_ref_date IS not NULL and
                                   vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (SwitchDate IS not NULL and SwitchDate != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date <= SwitchDate)
                                  )
                                  THEN '1st VL at 6 months post regimen change/switch 2'

                              When (
                                  (Follow_up.BreastFeeding = 'Yes')
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (TIMESTAMPDIFF(DAY, vlperfdate.viral_load_ref_date, REPORT_END_DATE) >= 90)
                                  )
                                  THEN 'Other 1'

                              When (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (TIMESTAMPDIFF(DAY, vlperfdate.viral_load_ref_date, REPORT_END_DATE) >= 90)
                                  )
                                  THEN 'Other 2'


                              ELSE 'Other 3' End                                   AS vl_key,

                          CASE
                              WHEN (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) <= 12)
                                  THEN (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE))
                              ELSE (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE))
                              END                                                  AS datediffer,

                          CASE
                              WHEN 1 = 1
                                  THEN DATE_ADD(Follow_up.art_start_date, INTERVAL
                                                365 * TIMESTAMPDIFF(YEAR, Follow_up.art_start_date, REPORT_END_DATE) DAY)
                              ELSE DATE_ADD(Follow_up.art_start_date, INTERVAL
                                            365 * TIMESTAMPDIFF(YEAR, Follow_up.art_start_date, REPORT_END_DATE) DAY)
                              END                                                  AS datesecond,
                          Follow_up.BreastFeeding,
                          Follow_up.lmp
                   FROM Follow_up
                            INNER JOIN temp2 ON Follow_up.encounter_id = temp2.encounter_id
                            LEFT JOIN VLPerfdate3 as vlperfdate ON vlperfdate.patient_id = Follow_up.patient_id
                            LEFT join VLSentdate as vlsentdate ON vlsentdate.patient_id = Follow_up.patient_id
                            LEFT join SwitchSubdate as sub_switch_date ON sub_switch_date.patient_id = Follow_up.patient_id
                            INNER join mamba_dim_client_art_follow_up dim_clinet
                                       on Follow_up.patient_id = dim_clinet.client_id),

         temp4 as (SELECT temp3.mrn,
                          temp3.patient_name,
                          temp3.uan,
                          temp3.mobile_no,
                          ' ',
                          temp3.age,
                          temp3.sex,
                          temp3.weight,
                          temp3.date_hiv_confirmed date_hiv_confirmed,
                          temp3.art_start_date      as art_start_date,
                          temp3.follow_up_date      as follow_up_date,
                          IsPregnant,
                          temp3.regimen                                                   AS ARVDispendsedDose,
                          arv_dispensed_dose_days,
                          temp3.next_visit_date     as next_visit_date,
                          temp3.follow_up_status,
                          treatment_end_date        as treatment_end_date,
                          viral_load_performed_date as viral_load_performed_date,
                          viral_load_status,
                          viral_load_count,
                          VL_Sent_Date              as VL_Sent_Date,
                          viral_load_ref_date       as viral_load_ref_date,
                          date_regimen_change       as date_regimen_change,
                          eligiblityDate            as eligiblityDate,
                          vl_key,
                          CASE
                              WHEN IsPregnant = 'Yes' or BreastFeeding = 'Yes' THEN 'Yes'
                              ELSE 'No' END                                               AS PMTCT_ART
                   FROM temp3
             -- WHERE follow_up_status != 'Dead'
             -- and eligiblityDate between @DateFrom and @DateTo
         )select
              mrn, patient_name as fullname, uan as UniqueArtNumber, mobile_no as MobilePhoneNumber, '' PhoneNumber,  age,  sex, weight as Weight,  date_hiv_confirmed,
              art_start_date, follow_up_date as FollowUpDate,  IsPregnant,  ARVDispendsedDose, arv_dispensed_dose_days as art_dose,  next_visit_date,  follow_up_status, treatment_end_date as art_dose_End,
              viral_load_performed_date as viral_load_perform_date,  viral_load_status,  viral_load_count, VL_Sent_Date as viral_load_sent_date,  viral_load_ref_date,
              date_regimen_change,    eligiblityDate,    vl_key,    PMTCT_ART
    from temp4;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_cxca_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DELIMITER
//

DROP PROCEDURE IF EXISTS sp_fact_cxca_query;

~-~-
CREATE PROCEDURE sp_fact_cxca_query()
BEGIN

-- SMARTCARE                       -    OPENMRS                                                                  MAMBA
-- hpv_dna_result_received_date    -    999,HPV DNA result received date                                 -  hpv_dna_result_received_date             - 510f2a47-3761-4903-b7eb-8ea389cecfe9
-- ccs_hpv_result                  -    2110,HPV DNA screening result                                    -  hpv_dna_screening_result                 - 8ecc6d15-26dd-4840-8667-a517a93bea5f
-- date_via_result                 -    1862,Date Visual Inspection of the Cervix with Acetic Acid       -  date_visual_inspection_of_the_cervi      - f46c7ed3-65c3-451c-a8e1-4c615f795db1
-- ccs_via_result                  -    922,VIA screening result                                         -  via_screening_result                     - ff6b60e4-7310-4ddc-98ce-a2910c32a7a0
-- cytology_result_received_date   -    993,Date cytology result received                                -  date_cytology_result_received            - f0892f21-406c-446b-abd5-bb62f3ea2387
-- cytology_result                 -    440,Cytology result * No Data                                    -  cytology_result                          - d0e78bd5-3e85-4651-a369-3e19870f0ea0
-- ccs_treat_received_date         -    1348, treatment_start_date                                       -  treatment_start_date                     - 163526AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
-- colposcopy_exam_finding         -    1858,Colposcopy of cervix findings                               -  colposcopy_of_cervix_findings            - 93bf2a3e-1675-44c9-b7ee-b8ba9cb32b22
-- ccs_next_date                   -    964,Next follow up screening date                                -  next_follow_up_screening_date            - 4ce065b6-aecb-46a3-b60b-41bc5dc8022f
-- ccs_screendoneyes               -    1871,Cervical cancer screening status                            -  cervical_cancer_screening_status         - 01c546b4-e08a-4c0c-82ef-d387cab6bbbf
WITH Follow_up AS (SELECT follow_up_date_followup_                                         AS follow_up_date,
                          follow_up.client_id                                              AS patient_id,
                          follow_up.encounter_id                                           AS encounter_id,
                          follow_up_status,
                          next_follow_up_screening_date                                    AS ccs_next_date,
                          cervical_cancer_screening_status                                 AS screening_status,
                          hpv_dna_result_received_date,
                          hpv_dna_screening_result                                         AS ccs_hpv_result,
                          cytology_done_                                                   AS cytology_result,
                          date_cytology_result_received                                    AS cytology_result_received_date,
                          via_screening_result                                             AS ccs_via_result,
                          via_done_,
                          date_visual_inspection_of_the_cervi                              AS date_via_result,
                          treatment_start_date                                             AS ccs_treat_received_date,
                          colposcopy_of_cervix_findings                                    AS colposcopy_exam_finding,
                          sex,
                          CEILING(TIMESTAMPDIFF(MONTH , date_of_birth, '2024-01-01') / 12) AS age,
                          mrn,
                          uan,
                          patient_name                                                     as NAME,
                          ''                                                               as phonenumber,
                          mobile_no                                                        as mobilephonenumber,
                          art_antiretroviral_start_date                                    AS art_start_date,
                          regimen,
                          adherence,
                          next_visit_date
                   FROM mamba_flat_encounter_follow_up follow_up
                            INNER JOIN
                        mamba_flat_encounter_follow_up_1 follow_up_1
                        ON follow_up.encounter_id = follow_up_1.encounter_id
                            LEFT JOIN
                        mamba_dim_client_art_follow_up dim_client
                        ON follow_up.client_id = dim_client.client_id
                   WHERE follow_up_1.follow_up_date_followup_ <= '2024-01-01'
                     AND follow_up_status IS NOT NULL),

     latest_follow_up_status as (SELECT patient_id,
                                        Follow_up.follow_up_status,
                                        ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY follow_up_date DESC, encounter_id DESC) AS rn
                                 from Follow_up),
     cervical_screening_performed AS (SELECT MAX(follow_up_date)    AS max_follow_up_date,
                                             Follow_up.patient_id   as max_patient_id,
                                             Follow_up.encounter_id as max_encounter_id,
                                             ROW_NUMBER()              OVER (PARTITION BY Follow_up.patient_id ORDER BY follow_up_date DESC, Follow_up.encounter_id DESC) AS rn
                                      FROM Follow_up
                                               inner join latest_follow_up_status
                                                          on Follow_up.patient_id = latest_follow_up_status.patient_id
                                      WHERE screening_status = 'Cervical cancer screening performed'
                                        and rn = 1
                                        and latest_follow_up_status.follow_up_status not in ('Dead', 'Transferred out')
                                      group by Follow_up.patient_id, Follow_up.encounter_id, Follow_up.follow_up_date),
     cervical_screening_not_performed AS (SELECT MAX(follow_up_date)    AS max_follow_up_date,
                                                 Follow_up.patient_id   as max_patient_id,
                                                 Follow_up.encounter_id as max_encounter_id,
                                                 ROW_NUMBER()              OVER (PARTITION BY Follow_up.patient_id ORDER BY follow_up_date DESC, Follow_up.encounter_id DESC) AS rn
                                          FROM Follow_up
                                                   inner join latest_follow_up_status
                                                              on Follow_up.patient_id = latest_follow_up_status.patient_id
                                          WHERE Follow_up.patient_id not in
                                                (select max_patient_id from cervical_screening_performed)
                                            and sex = 'FEMALE'
                                            and age > 15
                                            and rn = 1
                                            and latest_follow_up_status.follow_up_status not in
                                                ('Dead', 'Transferred out')
                                          group by Follow_up.patient_id, Follow_up.encounter_id,
                                                   Follow_up.follow_up_date),
     cervical_data as (SELECT NAME,
                              phonenumber,
                              mobilephonenumber,
                              mrn,
                              uan,
                              age,
                              art_start_date,
                              regimen,
                              adherence,
                              follow_up_status,
                              follow_up_date,
                              ccs_next_date             as appointmentdate,
                              Follow_up.next_visit_date,
                              Follow_up.next_visit_date as next_visit_date_et,
                              CASE
                                  WHEN TIMESTAMPDIFF(DAY, hpv_dna_result_received_date, '2024-01-01') > 1095
                                      AND ccs_hpv_result = 'Negative result'
                                      THEN 'HPV Screened Negative-Need Re-screening'
                                  WHEN TIMESTAMPDIFF(DAY, date_via_result, '2024-01-01') > 730
                                      AND ccs_via_result = 'VIA negative' THEN 'VIA Screened Negative-Need Re-screening'
                                  WHEN TIMESTAMPDIFF(DAY, cytology_result_received_date, '2024-01-01') > 1095
                                      AND cytology_result = 0 THEN 'Cytology Screened Negative-Need Re-screening'
                                  WHEN ccs_treat_received_date = '1900-01-01 00:00:00'
                                      AND (colposcopy_exam_finding IN ('Low Grade', 'High Grade')
                                          OR cytology_result = 2
                                          OR ccs_via_result IN ('VIA positive: eligible for cryo/thermo-coagula',
                                                                'VIA positive: non-eligible for cryo/thermo-coagula'))
                                      THEN 'Positive Result with No Treatment Date'
                                  WHEN TIMESTAMPDIFF(DAY, ccs_treat_received_date, '2024-01-01') > 181
                                      AND ccs_treat_received_date > '1900-01-01 00:00:00'
                                      THEN 'Treated with Cryotherapy/Thermocoagulation/LEEP'
                                  WHEN TIMESTAMPDIFF(DAY, hpv_dna_result_received_date, '2024-01-01') > 365
                                      AND ccs_hpv_result = 'Positive'
                                      AND ccs_via_result = 'VIA negative'
                                      THEN 'HPV Positive but VIA Negative- Need Re-screening'
                                  WHEN ccs_next_date <= '2024-01-01'
                                      AND ccs_next_date != '1900-01-01' THEN 'Other'
                                  END                                                         AS treatmentreceived
                       FROM cervical_screening_performed
                                INNER JOIN
                            Follow_up ON Follow_up.encounter_id = cervical_screening_performed.max_encounter_id
                       WHERE cervical_screening_performed.rn = 1
                       UNION
                       select NAME,
                              phonenumber,
                              mobilephonenumber,
                              mrn,
                              uan,
                              age,
                              art_start_date,
                              regimen,
                              adherence,
                              follow_up_status,
                              follow_up_date,
                              ccs_next_date             as appointmentdate,
                              Follow_up.next_visit_date,
                              next_visit_date           as next_visit_date_et,
                              'Never screened for CxCa' as treatmentreceived
                       from cervical_screening_not_performed
                                INNER JOIN Follow_up
                                           ON Follow_up.encounter_id = cervical_screening_not_performed.max_encounter_id
                       WHERE cervical_screening_not_performed.rn = 1)
select *
from cervical_data
where treatmentreceived is not null;

END
//



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_pediatric_age_out_query  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_fact_pediatric_age_out_query;

~-~-
CREATE PROCEDURE sp_fact_pediatric_age_out_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    WITH Follow_up AS (SELECT follow_up_date_followup_                                      AS follow_up_date,
                              follow_up.client_id                                           AS patient_id,
                              follow_up.encounter_id                                        AS encounter_id,
                              follow_up_status,
                              FLOOR(TIMESTAMPDIFF(MONTH, date_of_birth, '2024-01-01') / 12) AS age,
                              mrn,
                              uan,
                              sex,
                              date_of_birth,
                              patient_name                                                  as NAME,
                              ''                                                            as phonenumber,
                              mobile_no                                                     as mobilephonenumber,
                              regimen,
                              art_antiretroviral_start_date                                 as art_sart_date,
                              diagnosis_date                                                as date_hiv_confirmed,
                              next_visit_date,
                              antiretroviral_art_dispensed_dose_i                           as art_dose,
                              intake_a.date_enrolled_in_care                                as registration_date
                       FROM mamba_flat_encounter_follow_up follow_up
                                INNER JOIN
                            mamba_flat_encounter_follow_up_1 follow_up_1
                            ON follow_up.encounter_id = follow_up_1.encounter_id
                                LEFT JOIN
                            mamba_dim_client_art_follow_up dim_client
                            ON follow_up.client_id = dim_client.client_id
                                left join mamba_flat_encounter_intake_a intake_a
                                          on intake_a.client_id = follow_up.client_id
                       where follow_up_date_followup_ < REPORT_END_DATE),
         firstArtRegimen as (select patient_id,
                                    Min(follow_up_date)                                                                         as followupdate,
                                    encounter_id,
                                    ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY follow_up_date DESC, encounter_id DESC) AS rn
                             FROM Follow_up
                             WHERE regimen IS NOT NULL
                             GROUP BY patient_id, encounter_id, follow_up_date),
         firstArtRegimen2 as (SELECT regimen, Follow_up.patient_id
                              from Follow_up
                                       INNER JOIN firstArtRegimen
                                                  on Follow_up.encounter_id = firstArtRegimen.encounter_id
                              where Follow_up.regimen IS NOT NULL
                                and firstArtRegimen.rn = 1),
         latest_follow_up_encounter as (SELECT patient_id,
                                               encounter_id,
                                               ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY follow_up_date DESC, encounter_id DESC) AS rn
                                        from Follow_up
                                        where follow_up_status IS NOT NULL
                                          and art_sart_date is not null)


    SELECT DISTINCT Follow_up.patient_id,
                    Follow_up.NAME              AS patientname,
                    mrn,
                    uan,
                    sex,
                    date_of_birth               AS date_of_birth_gc,

                    date_of_birth               AS date_of_birth_et,
                    date_of_birth,
                    FLOOR(TIMESTAMPDIFF(MONTH, date_of_birth, registration_date) / 12)
                                                AS age_at_enrollment,
                    CEILING(TIMESTAMPDIFF(MONTH, date_of_birth, '2024-01-01') / 12)
                                                AS current_age,
                    registration_date           AS enrollment_date,
                    date_hiv_confirmed          AS hivconfirmed_date_et,
                    date_hiv_confirmed          AS hivconfirmed_date_gc,
                    art_sart_date               AS artstartdate_et,
                    art_sart_date               AS artstarteddate_gc,
                    follow_up_date              AS followupdate_gc,
                    follow_up_date              AS followupdate_ec,
                    follow_up_status,
                    CASE
                        WHEN TIMESTAMPDIFF(DAY, next_visit_date, CURDATE()) >= 30
                            AND TIMESTAMPDIFF(DAY, next_visit_date, CURDATE()) <= 60 THEN '1lost'
                        WHEN TIMESTAMPDIFF(DAY, next_visit_date, CURDATE()) > 60
                            AND TIMESTAMPDIFF(DAY, next_visit_date, CURDATE()) <= 90 THEN '2lost'
                        WHEN TIMESTAMPDIFF(DAY, next_visit_date, CURDATE()) > 90 THEN 'dropped'
                        ELSE ' ' END            AS current_status,
                    firstArtRegimen2.regimen    AS arv_regimen_when_started_art,
                    Follow_up.regimen           AS arv_regimen,
                    CASE
                        WHEN art_dose = 0 THEN '30'
                        WHEN art_dose = 1 THEN '60'
                        WHEN art_dose = 2 THEN '90'
                        WHEN art_dose = 3 THEN '120'
                        WHEN art_dose = 4 THEN '150'
                        WHEN art_dose = 5 THEN '180'
                        ELSE art_dose
                        END                     AS ndays,
                    CASE
                        WHEN next_visit_date = '1900-01-01' THEN NULL
                        ELSE
                            next_visit_date END AS nextvisitdate,
                    DATE_ADD(
                            date_of_birth, INTERVAL 15 YEAR
                    )                           AS date_of_15th_birthday_gc,
                    DATE_ADD(
                            date_of_birth, INTERVAL 15 YEAR
                    )                           AS date_of_15th_birthday_et,
                    CASE
                        WHEN FLOOR(TIMESTAMPDIFF(MONTH, date_of_birth, '2024-01-01') / 12) >= '15' THEN 'YES'
                        ELSE 'NO'
                        END                     AS age_out
    FROM latest_follow_up_encounter
             left join Follow_up on latest_follow_up_encounter.encounter_id = Follow_up.encounter_id


             LEFT OUTER JOIN firstArtRegimen2 on firstArtRegimen2.patient_id = Follow_up.patient_id
    where rn = 1

      and (FLOOR(TIMESTAMPDIFF(MONTH, date_of_birth, registration_date) / 12) <= '15')
    order by patientname
                 AND (DATE_ADD(
                date_of_birth, INTERVAL 15 YEAR
                      )) BETWEEN REPORT_START_DATE
                 AND REPORT_END_DATE;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_dim_client_transfer_in  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_dim_client_transfer_in;


~-~-
CREATE PROCEDURE sp_dim_client_transfer_in()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_dim_client_transfer_in', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_dim_client_transfer_in', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_dim_client_transfer_in_create();
CALL sp_dim_client_transfer_in_insert();
CALL sp_dim_client_transfer_in_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_dim_client_transfer_in_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_dim_client_transfer_in_create;


~-~-
CREATE PROCEDURE sp_dim_client_transfer_in_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_dim_client_transfer_in_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_dim_client_transfer_in_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_dim_client_transfer_in
(
    id                INT AUTO_INCREMENT,
    client_id         INT           NOT NULL,
    patient_name      NVARCHAR(255) NOT NULL,
    mrn               NVARCHAR(50)  NULL,
    uan               NVARCHAR(50)  NULL,
    current_age       INT,
    mobile_no         NVARCHAR(50),
    date_of_birth     DATE          NULL,
    sex               NVARCHAR(50)  NULL,
    state_province    NVARCHAR(255) NULL,
    county_district   NVARCHAR(255) NULL,
    city_village      NVARCHAR(255) NULL,
    PRIMARY KEY (id)
);
CREATE INDEX mamba_dim_client_transfer_in_client_id_index ON mamba_dim_client_transfer_in (client_id);
CREATE INDEX mamba_dim_client_transfer_in_mrn_index ON mamba_dim_client_transfer_in (mrn);
CREATE INDEX mamba_dim_client_transfer_in_uan_index ON mamba_dim_client_transfer_in (uan);
CREATE INDEX mamba_dim_client_transfer_in_age_index ON mamba_dim_client_transfer_in (current_age);
CREATE INDEX mamba_dim_client_transfer_in_sex_index ON mamba_dim_client_transfer_in (sex);
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_dim_client_transfer_in_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_dim_client_transfer_in_insert;


~-~-
CREATE PROCEDURE sp_dim_client_transfer_in_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_dim_client_transfer_in_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_dim_client_transfer_in_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
INSERT INTO mamba_dim_client_transfer_in (
                        client_id,
                        patient_name,
                        mrn,
                        uan,
                        current_age,
                        mobile_no,
                        date_of_birth,
                        sex,
                        state_province,
                        county_district,
                        city_village
                    )
SELECT DISTINCT
    person.person_id,
    person.person_name_long,
    (SELECT pid.identifier
     FROM mamba_dim_patient_identifier pid
              INNER JOIN mamba_dim_patient_identifier_type pid_type ON pid.identifier_type = pid_type.id
     WHERE pid.patient_id = person.person_id AND pid_type.patient_identifier_type_id = 5
        LIMIT 1) AS 'MRN',
    (SELECT pid.identifier
     FROM mamba_dim_patient_identifier pid
     INNER JOIN mamba_dim_patient_identifier_type pid_type ON pid.identifier_type = pid_type.id
     WHERE pid.patient_id = person.person_id AND pid_type.patient_identifier_type_id = 6
     LIMIT 1) AS 'UAN',
    DATEDIFF(CURRENT_DATE(), person.birthdate) / 365 AS current_age,
    p_attr.value,
    person.birthdate,
    CASE
        WHEN person.gender = 'F' THEN 'FEMALE'
        WHEN person.gender = 'M' THEN 'MALE'
    END AS gender,
    p_add.state_province,
    p_add.county_district,
    p_add.city_village
FROM
    mamba_dim_person person
    LEFT JOIN mamba_dim_person_address p_add ON person.person_id = p_add.person_id
    LEFT JOIN mamba_dim_person_attribute p_attr ON person.person_id = p_attr.person_id AND p_attr.person_attribute_type_id = 9 
    LEFT JOIN mamba_dim_person_attribute_type p_attr_type ON p_attr.person_attribute_type_id = p_attr_type.person_attribute_type_id;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_dim_client_transfer_in_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_dim_client_transfer_in_update;


~-~-
CREATE PROCEDURE sp_dim_client_transfer_in_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_dim_client_transfer_in_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_dim_client_transfer_in_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_transfer_in  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_transfer_in;


~-~-
CREATE PROCEDURE sp_fact_encounter_transfer_in()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_transfer_in', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_transfer_in', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_fact_encounter_transfer_in_create();
CALL sp_fact_encounter_transfer_in_insert();
CALL sp_fact_encounter_transfer_in_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_transfer_in_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_transfer_in_create;


~-~-
CREATE PROCEDURE sp_fact_encounter_transfer_in_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_transfer_in_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_transfer_in_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_fact_client_tranfer_in
(
  id INT AUTO_INCREMENT,
  client_id INT NOT NULL,
  ti_status VARCHAR(255) NULL,
  ti_date DATE NULL,
  art_start_date DATE,
  treatment_end_date DATE,
  next_visit_date        DATE,
  latest_followup_date   DATE,
  latest_followup_status NVARCHAR(255),
  latest_regimen NVARCHAR(255),
  adherence NVARCHAR(255),
  PRIMARY KEY (id)
);
CREATE INDEX mamba_fact_client_tranfer_in_latest_followup_date_index ON mamba_fact_client_tranfer_in (latest_followup_date);
CREATE INDEX mamba_fact_client_tranfer_in_client_id_index ON mamba_fact_client_tranfer_in (client_id);
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_transfer_in_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_transfer_in_insert;


~-~-
CREATE PROCEDURE sp_fact_encounter_transfer_in_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_transfer_in_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_transfer_in_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
INSERT INTO mamba_fact_client_tranfer_in (
    client_id,
    ti_status,
    ti_date,
    art_start_date,
    treatment_end_date,
    next_visit_date,
    latest_followup_date,
    latest_followup_status,
    latest_regimen,
    adherence
)

SELECT client_id,
	   ti_status,
       MIN(CASE WHEN rn_asc = 1 THEN follow_up_date_followup_ END)             AS ti_date,
       MIN(CASE WHEN rn_asc = 1 THEN art_antiretroviral_start_date END)        AS art_start_date,
       MAX(CASE WHEN rn_desc = 1 THEN treatment_end_date END)                  AS treatment_end_date,
       MAX(CASE WHEN rn_desc = 1 THEN return_visit_date END)                   AS next_visit_date,
       MAX(CASE WHEN rn_desc = 1 THEN follow_up_date_followup_ END)            AS latest_followup_date,
       MAX(CASE WHEN rn_desc = 1 THEN follow_up_status END)                    AS latest_followup_status,
       MAX(CASE WHEN rn_desc = 1 THEN regimen END)                             AS latest_regimen,
       MAX(CASE WHEN rn_desc = 1 THEN adherence END) AS adherence
FROM (SELECT f1.client_id,
			 CASE f2.why_eligible_for_hiv_test_ WHEN 'Transfer in' THEN 'TI' ELSE '' END ti_status,
             adherence,
             art_antiretroviral_start_date,
             follow_up_date_followup_,
             regimen,
             follow_up_status,
             treatment_end_date,
             return_visit_date,
             ROW_NUMBER() OVER (PARTITION BY f1.client_id ORDER BY follow_up_date_followup_ )     AS rn_asc,
             ROW_NUMBER() OVER (PARTITION BY f1.client_id ORDER BY follow_up_date_followup_ DESC) AS rn_desc
      FROM mamba_flat_encounter_follow_up f1 INNER JOIN
mamba_flat_encounter_follow_up_1 f2 ON f1.encounter_id=f2.encounter_id AND f1.client_id=f2.client_id
      where art_antiretroviral_start_date is not null AND f2.why_eligible_for_hiv_test_ ='Transfer in') AS subquery
WHERE rn_asc = 1 OR rn_desc = 1
GROUP BY client_id;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_transfer_in_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_transfer_in_update;


~-~-
CREATE PROCEDURE sp_fact_encounter_transfer_in_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_transfer_in_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_transfer_in_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encouter_transfer_in_new_query  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_fact_encounter_transfer_in_new_query;

~-~-
CREATE PROCEDURE sp_fact_encounter_transfer_in_new_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    SELECT
        dim.client_id,
        dim.patient_name,
        dim.mrn,
        dim.uan,
        dim.current_age,
        dim.mobile_no,
        dim.date_of_birth,
        dim.sex,
        dim.state_province AS region,
        dim.county_district AS zone,
        dim.city_village AS woreda,
        fact.ti_status,
        fact.ti_date,
        fact.art_start_date,
        fact.treatment_end_date,
        fact.next_visit_date,
        fact.latest_followup_date,
        fact.latest_followup_status,
        fact.latest_regimen,
        fact.adherence
    FROM mamba_dim_client_transfer_in dim
             INNER JOIN mamba_fact_client_tranfer_in fact ON fact.client_id = dim.client_id
    WHERE fact.ti_date BETWEEN  CAST(REPORT_START_DATE as DATE) AND CAST(REPORT_END_DATE as DATE)
      and dim.mrn is not null and fact.latest_followup_status in ('Alive', 'Restart');
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_data_processing_derived_transfer_in  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_data_processing_derived_transfer_in;


~-~-
CREATE PROCEDURE sp_data_processing_derived_transfer_in()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_data_processing_derived_transfer_in', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_data_processing_derived_transfer_in', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_dim_client_transfer_in();
CALL sp_fact_encounter_transfer_in();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_transfer_out  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_transfer_out;


~-~-
CREATE PROCEDURE sp_fact_encounter_transfer_out()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_transfer_out', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_transfer_out', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_fact_encounter_transfer_out_create();
CALL sp_fact_encounter_transfer_out_insert();
CALL sp_fact_encounter_transfer_out_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_transfer_out_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_transfer_out_create;


~-~-
CREATE PROCEDURE sp_fact_encounter_transfer_out_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_transfer_out_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_transfer_out_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_fact_client_tranfer_out
(
  id INT AUTO_INCREMENT,
  client_id INT NOT NULL,
  latest_followup_date   DATE,
  art_start_date DATE,
  adherence NVARCHAR(255),
  regimen NVARCHAR(255),
  follow_up_status VARCHAR(255) NULL,
  next_visit_date        DATE,
  treatment_end_date DATE,
  PRIMARY KEY (id)
);
CREATE INDEX mamba_fact_client_tranfer_out_latest_followup_date_index ON mamba_fact_client_tranfer_out (latest_followup_date);
CREATE INDEX mamba_fact_client_tranfer_out_client_id_index ON mamba_fact_client_tranfer_out (client_id);
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_transfer_out_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_transfer_out_insert;


~-~-
CREATE PROCEDURE sp_fact_encounter_transfer_out_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_transfer_out_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_transfer_out_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
INSERT INTO mamba_fact_client_tranfer_out (
    client_id,
    latest_followup_date,
    art_start_date,
    adherence,
    regimen,
    follow_up_status,
    next_visit_date,
    treatment_end_date
)
SELECT client_id,
       MAX(CASE WHEN rn_desc = 1 THEN art_antiretroviral_start_date END)        AS art_start_date,
       MAX(CASE WHEN rn_desc = 1 THEN treatment_end_date END)                  AS treatment_end_date,
       MAX(CASE WHEN rn_desc = 1 THEN return_visit_date END)                   AS next_visit_date,
       MAX(CASE WHEN rn_desc = 1 THEN follow_up_date_followup_ END)            AS followup_date,
       MAX(CASE WHEN rn_desc = 1 THEN follow_up_status END)                    AS followup_status,
       MAX(CASE WHEN rn_desc = 1 THEN regimen END)                             AS regimen,
       MAX(CASE WHEN rn_desc = 1 THEN adherence END) AS adherence
FROM (SELECT f1.client_id,
             adherence,
             art_antiretroviral_start_date,
             follow_up_date_followup_,
             regimen,
             follow_up_status,
             treatment_end_date,
             return_visit_date,
             -- ROW_NUMBER() OVER (PARTITION BY f1.client_id ORDER BY follow_up_date_followup_ )     AS rn_asc,
             ROW_NUMBER() OVER (PARTITION BY f1.client_id ORDER BY follow_up_date_followup_ DESC) AS rn_desc
      FROM mamba_flat_encounter_follow_up f1 INNER JOIN
mamba_flat_encounter_follow_up_1 f2 ON f1.encounter_id=f2.encounter_id AND f1.client_id=f2.client_id
      where art_antiretroviral_start_date is not null AND f1.follow_up_status ='Transferred out') AS subquery
WHERE rn_desc = 1
GROUP BY client_id;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_fact_encounter_transfer_out_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_fact_encounter_transfer_out_update;


~-~-
CREATE PROCEDURE sp_fact_encounter_transfer_out_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_fact_encounter_transfer_out_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_fact_encounter_transfer_out_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_data_processing_derived_transfer_out  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_data_processing_derived_transfer_out;


~-~-
CREATE PROCEDURE sp_data_processing_derived_transfer_out()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_data_processing_derived_transfer_out', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_data_processing_derived_transfer_out', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_fact_encounter_transfer_out();
-- $END
END;
~-~-



-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_mother_hiv_status_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_mother_hiv_status_query;


~-~-
CREATE PROCEDURE sp_mamba_report_mother_hiv_status_query(IN ptracker_id VARCHAR(255), IN person_uuid VARCHAR(255))
BEGIN

SELECT pm.hiv_test_result AS hiv_test_result FROM mamba_flat_encounter_pmtct_anc pm INNER JOIN mamba_dim_person p ON pm.client_id = p.person_id WHERE p.uuid = person_uuid AND pm.ptracker_id = ptracker_id;

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_mother_hiv_status_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_mother_hiv_status_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_report_mother_hiv_status_columns_query(IN ptracker_id VARCHAR(255), IN person_uuid VARCHAR(255))
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_report_mother_hiv_status;
CREATE TABLE mamba_report_mother_hiv_status AS
SELECT pm.hiv_test_result AS hiv_test_result FROM mamba_flat_encounter_pmtct_anc pm INNER JOIN mamba_dim_person p ON pm.client_id = p.person_id WHERE p.uuid = person_uuid AND pm.ptracker_id = ptracker_id
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_report_mother_hiv_status';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='mother_hiv_status';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_total_deliveries_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_total_deliveries_query;


~-~-
CREATE PROCEDURE sp_mamba_report_total_deliveries_query()
BEGIN

SELECT COUNT(*) AS total_deliveries FROM mamba_dim_encounter e inner join mamba_dim_encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '6dc5308d-27c9-4d49-b16f-2c5e3c759757' AND DATE(e.encounter_datetime) > CONCAT(YEAR(CURDATE()), '-01-01 00:00:00');

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_total_deliveries_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_total_deliveries_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_report_total_deliveries_columns_query()
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_report_total_deliveries;
CREATE TABLE mamba_report_total_deliveries AS
SELECT COUNT(*) AS total_deliveries FROM mamba_dim_encounter e inner join mamba_dim_encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '6dc5308d-27c9-4d49-b16f-2c5e3c759757' AND DATE(e.encounter_datetime) > CONCAT(YEAR(CURDATE()), '-01-01 00:00:00')
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_report_total_deliveries';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='total_deliveries';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_total_hiv_exposed_infants_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_total_hiv_exposed_infants_query;


~-~-
CREATE PROCEDURE sp_mamba_report_total_hiv_exposed_infants_query()
BEGIN

SELECT COUNT(DISTINCT ei.infant_client_id) AS total_hiv_exposed_infants FROM mamba_fact_pmtct_exposedinfants ei INNER JOIN mamba_dim_person p ON ei.infant_client_id = p.person_id WHERE ei.encounter_datetime BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND NOW() AND birthdate BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND NOW();

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_total_hiv_exposed_infants_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_total_hiv_exposed_infants_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_report_total_hiv_exposed_infants_columns_query()
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_report_total_hiv_exposed_infants;
CREATE TABLE mamba_report_total_hiv_exposed_infants AS
SELECT COUNT(DISTINCT ei.infant_client_id) AS total_hiv_exposed_infants FROM mamba_fact_pmtct_exposedinfants ei INNER JOIN mamba_dim_person p ON ei.infant_client_id = p.person_id WHERE ei.encounter_datetime BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND NOW() AND birthdate BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND NOW()
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_report_total_hiv_exposed_infants';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='total_hiv_exposed_infants';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_total_pregnant_women_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_total_pregnant_women_query;


~-~-
CREATE PROCEDURE sp_mamba_report_total_pregnant_women_query()
BEGIN

SELECT COUNT(DISTINCT pw.client_id) AS total_pregnant_women FROM mamba_fact_pmtct_pregnant_women pw WHERE visit_type like 'New%' AND encounter_datetime BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND NOW() AND DATE_ADD(date_of_last_menstrual_period, INTERVAL 40 WEEK) > NOW();

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_total_pregnant_women_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_total_pregnant_women_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_report_total_pregnant_women_columns_query()
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_report_total_pregnant_women;
CREATE TABLE mamba_report_total_pregnant_women AS
SELECT COUNT(DISTINCT pw.client_id) AS total_pregnant_women FROM mamba_fact_pmtct_pregnant_women pw WHERE visit_type like 'New%' AND encounter_datetime BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND NOW() AND DATE_ADD(date_of_last_menstrual_period, INTERVAL 40 WEEK) > NOW()
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_report_total_pregnant_women';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='total_pregnant_women';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_total_active_dr_cases_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_total_active_dr_cases_query;


~-~-
CREATE PROCEDURE sp_mamba_report_total_active_dr_cases_query()
BEGIN

SELECT COUNT(DISTINCT e.patient_id) AS total_active_dr FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id, max(value_datetime)date_enrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid ='161552AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') group by e.patient_id ) ed ON ed.patient_id = e.patient_id LEFT JOIN ( SELECT DISTINCT e.patient_id, max(value_datetime)date_disenrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid ='159431AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') group by e.patient_id ) dd ON dd.patient_id = e.patient_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid ='163775AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AND o.value_coded = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid ='160052AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AND ( dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled);

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_total_active_dr_cases_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_total_active_dr_cases_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_report_total_active_dr_cases_columns_query()
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_report_total_active_dr_cases;
CREATE TABLE mamba_report_total_active_dr_cases AS
SELECT COUNT(DISTINCT e.patient_id) AS total_active_dr FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id, max(value_datetime)date_enrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid ='161552AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') group by e.patient_id ) ed ON ed.patient_id = e.patient_id LEFT JOIN ( SELECT DISTINCT e.patient_id, max(value_datetime)date_disenrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid ='159431AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') group by e.patient_id ) dd ON dd.patient_id = e.patient_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid ='163775AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AND o.value_coded = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid ='160052AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AND ( dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled)
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_report_total_active_dr_cases';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='total_active_dr_cases';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_total_active_ds_cases_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_total_active_ds_cases_query;


~-~-
CREATE PROCEDURE sp_mamba_report_total_active_ds_cases_query()
BEGIN

SELECT COUNT(DISTINCT e.patient_id) AS total_active_ds FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id, max(value_datetime)date_enrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid ='161552AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') group by e.patient_id ) ed ON ed.patient_id = e.patient_id LEFT JOIN ( SELECT DISTINCT e.patient_id, max(value_datetime)date_disenrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid ='159431AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') group by e.patient_id ) dd ON dd.patient_id = e.patient_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid ='163775AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AND o.value_coded = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid ='160541AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AND ( dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled);

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_total_active_ds_cases_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_total_active_ds_cases_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_report_total_active_ds_cases_columns_query()
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_report_total_active_ds_cases;
CREATE TABLE mamba_report_total_active_ds_cases AS
SELECT COUNT(DISTINCT e.patient_id) AS total_active_ds FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id, max(value_datetime)date_enrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid ='161552AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') group by e.patient_id ) ed ON ed.patient_id = e.patient_id LEFT JOIN ( SELECT DISTINCT e.patient_id, max(value_datetime)date_disenrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid ='159431AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') group by e.patient_id ) dd ON dd.patient_id = e.patient_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid ='163775AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AND o.value_coded = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid ='160541AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AND ( dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled)
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_report_total_active_ds_cases';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='total_active_ds_cases';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_mother_status_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_mother_status_query;


~-~-
CREATE PROCEDURE sp_mamba_report_mother_status_query(IN person_uuid VARCHAR(255))
BEGIN

SELECT cn.name as mother_status FROM mamba_source_db.obs o INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.concept_name cn on o.value_coded = cn.concept_id WHERE p.uuid = person_uuid AND c.uuid = '1856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND o.encounter_id = (SELECT e.encounter_id FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '6dc5308d-27c9-4d49-b16f-2c5e3c759757' AND c.uuid = '1856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid ORDER BY encounter_datetime desc  LIMIT 1) AND cn.voided = 0 AND cn.locale_preferred = 1;

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_mother_status_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_mother_status_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_report_mother_status_columns_query(IN person_uuid VARCHAR(255))
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_report_mother_status;
CREATE TABLE mamba_report_mother_status AS
SELECT cn.name as mother_status FROM mamba_source_db.obs o INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.concept_name cn on o.value_coded = cn.concept_id WHERE p.uuid = person_uuid AND c.uuid = '1856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND o.encounter_id = (SELECT e.encounter_id FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '6dc5308d-27c9-4d49-b16f-2c5e3c759757' AND c.uuid = '1856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid ORDER BY encounter_datetime desc  LIMIT 1) AND cn.voided = 0 AND cn.locale_preferred = 1
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_report_mother_status';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='mother_status';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_estimated_date_of_delivery_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_estimated_date_of_delivery_query;


~-~-
CREATE PROCEDURE sp_mamba_report_estimated_date_of_delivery_query(IN person_uuid VARCHAR(255))
BEGIN

(SELECT CASE WHEN o.value_datetime >=curdate() THEN DATE_FORMAT(o.value_datetime, '%d %m %Y') ELSE '' END AS estimated_date_of_delivery FROM mamba_source_db.obs o INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id  WHERE p.uuid = person_uuid AND c.uuid = '5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND o.encounter_id = (SELECT e.encounter_id FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '677d1a80-dbbe-4399-be34-aa7f54f11405' AND c.uuid = '5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid ORDER BY encounter_datetime desc  LIMIT 1) ORDER BY o.value_datetime DESC  LIMIT 1);

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_estimated_date_of_delivery_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_estimated_date_of_delivery_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_report_estimated_date_of_delivery_columns_query(IN person_uuid VARCHAR(255))
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_report_estimated_date_of_delivery;
CREATE TABLE mamba_report_estimated_date_of_delivery AS
(SELECT CASE WHEN o.value_datetime >=curdate() THEN DATE_FORMAT(o.value_datetime, '%d %m %Y') ELSE '' END AS estimated_date_of_delivery FROM mamba_source_db.obs o INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id  WHERE p.uuid = person_uuid AND c.uuid = '5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND o.encounter_id = (SELECT e.encounter_id FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '677d1a80-dbbe-4399-be34-aa7f54f11405' AND c.uuid = '5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid ORDER BY encounter_datetime desc  LIMIT 1) ORDER BY o.value_datetime DESC  LIMIT 1)
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_report_estimated_date_of_delivery';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='estimated_date_of_delivery';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_next_appointment_date_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_next_appointment_date_query;


~-~-
CREATE PROCEDURE sp_mamba_report_next_appointment_date_query(IN person_uuid VARCHAR(255))
BEGIN

(SELECT DATE_FORMAT(o.value_datetime, '%d %m %Y') as next_appointment FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT e.encounter_id FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '677d1a80-dbbe-4399-be34-aa7f54f11405' AND c.uuid = '5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid) a ON o.encounter_id = a.encounter_id LEFT JOIN (SELECT e.encounter_id FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id  INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '4362fd2d-1866-4ea0-84ef-5e5da9627440' AND c.uuid = '5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid)b  on o.encounter_id = b.encounter_id WHERE et.uuid = '677d1a80-dbbe-4399-be34-aa7f54f11405' AND c.uuid = '5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid ORDER BY  encounter_datetime DESC LIMIT 1);

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_next_appointment_date_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_next_appointment_date_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_report_next_appointment_date_columns_query(IN person_uuid VARCHAR(255))
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_report_next_appointment_date;
CREATE TABLE mamba_report_next_appointment_date AS
(SELECT DATE_FORMAT(o.value_datetime, '%d %m %Y') as next_appointment FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT e.encounter_id FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '677d1a80-dbbe-4399-be34-aa7f54f11405' AND c.uuid = '5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid) a ON o.encounter_id = a.encounter_id LEFT JOIN (SELECT e.encounter_id FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id  INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '4362fd2d-1866-4ea0-84ef-5e5da9627440' AND c.uuid = '5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid)b  on o.encounter_id = b.encounter_id WHERE et.uuid = '677d1a80-dbbe-4399-be34-aa7f54f11405' AND c.uuid = '5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid ORDER BY  encounter_datetime DESC LIMIT 1)
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_report_next_appointment_date';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='next_appointment_date';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_no_of_anc_visits_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_no_of_anc_visits_query;


~-~-
CREATE PROCEDURE sp_mamba_report_no_of_anc_visits_query(IN person_uuid VARCHAR(255))
BEGIN

SELECT COUNT(DISTINCT e.encounter_id)no_of_anc_visits FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '677d1a80-dbbe-4399-be34-aa7f54f11405' AND c.uuid = '5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND o.value_datetime >= curdate() AND p.uuid = person_uuid;

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_no_of_anc_visits_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_no_of_anc_visits_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_report_no_of_anc_visits_columns_query(IN person_uuid VARCHAR(255))
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_report_no_of_anc_visits;
CREATE TABLE mamba_report_no_of_anc_visits AS
SELECT COUNT(DISTINCT e.encounter_id)no_of_anc_visits FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '677d1a80-dbbe-4399-be34-aa7f54f11405' AND c.uuid = '5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND o.value_datetime >= curdate() AND p.uuid = person_uuid
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_report_no_of_anc_visits';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='no_of_anc_visits';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_total_active_tpt_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_total_active_tpt_query;


~-~-
CREATE PROCEDURE sp_mamba_report_total_active_tpt_query()
BEGIN

SELECT DISTINCT COUNT(DISTINCT e.patient_id) AS total_active_tpt FROM mamba_source_db.obs o  INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id,max(value_datetime) date_enrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id  WHERE et.uuid = 'dc6ce80c-83f8-4ace-a638-21df78542551' AND o.concept_id =  (SELECT concept_id FROM mamba_source_db.concept WHERE uuid = '162320AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') GROUP BY e.patient_id) ed ON ed.patient_id = e.patient_id LEFT JOIN(SELECT DISTINCT e.patient_id, max(value_datetime) date_disenrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = 'dc6ce80c-83f8-4ace-a638-21df78542551' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid = '163284AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') GROUP BY e.patient_id) dd ON dd.patient_id = e.patient_id WHERE et.uuid = 'dc6ce80c-83f8-4ace-a638-21df78542551' AND (dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled);

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_report_total_active_tpt_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_report_total_active_tpt_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_report_total_active_tpt_columns_query()
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_report_total_active_tpt;
CREATE TABLE mamba_report_total_active_tpt AS
SELECT DISTINCT COUNT(DISTINCT e.patient_id) AS total_active_tpt FROM mamba_source_db.obs o  INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id,max(value_datetime) date_enrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id  WHERE et.uuid = 'dc6ce80c-83f8-4ace-a638-21df78542551' AND o.concept_id =  (SELECT concept_id FROM mamba_source_db.concept WHERE uuid = '162320AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') GROUP BY e.patient_id) ed ON ed.patient_id = e.patient_id LEFT JOIN(SELECT DISTINCT e.patient_id, max(value_datetime) date_disenrolled FROM mamba_source_db.obs o INNER JOIN mamba_source_db.concept c on o.concept_id = c.concept_id INNER JOIN mamba_source_db.person p on o.person_id = p.person_id INNER JOIN mamba_source_db.encounter e on o.encounter_id = e.encounter_id INNER JOIN mamba_source_db.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = 'dc6ce80c-83f8-4ace-a638-21df78542551' AND o.concept_id = (SELECT concept_id FROM mamba_source_db.concept WHERE uuid = '163284AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') GROUP BY e.patient_id) dd ON dd.patient_id = e.patient_id WHERE et.uuid = 'dc6ce80c-83f8-4ace-a638-21df78542551' AND (dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled)
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_report_total_active_tpt';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='total_active_tpt';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------------  Setup the MambaETL Scheduler  ---------------------------------
-- ---------------------------------------------------------------------------------------------


-- Enable the event etl_scheduler
SET GLOBAL event_scheduler = ON;

~-~-

-- Drop/Create the Event responsible for firing up the ETL process
DROP EVENT IF EXISTS _mamba_etl_scheduler_event;

~-~-

-- Setup ETL configurations
CALL sp_mamba_etl_setup(?, ?, ?, ?, ?,?,?);
-- pass them from the runtime properties file

~-~-

CREATE EVENT IF NOT EXISTS _mamba_etl_scheduler_event
    ON SCHEDULE EVERY ? SECOND
        STARTS CURRENT_TIMESTAMP
    DO CALL sp_mamba_etl_schedule();

~-~-

