-- $BEGIN
INSERT INTO mamba_dim_client_care_and_treatment (
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
     WHERE pid.patient_id = person.person_id AND pid_type.name = 'MRN'
        LIMIT 1) AS 'MRN',
    (SELECT pid.identifier
     FROM mamba_dim_patient_identifier pid
     INNER JOIN mamba_dim_patient_identifier_type pid_type ON pid.identifier_type = pid_type.id
     WHERE pid.patient_id = person.person_id AND pid_type.name = 'UAN'
     LIMIT 1) AS 'UAN',
    DATEDIFF(CURRENT_DATE(), person.birthdate) / 365 AS current_age,
    'mobile_no',
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
    INNER JOIN mamba_flat_encounter_follow_up enc_follow_up ON person.person_id = enc_follow_up.client_id
    INNER JOIN mamba_dim_person_address p_add ON person.person_id = p_add.person_id;
-- $END