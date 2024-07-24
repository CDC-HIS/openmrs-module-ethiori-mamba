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
                        city_village,
                        coarse_age_group,
                        fine_age_group
                    )
SELECT
    person.person_id,
    person.person_name_long,
    (SELECT pid.identifier
     FROM mamba_dim_patient_identifier pid
     WHERE pid.patient_id = person.person_id AND pid.identifier_type = 5
     LIMIT 1) AS 'MRN',
    (SELECT pid.identifier
     FROM mamba_dim_patient_identifier pid
     WHERE pid.patient_id = person.person_id AND pid.identifier_type = 6
     LIMIT 1) AS 'UAN',
    fn_mamba_age_calculator(birthdate,null) AS current_age,
    (select p_attr.value as mobile_no from mamba_dim_person_attribute p_attr
     where person.person_id = p_attr.person_id and p_attr.person_attribute_type_id=9 LIMIT 1),
    person.birthdate,
    CASE
        WHEN person.gender = 'F' THEN 'FEMALE'
        WHEN person.gender = 'M' THEN 'MALE'
        END AS gender,
    p_add.state_province,
    p_add.county_district,
    p_add.city_village,
    (SELECT datim_agegroup from mamba_dim_agegroup where age=fn_mamba_age_calculator(birthdate,null)) as coarse_age_group,
    (SELECT normal_agegroup from mamba_dim_agegroup where age= fn_mamba_age_calculator(birthdate,null)) as fine_age_group
FROM
    mamba_dim_person person
        LEFT JOIN mamba_dim_person_address p_add ON person.person_id = p_add.person_id;
-- $END