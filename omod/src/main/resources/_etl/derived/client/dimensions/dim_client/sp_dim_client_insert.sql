-- $BEGIN
INSERT INTO mamba_dim_client (client_id,
                              patient_name,
                              prefix,
                              given_name,
                              middle_name,
                              family_name,
                              mrn,
                              uan,
                              patient_uuid,
                              current_age,
                              mobile_no,
                              phone_no,
                              date_of_birth,
                              sex,
                              state_province,
                              county_district,
                              city_village,
                              key_population,
                              marital_status,
                              education_level,
                              coarse_age_group,
                              fine_age_group)
SELECT p.person_id,
       p.person_name_long,
       pn.prefix,
       pn.given_name,
       pn.middle_name,
       pn.family_name,
       MAX(CASE WHEN p_id.identifier_type = 5 THEN p_id.identifier END)          AS MRN,
       MAX(CASE WHEN p_id.identifier_type = 6 THEN p_id.identifier END)          AS UAN,
       p.uuid,
       fn_mamba_age_calculator(p.birthdate, CURDATE())                           AS current_age,
       MAX(CASE WHEN p_attr.person_attribute_type_id = 26 THEN p_attr.value END) AS mobile_no,
       MAX(CASE WHEN p_attr.person_attribute_type_id = 16 THEN p_attr.value END) AS phone_no,
       p.birthdate,
       CASE
           WHEN p.gender = 'F' THEN 'FEMALE'
           WHEN p.gender = 'M' THEN 'MALE'
           END                                                                   AS gender,
       p_addr.state_province,
       p_addr.county_district,
       p_addr.city_village,
       MAX(CASE WHEN p_attr.person_attribute_type_id = 25 THEN p_attr.value END) AS key_population,
       MAX(CASE WHEN p_attr.person_attribute_type_id = 5 THEN p_attr.value END)  AS marital_status,
       MAX(CASE WHEN p_attr.person_attribute_type_id = 24 THEN p_attr.value END) AS education_level,
       mag_normal.normal_agegroup                                                AS coarse_age_group,
       mag_datim.datim_agegroup                                                  AS fine_age_group
FROM mamba_dim_person p
         LEFT JOIN mamba_dim_person_name pn ON p.person_id = pn.person_id
         LEFT JOIN mamba_dim_person_address p_addr ON p.person_id = p_addr.person_id
         LEFT JOIN mamba_dim_patient_identifier p_id on p.person_id = p_id.patient_id
         LEFT JOIN mamba_dim_person_attribute p_attr on p.person_id = p_attr.person_id
         LEFT JOIN mamba_dim_agegroup mag_normal ON fn_mamba_age_calculator(p.birthdate, CURDATE()) = mag_normal.age
         LEFT JOIN mamba_dim_agegroup mag_datim ON fn_mamba_age_calculator(p.birthdate, CURDATE()) = mag_datim.age
where pn.preferred = 1
  and pn.voided = 0
  and p_addr.preferred = 1
  and p_addr.voided = 0
  and p_id.voided = 0
GROUP BY p.person_id,
         pn.prefix,
         pn.given_name,
         pn.middle_name,
         pn.family_name,
         p.uuid,
         p.birthdate,
         gender,
         p_addr.state_province,
         p_addr.county_district,
         p_addr.city_village,
         coarse_age_group,
         fine_age_group;
-- $END