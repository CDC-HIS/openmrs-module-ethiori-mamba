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
SELECT person.person_id,
       person.person_name_long,
       p_name.prefix,
       p_name.given_name,
       p_name.middle_name,
       p_name.family_name,
       mrn.mrn                                                                  AS MRN ,
       uan.uan                                                                  AS UAN,
       person.uuid,
       fn_mamba_age_calculator(person.birthdate, CURDATE())                     AS current_age,
       d.mobile_no,
       e.phone_no,
       person.birthdate,
       CASE
           WHEN person.gender = 'F' THEN 'FEMALE'
           WHEN person.gender = 'M' THEN 'MALE'
           END                                                                  AS gender,
       p_add.state_province,
       p_add.county_district,
       p_add.city_village,
       CASE WHEN a.key_population = 'General population' THEN NULL ELSE a.key_population END AS key_population,
       b.marital_status,
       c.education_level,
       (SELECT normal_agegroup from mamba_dim_agegroup where age = current_age) as coarse_age_group,
       (SELECT datim_agegroup from mamba_dim_agegroup where age = current_age)  as fine_age_group

FROM mamba_dim_person person
         LEFT JOIN mamba_dim_person_address p_add ON person.person_id = p_add.person_id
         LEFT JOIN mamba_dim_person_name p_name ON person.person_id = p_name.person_id
         LEFT JOIN
     (SELECT pa.patient_id,
             pa.identifier mrn
      FROM mamba_dim_patient_identifier pa
      WHERE pa.identifier_type = 5) mrn ON mrn.patient_id = person.person_id
         LEFT JOIN
     (SELECT pa.patient_id,
             pa.identifier uan
      FROM mamba_dim_patient_identifier pa
      WHERE pa.identifier_type = 6) uan ON uan.patient_id = person.person_id
         LEFT JOIN
     (SELECT pa.person_id,
             pa.value key_population
      FROM mamba_dim_person_attribute pa
      WHERE pa.person_attribute_type_id = 25) a ON a.person_id = person.person_id
         LEFT JOIN
     (SELECT pa.person_id,
             pa.value marital_status
      FROM mamba_dim_person_attribute pa
      WHERE pa.person_attribute_type_id = 5) b ON b.person_id = person.person_id
         LEFT JOIN
     (SELECT pa.person_id,
             pa.value education_level
      FROM mamba_dim_person_attribute pa
      WHERE pa.person_attribute_type_id = 24) c ON c.person_id = person.person_id
         LEFT JOIN
     (SELECT pa.person_id,
             pa.value mobile_no
      FROM mamba_dim_person_attribute pa
      WHERE pa.person_attribute_type_id = 26) d ON d.person_id = person.person_id
         LEFT JOIN
     (SELECT pa.person_id,
             pa.value phone_no
      FROM mamba_dim_person_attribute pa
      WHERE pa.person_attribute_type_id = 16) e ON e.person_id = person.person_id;
-- $END