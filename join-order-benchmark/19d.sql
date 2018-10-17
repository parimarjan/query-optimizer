SELECT MIN(n.name) AS voicing_actress,
       MIN(t.title) AS jap_engl_voiced_movie
FROM title AS t
JOIN movie_info AS mi ON t.id = mi.movie_id
JOIN movie_companies AS mc ON t.id = mc.movie_id
JOIN cast_info AS ci ON t.id = ci.movie_id
JOIN company_name AS cn ON cn.id = mc.company_id
JOIN info_type AS it ON it.id = mi.info_type_id
JOIN name AS n ON n.id = ci.person_id
JOIN role_type AS rt ON rt.id = ci.role_id
JOIN aka_name AS an ON n.id = an.person_id
JOIN char_name AS chn ON chn.id = ci.person_role_id;
WHERE ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)')
  AND cn.country_code ='[us]'
  AND it.info = 'release dates'
  AND n.gender ='f'
  AND rt.role ='actress'
  AND t.production_year > 2000
  AND mc.movie_id = ci.movie_id
  AND mc.movie_id = mi.movie_id
  AND mi.movie_id = ci.movie_id
  AND ci.person_id = an.person_id
