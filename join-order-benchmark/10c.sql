SELECT MIN(chn.name),
       MIN(t.title)
FROM char_name AS chn
JOIN cast_info AS ci ON chn.id = ci.person_role_id
JOIN movie_companies AS mc ON ci.movie_id = mc.movie_id
JOIN company_name AS cn ON cn.id = mc.company_id
JOIN company_type AS ct ON ct.id = mc.company_type_id
JOIN role_type AS rt ON rt.id = ci.role_id
JOIN title AS t ON t.id = mc.movie_id AND t.id = ci.movie_id
WHERE ci.note LIKE '%(producer)%'
  AND cn.country_code = '[us]'
  AND t.production_year > 1990
