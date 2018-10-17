SELECT MIN(mi.info) AS release_date,
       MIN(miidx.info) AS rating,
       MIN(t.title) AS german_movie
FROM movie_info AS mi
JOIN title AS t ON mi.movie_id = t.id
JOIN info_type AS it2 ON it2.id = mi.info_type_id
JOIN kind_type AS kt ON kt.id = t.kind_id
JOIN movie_companies AS mc ON mc.movie_id = t.id AND mi.movie_id = mc.movie_id
JOIN company_name AS cn ON cn.id = mc.company_id
JOIN company_type AS ct ON ct.id = mc.company_type_id
JOIN movie_info_idx AS miidx ON miidx.movie_id = t.id AND mi.movie_id = miidx.movie_id AND miidx.movie_id = mc.movie_id;
JOIN info_type AS it ON it.id = miidx.info_type_id
WHERE cn.country_code ='[de]'
  AND ct.kind ='production companies'
  AND it.info ='rating'
  AND it2.info ='release dates'
  AND kt.kind ='movie'
