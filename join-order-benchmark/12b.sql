SELECT MIN(mi.info) AS budget,
       MIN(t.title) AS unsuccsessful_movie
FROM title as t
JOIN movie_info AS mi ON t.id = mi.movie_id
JOIN movie_info_idx AS mi_idx ON t.id = mi_idx.movie_id AND mi.movie_id = mi_idx.movie_id
JOIN info_type AS it1 ON mi.info_type_id = it1.id
JOIN info_type AS it2 ON mi_idx.info_type_id = it2.id
JOIN movie_companies AS mc ON t.id = mc.movie_id AND mc.movie_id = mi.movie_id AND mc.movie_id = mi_idx.movie_id
JOIN company_type AS ct ON ct.id = mc.company_type_id
JOIN company_name AS cn ON cn.id = mc.company_id

WHERE cn.country_code ='[us]'
  AND ct.kind IS NOT NULL
  AND (ct.kind ='production companies'
       OR ct.kind = 'distributors')
  AND it1.info ='budget'
  AND it2.info ='bottom 10 rank'
  AND t.production_year >2000
  AND (t.title LIKE 'Birdemic%'
       OR t.title LIKE '%Movie%')
