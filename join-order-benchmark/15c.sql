SELECT MIN(mi.info) AS release_date,
       MIN(t.title) AS modern_american_internet_movie
FROM title AS t
JOIN aka_title AS at1 ON t.id = at1.movie_id
JOIN movie_info AS mi ON t.id = mi.movie_id
JOIN movie_keyword AS mk ON t.id = mk.movie_id
JOIN movie_companies AS mc ON t.id = mc.movie_id
JOIN keyword AS k ON k.id = mk.keyword_id
JOIN info_type AS it1 ON it1.id = mi.info_type_id
JOIN company_name AS cn ON cn.id = mc.company_id
JOIN company_type AS ct ON ct.id = mc.company_type_id

WHERE cn.country_code = '[us]'
  AND it1.info = 'release dates'
  AND mi.note LIKE '%internet%'
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'USA:% 199%'
       OR mi.info LIKE 'USA:% 200%')
  AND t.production_year > 1990
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = at1.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = at1.movie_id
  AND mc.movie_id = at1.movie_id
