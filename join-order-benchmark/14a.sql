SELECT MIN(mi_idx.info) AS rating,
       MIN(t.title) AS northern_dark_movie
FROM kind_type AS kt
JOIN title as t ON kt.id = t.kind_id
JOIN movie_info AS mi ON t.id = mi.movie_id
JOIN movie_keyword AS mk ON t.id = mk.movie_id
JOIN movie_info_idx AS mi_idx ON t.id = mi_idx.movie_id
JOIN keyword AS k ON k.id = mk.keyword_id
JOIN info_type AS it1 ON it1.id = mi.info_type_id
JOIN info_type AS it2 ON it2.id = mi_idx.info_type_id
WHERE it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind = 'movie'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '8.5'
  AND t.production_year > 2010
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id
