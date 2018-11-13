SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(t.title) AS movie_title
FROM title AS t
JOIN movie_info AS mi ON t.id = mi.movie_id
JOIN movie_info_idx AS mi_idx ON t.id = mi_idx.movie_id
JOIN cast_info AS ci ON t.id = ci.movie_id
JOIN name AS n ON n.id = ci.person_id
JOIN info_type AS it1 ON it1.id = mi.info_type_id
JOIN info_type AS it2 ON it2.id = mi_idx.info_type_id
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND mi.info IN ('Horror',
                  'Action',
                  'Sci-Fi',
                  'Thriller',
                  'Crime',
                  'War')
  AND n.gender = 'm'
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id
