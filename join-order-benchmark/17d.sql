SELECT MIN(n.name) AS member_in_charnamed_movie
FROM name AS n
JOIN cast_info as ci ON n.id = ci.person_id
JOIN title AS t ON ci.movie_id = t.id
JOIN movie_keyword AS mk ON t.id = mk.movie_id
JOIN keyword AS k ON mk.keyword_id = k.id
JOIN movie_companies AS mc ON t.id = mc.movie_id
JOIN company_name AS cn ON mc.company_id = cn.id

WHERE k.keyword ='character-name-in-title'
  AND n.name LIKE '%Bert%'
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id
