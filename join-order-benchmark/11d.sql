SELECT MIN(cn.name) AS from_company,
       MIN(mc.note) AS production_note,
       MIN(t.title) AS movie_based_on_book
FROM link_type AS lt
JOIN movie_link AS ml ON lt.id = ml.link_type_id
JOIN title AS t ON ml.movie_id = t.id
JOIN movie_keyword AS mk ON t.id = mk.movie_id AND ml.movie_id = mk.movie_id
JOIN keyword AS k ON mk.keyword_id = k.id
JOIN movie_companies AS mc ON t.id = mc.movie_id AND ml.movie_id = mc.movie_id AND mk.movie_id = mc.movie_id
JOIN company_type AS ct ON mc.company_type_id = ct.id
JOIN company_name AS cn ON mc.company_id = cn.id
WHERE cn.country_code !='[pl]'
  AND ct.kind != 'production companies'
  AND ct.kind IS NOT NULL
  AND k.keyword IN ('sequel',
                    'revenge',
                    'based-on-novel')
  AND mc.note IS NOT NULL
  AND t.production_year > 1950
