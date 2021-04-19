--rename this file to create-{genre}.sql
WITH genredata AS (
SELECT REPLACE ( m.title , '"' , '' ) as title, r.rating
FROM {db}.movies m
INNER JOIN (SELECT rating, movieId FROM {db}.ratings) r on m.movieId = r.movieId WHERE REGEXP_LIKE (genres, '{genre}')
  )
SELECT title, replace(substr(trim(title),-5),')','') as year, AVG(rating) as avrating from genredata GROUP BY title ORDER BY year DESC,  title ASC;