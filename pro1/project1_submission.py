# Collaborators: Ziqi Wang 06280260

def query_one():
    """Query for Stanford's venue"""
    return """
       SELECT
  venue_name,
  venue_capacity
FROM
  `bigquery-public-data.ncaa_basketball.mbb_teams`
WHERE
  school_ncaa = 'Stanford'
LIMIT
  100
    """


def query_two():
    """Query for games in Stanford's venue"""
    return """
      SELECT
  COUNT(season) AS games_at_stanford_season_2013
FROM
  `bigquery-public-data.ncaa_basketball.mbb_games_sr`
WHERE
  season = 2013
  AND venue_name = 'Maples Pavilion'
LIMIT
  100
    """


def query_three():
    """Query for maximum-red-intensity teams"""
    return """
       SELECT
   team_colors.market,team_colors.color
FROM
  `bigquery-public-data.ncaa_basketball.team_colors` team_colors
JOIN
  (SELECT MAX(LOWER(SUBSTR(team_colors_2.color,1,3))) AS color_max
    FROM `bigquery-public-data.ncaa_basketball.team_colors` team_colors_2
)

ON LOWER(SUBSTR(team_colors.color,1,3)) = color_max
ORDER BY   team_colors.market
LIMIT
  100
    """



def query_four():
    """Query for Stanford's wins at home"""
    return """
       SELECT COUNT(*) AS number,
ROUND(AVG(h_points),2) AS avg_stanford,
ROUND(AVG(a_points),2) AS avg_opponent
FROM `bigquery-public-data.ncaa_basketball.mbb_games_sr` 
WHERE
h_market='Stanford'
AND h_points_game > a_points_game
AND 2013 <= season
AND 2017 >= season
LIMIT 100
    """


def query_five():
    """Query for players for birth city"""
    return """
      SELECT
  COUNT(*)
FROM (
  SELECT
    birthplace_state,
    birthplace_city,
    team_id
  FROM
    `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
  GROUP BY
    player_id,
    birthplace_state,
    birthplace_city,
    team_id),
  `bigquery-public-data.ncaa_basketball.mbb_teams` c2
WHERE
  c2.venue_state= birthplace_state
  AND c2.venue_city= birthplace_city
  AND c2.id = team_id
LIMIT
  1000
    """

def query_six():
    """Query for biggest blowout"""
    return """
       SELECT
  x.win_name, x.lose_name, x.win_pts, x.lose_pts, margin
FROM
  `bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games` x
JOIN (
  SELECT
    MAX(y.win_pts-y.lose_pts) AS margin
  FROM
    `bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games` y
    )
ON
  (x.win_pts-x.lose_pts)=margin
LIMIT
  10
    """

def query_seven():
    """Query for historical upset percentage"""
    return """
       SELECT
  ROUND(100*(
  SELECT
    COUNT(*) AS setup
  FROM
    `bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games` 
  WHERE
    win_seed > lose_seed
    )
    /
    (
  SELECT
    COUNT(*)
  FROM
    `bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games`
    ),2)
    AS percentage
LIMIT
  100
    """

def query_eight():
    """Query for teams with same states and colors"""
    return """
       SELECT
  TeamA,
  TeamB,
  state as venue_state
FROM (
  SELECT
    c1.market AS a,
    c2.market AS b
  FROM
    `bigquery-public-data.ncaa_basketball.team_colors` c1
  JOIN
    `bigquery-public-data.ncaa_basketball.team_colors` c2
  ON
    c1.color=c2.color
    AND c1.market<c2.market)
JOIN (
  SELECT
    c3.market AS c,
    c4.market AS d,
    c3.name AS TeamA,
    c4.name AS TeamB,
    c3.venue_state AS state
  FROM
    `bigquery-public-data.ncaa_basketball.mbb_teams` c3
  JOIN
    `bigquery-public-data.ncaa_basketball.mbb_teams` c4
  ON
    c3.venue_state = c4.venue_state
    AND c3.market<c4.market)
ON
  a = c
  AND b=d
ORDER BY TeamA
LIMIT
  100
    """

def query_nine():
    """Query for teams with lots of high-scorers"""
    return """
       SELECT
  team_market,
  COUNT(DISTINCT player_id) num_players
FROM (
  SELECT
    player_id,
    team_market,
    game_id
  FROM
    `bigquery-public-data.ncaa_basketball.mbb_pbp_sr`
  WHERE
    period = 1
  GROUP BY
    player_id,
    team_market,
    game_id
  HAVING
    SUM( points_scored ) >= 15)
GROUP BY
  team_market
HAVING
  num_players > 5
ORDER BY
  num_players DESC,
  team_market
LIMIT
  100
    """

def query_ten():
    """Query for top geographical locations"""
    return """
       SELECT
  birthplace_city as city,
  birthplace_state as state,
  birthplace_country as country,
  SUM(points) as total_points
FROM
  `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
WHERE
  team_market= 'Stanford'
GROUP BY
  birthplace_city, birthplace_state,birthplace_country
ORDER BY total_points DESC
LIMIT
  3
    """

def query_eleven():
    """Query for highest-winner teams"""
    return """
       SELECT
  team_name,
  COUNT(*) AS top_performer_count
FROM (
  SELECT
    DISTINCT c2.season,
    c2.market AS team_name
  FROM (
    SELECT
      season AS a,
      MAX(wins) AS win_max
    FROM
      `bigquery-public-data.ncaa_basketball.mbb_historical_teams_seasons`
    WHERE
      season<=2000
      AND season >=1900
    GROUP BY
      season ),
    `bigquery-public-data.ncaa_basketball.mbb_historical_teams_seasons` c2
  WHERE
    win_max = c2.wins
    AND a = c2.season
    AND c2.market IS NOT NULL
  ORDER BY
    c2.season)
GROUP BY
  team_name
ORDER BY
  top_performer_count DESC,
  team_name
LIMIT
  150
    """

