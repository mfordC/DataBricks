# Databricks notebook source
# Fifa19 data

# File location and type
file_location = "/FileStore/tables/fifa19.csv"

file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

import unicodedata
import sys

from pyspark.sql.functions import translate

def get_accent_lookup():
    matching_string = ""
    replace_string = ""

    for i in range(ord(" "), sys.maxunicode):
        name = unicodedata.name(chr(i), "")
        if "WITH" in name:
            try:
                base = unicodedata.lookup(name.split(" WITH")[0])
                matching_string += chr(i)
                replace_string += base
            except KeyError:
                pass
    return matching_string, replace_string

def clean_text(c):
    matching_string, replace_string = get_accent_lookup()
    return translate(c, matching_string, replace_string)

@udf
def replace_discrepancies(c):
    return c.replace(" Jr","").replace("-","").replace("ð","d")

@udf
def lastName(c):
    if c is not None and " " in c:
        c = c[(c.index(" ")+1):]
    return c  

@udf
def formatTeam(c):
    if c is not None:
        c = c.replace("Utd","United").replace("FC ","").replace("Bayern München","Bayern Munich") 
    return c
  
df = df.withColumn("Name", lower(clean_text(replace_discrepancies("Name"))))
df = df.withColumn("Last_Name", lower(clean_text(lastName(replace_discrepancies("Name")))))
df = df.withColumn("club", formatTeam("club"))


# Create a view or table

temp_table_name = "fifa19"

df.createOrReplaceTempView(temp_table_name)

display(df)

# COMMAND ----------

# Football events data

# File location and type
file_location = "/FileStore/tables/football_events.csv"

file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

@udf
def formatName(c):
    if c is not None and " " in c:
        c = c[0] + "." + c[c.index(" "):]
    return c
  
@udf
def lastName(c):
    if c is not None and " " in c:
        c = c[(c.index(" ")+1):]
    return c
  
@udf
def formatTeam(c):
    return c.replace("Utd","United").replace("FC ","").replace("Tottenham","Tottenham Hotspur")
  
  
df = df.withColumn("name", formatName("player"))
df = df.withColumn("last_name", lastName("player"))
df = df.withColumn("event_team", formatTeam("event_team"))

# Create a view or table

temp_table_name = "football_events"

df.createOrReplaceTempView(temp_table_name)

display(df)

# COMMAND ----------

# Football games data

# File location and type
file_location = "/FileStore/tables/football_games.csv"

file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# Create a view or table

temp_table_name = "football_games"

df.createOrReplaceTempView(temp_table_name)

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Perfect matches on name
# MAGIC drop table if exists player_match_perfect;
# MAGIC 
# MAGIC create table if not exists player_match_perfect as
# MAGIC select e2.player, fi2.id
# MAGIC from football_events e2
# MAGIC join (
# MAGIC     select e.player, count(distinct fi.id) num_of_ids
# MAGIC     from football_events e
# MAGIC     join fifa19 fi
# MAGIC     on e.player = fi.name
# MAGIC     group by e.player
# MAGIC     having num_of_ids = 1
# MAGIC ) inn
# MAGIC on inn.player = e2.player
# MAGIC join fifa19 fi2
# MAGIC on inn.player = fi2.name
# MAGIC group by e2.player, fi2.id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Matches on the first initial, last name, and team
# MAGIC drop table if exists player_match_first_inital_last_name_team;
# MAGIC 
# MAGIC create table if not exists player_match_first_inital_last_name_team as 
# MAGIC select e2.player, fi2.id, fi2.club
# MAGIC from (
# MAGIC    select fi.id, count(distinct e.player) num_of_player_names
# MAGIC    from football_events e
# MAGIC    join fifa19 fi
# MAGIC    on e.name = fi.name
# MAGIC    and e.event_team = fi.club
# MAGIC    left join player_match_perfect pmp
# MAGIC    on fi.id = pmp.id
# MAGIC    left join player_match_perfect pmp2
# MAGIC    on e.player = pmp2.player
# MAGIC    where pmp.id is null
# MAGIC    and pmp2.player is null
# MAGIC    group by fi.id
# MAGIC    having num_of_player_names = 1
# MAGIC ) inn
# MAGIC join fifa19 fi2
# MAGIC on inn.id = fi2.id
# MAGIC join football_events e2
# MAGIC on e2.name = fi2.name
# MAGIC and e2.event_team = fi2.club
# MAGIC group by e2.player, fi2.id, fi2.club
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Union the tables
# MAGIC drop table if exists player_id_matches_first_two_rounds;
# MAGIC 
# MAGIC create table player_id_matches_first_two_rounds as 
# MAGIC select player, id
# MAGIC from player_match_perfect
# MAGIC 
# MAGIC UNION ALL 
# MAGIC 
# MAGIC select player, id
# MAGIC from player_match_first_inital_last_name_team

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Matches on the first initial and last name
# MAGIC drop table if exists player_match_first_inital_last_name;
# MAGIC 
# MAGIC create table if not exists player_match_first_inital_last_name as 
# MAGIC select e3.player, fi3.id, fi3.club
# MAGIC from (
# MAGIC     select e2.player, e2.name, count(distinct fi2.id) num_of_ids
# MAGIC     from (
# MAGIC         select fi.id, count(distinct e.player) num_of_player_names
# MAGIC         from football_events e
# MAGIC         join fifa19 fi
# MAGIC         on e.name = fi.name
# MAGIC         left join player_id_matches_first_two_rounds pimftr
# MAGIC         on fi.id = pimftr.id
# MAGIC         left join player_id_matches_first_two_rounds pimftr2
# MAGIC         on e.player = pimftr2.player
# MAGIC         where pimftr.id is null
# MAGIC         and pimftr2.id is null
# MAGIC         group by fi.id
# MAGIC         having num_of_player_names = 1
# MAGIC     ) inn
# MAGIC     join fifa19 fi2
# MAGIC     on inn.id = fi2.id
# MAGIC     join football_events e2
# MAGIC     on e2.name = fi2.name
# MAGIC     left join player_id_matches_first_two_rounds pimftr3
# MAGIC     on e2.player = pimftr3.player
# MAGIC     where pimftr3.id is null
# MAGIC     group by e2.player, e2.name
# MAGIC     having num_of_ids = 1
# MAGIC ) inn2
# MAGIC join football_events e3
# MAGIC on inn2.player = e3.player
# MAGIC join fifa19 fi3
# MAGIC on e3.name = fi3.name
# MAGIC left join player_id_matches_first_two_rounds pimftr4
# MAGIC on fi3.id = pimftr4.id
# MAGIC where pimftr4.id is null
# MAGIC group by e3.player, fi3.id, fi3.club
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Union the tables
# MAGIC drop table if exists player_id_matches;
# MAGIC 
# MAGIC create table player_id_matches as 
# MAGIC select player, id
# MAGIC from player_id_matches_first_two_rounds
# MAGIC 
# MAGIC UNION ALL 
# MAGIC 
# MAGIC select player, id
# MAGIC from player_match_first_inital_last_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Matches by last name
# MAGIC drop table if exists player_match_last_name;
# MAGIC 
# MAGIC create table if not exists player_match_last_name as 
# MAGIC select e3.player, fi3.id, fi3.club
# MAGIC from (
# MAGIC     select e2.player, count(distinct fi2.id) num_of_ids
# MAGIC     from (
# MAGIC         select fi.id, count(distinct e.player) num_of_player_names
# MAGIC         from football_events e
# MAGIC         join fifa19 fi
# MAGIC         on e.last_name = fi.last_name
# MAGIC         left join player_id_matches pim
# MAGIC         on fi.id = pim.id
# MAGIC         left join player_id_matches pim2
# MAGIC         on e.player = pim2.player
# MAGIC         where pim.id is null
# MAGIC         and pim2.id is null
# MAGIC         group by fi.id
# MAGIC         having num_of_player_names = 1
# MAGIC     ) inn
# MAGIC     join fifa19 fi2
# MAGIC     on inn.id = fi2.id
# MAGIC     join football_events e2
# MAGIC     on e2.last_name = fi2.last_name
# MAGIC     left join player_id_matches pim3
# MAGIC     on e2.player = pim3.player
# MAGIC     where pim3.id is null
# MAGIC     group by e2.player
# MAGIC     having num_of_ids = 1
# MAGIC ) inn2
# MAGIC join football_events e3
# MAGIC on inn2.player = e3.player
# MAGIC join fifa19 fi3
# MAGIC on e3.last_name = fi3.last_name
# MAGIC left join player_id_matches pim4
# MAGIC on fi3.id = pim4.id
# MAGIC where pim4.id is null
# MAGIC group by e3.player, fi3.id, fi3.club
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Union data from these tables.
# MAGIC -- The reason we have to re-combine these is because player_match_first_inital_last_name_team needs to join on team
# MAGIC -- The other tables don't need to
# MAGIC drop table if exists player_id_matches_final;
# MAGIC 
# MAGIC create table player_id_matches_final as 
# MAGIC select player, id
# MAGIC from player_match_perfect
# MAGIC 
# MAGIC UNION ALL 
# MAGIC 
# MAGIC select player, id
# MAGIC from player_match_first_inital_last_name
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC select player, id
# MAGIC from player_match_last_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the summary table!
# MAGIC drop table if exists summary_by_player;
# MAGIC 
# MAGIC create table if not exists summary_by_player as
# MAGIC select e.player, fi.id, fi.club,fi.height,
# MAGIC fi.value, fi.wage, fi.position, fi.joined, fi.weight,
# MAGIC fi.crossing, fi.finishing, fi.HeadingAccuracy, fi.ShortPassing,
# MAGIC fi.Volleys, fi.Dribbling, fi.Curve, fi.FKAccuracy, fi.LongPassing,
# MAGIC fi.BallControl, fi.Acceleration, fi.SprintSpeed, fi.Agility,
# MAGIC fi.Reactions, fi.Balance, fi.ShotPower, fi.Jumping, fi.Stamina,
# MAGIC fi.Strength, fi.LongShots, fi.Aggression, fi.Interceptions,
# MAGIC fi.Positioning, fi.Vision, fi.Penalties, fi.Composure, fi.Marking,
# MAGIC fi.StandingTackle, fi.SlidingTackle, fi.GKDiving, fi.GKHandling,
# MAGIC fi.GKKicking, fi.GKPositioning, fi.GKReflexes,
# MAGIC sum(is_goal) sum_goals, 
# MAGIC count(distinct g.id_odsp) num_of_games,
# MAGIC max(date) max_date
# MAGIC from football_events e
# MAGIC join football_games g
# MAGIC on e.id_odsp = g.id_odsp
# MAGIC join player_id_matches_final matches
# MAGIC on e.player = matches.player
# MAGIC join fifa19 fi
# MAGIC on fi.id = matches.id
# MAGIC group by e.player, fi.id, fi.club,fi.height,
# MAGIC fi.value, fi.wage, fi.position, fi.joined, fi.weight,
# MAGIC fi.crossing, fi.finishing, fi.HeadingAccuracy, fi.ShortPassing,
# MAGIC fi.Volleys, fi.Dribbling, fi.Curve, fi.FKAccuracy, fi.LongPassing,
# MAGIC fi.BallControl, fi.Acceleration, fi.SprintSpeed, fi.Agility,
# MAGIC fi.Reactions, fi.Balance, fi.ShotPower, fi.Jumping, fi.Stamina,
# MAGIC fi.Strength, fi.LongShots, fi.Aggression, fi.Interceptions,
# MAGIC fi.Positioning, fi.Vision, fi.Penalties, fi.Composure, fi.Marking,
# MAGIC fi.StandingTackle, fi.SlidingTackle, fi.GKDiving, fi.GKHandling,
# MAGIC fi.GKKicking, fi.GKPositioning, fi.GKReflexes
# MAGIC 
# MAGIC UNION ALL 
# MAGIC 
# MAGIC select e.player, fi.id, fi.club,fi.height,
# MAGIC fi.value, fi.wage, fi.position, fi.joined, fi.weight,
# MAGIC fi.crossing, fi.finishing, fi.HeadingAccuracy, fi.ShortPassing,
# MAGIC fi.Volleys, fi.Dribbling, fi.Curve, fi.FKAccuracy, fi.LongPassing,
# MAGIC fi.BallControl, fi.Acceleration, fi.SprintSpeed, fi.Agility,
# MAGIC fi.Reactions, fi.Balance, fi.ShotPower, fi.Jumping, fi.Stamina,
# MAGIC fi.Strength, fi.LongShots, fi.Aggression, fi.Interceptions,
# MAGIC fi.Positioning, fi.Vision, fi.Penalties, fi.Composure, fi.Marking,
# MAGIC fi.StandingTackle, fi.SlidingTackle, fi.GKDiving, fi.GKHandling,
# MAGIC fi.GKKicking, fi.GKPositioning, fi.GKReflexes,
# MAGIC sum(is_goal) sum_goals, 
# MAGIC count(distinct g.id_odsp) num_of_games,
# MAGIC max(date) max_date
# MAGIC from football_events e
# MAGIC join football_games g
# MAGIC on e.id_odsp = g.id_odsp
# MAGIC join player_match_first_inital_last_name_team matches
# MAGIC on e.player = matches.player
# MAGIC and e.event_team = matches.club
# MAGIC join fifa19 fi
# MAGIC on fi.id = matches.id
# MAGIC group by e.player, fi.id, fi.club,fi.height,
# MAGIC fi.value, fi.wage, fi.position, fi.joined, fi.weight,
# MAGIC fi.crossing, fi.finishing, fi.HeadingAccuracy, fi.ShortPassing,
# MAGIC fi.Volleys, fi.Dribbling, fi.Curve, fi.FKAccuracy, fi.LongPassing,
# MAGIC fi.BallControl, fi.Acceleration, fi.SprintSpeed, fi.Agility,
# MAGIC fi.Reactions, fi.Balance, fi.ShotPower, fi.Jumping, fi.Stamina,
# MAGIC fi.Strength, fi.LongShots, fi.Aggression, fi.Interceptions,
# MAGIC fi.Positioning, fi.Vision, fi.Penalties, fi.Composure, fi.Marking,
# MAGIC fi.StandingTackle, fi.SlidingTackle, fi.GKDiving, fi.GKHandling,
# MAGIC fi.GKKicking, fi.GKPositioning, fi.GKReflexes

# COMMAND ----------

# MAGIC %sql
# MAGIC --Goals vs height
# MAGIC select height, percentile(sum_goals,0.5) median_goals
# MAGIC from summary_by_player
# MAGIC where sum_goals > 10
# MAGIC group by height
# MAGIC order by height

# COMMAND ----------

# MAGIC %sql
# MAGIC --histogram for 6'5
# MAGIC select height, sum_goals
# MAGIC from summary_by_player
# MAGIC where sum_goals > 10
# MAGIC and height = "6'5"

# COMMAND ----------

# MAGIC %sql
# MAGIC --Goals vs position
# MAGIC select position, percentile(sum_goals,0.5) median_goals
# MAGIC from summary_by_player
# MAGIC where sum_goals > 10
# MAGIC group by position
# MAGIC order by median_goals desc
# MAGIC limit 15

# COMMAND ----------

# MAGIC %sql
# MAGIC --Goals per game
# MAGIC select player, sum_goals, num_of_games, sum_goals / num_of_games goals_per_game
# MAGIC from summary_by_player
# MAGIC where num_of_games > 10
# MAGIC order by goals_per_game desc

# COMMAND ----------

# MAGIC %sql
# MAGIC --Goals per game by club
# MAGIC select club, sum(sum_goals / num_of_games) goals_per_game
# MAGIC from summary_by_player
# MAGIC where num_of_games > 10
# MAGIC group by club
# MAGIC order by goals_per_game desc

# COMMAND ----------

# MAGIC %sql
# MAGIC --Dedupe verification
# MAGIC select id, count(*) c
# MAGIC from summary_by_player
# MAGIC group by id
# MAGIC having c > 1
# MAGIC order by c desc

# COMMAND ----------

# MAGIC %sql
# MAGIC --Dedupe verification
# MAGIC select player, count(*) c
# MAGIC from summary_by_player
# MAGIC group by player
# MAGIC having c > 1
# MAGIC order by c desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- There are 2 player names that repeat
# MAGIC -- These are not duplicates because we joined on club for those records
# MAGIC select *
# MAGIC from summary_by_player
# MAGIC where player = 'rafael' or player = 'rafinha'
# MAGIC order by player, id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Players in events data not in merged dataset. Common reasons why:
# MAGIC -- Player retired
# MAGIC -- Ambiguous name
# MAGIC -- Name format is completely different from other dataset
# MAGIC -- Player is missing from Fifa19 dataset
# MAGIC select e.player, sum(e.is_goal) sum_goals, max(g.date) max_date 
# MAGIC from football_events e
# MAGIC join football_games g
# MAGIC on e.id_odsp = g.id_odsp
# MAGIC left join summary_by_player s
# MAGIC on e.player = s.player
# MAGIC where s.player is null
# MAGIC group by e.player
# MAGIC order by max_date desc, sum_goals desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- # of players in merged dataset
# MAGIC select count(*)
# MAGIC from summary_by_player

# COMMAND ----------

# MAGIC %sql
# MAGIC -- # of players with goals in Events data that aren't in merged data
# MAGIC select count(distinct player) num_of_players
# MAGIC from (
# MAGIC     select e.player, sum(is_goal) sum_goals
# MAGIC     from football_events e
# MAGIC     join football_games g
# MAGIC     on e.id_odsp = g.id_odsp
# MAGIC     left join summary_by_player s
# MAGIC     on e.player = s.player
# MAGIC     where s.player is null
# MAGIC     group by e.player
# MAGIC     having sum_goals > 0
# MAGIC ) players_with_goals;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- # of players in Fifa19 data that aren't in merged data
# MAGIC select count(distinct fi.id) num_of_players
# MAGIC from fifa19 fi
# MAGIC left join summary_by_player s
# MAGIC on fi.id = s.id
# MAGIC where s.player is null
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Players in Fifa19 data that aren't in merged data. Common reasons why:
# MAGIC -- Name format is completely different from other dataset
# MAGIC -- Ambiguous name
# MAGIC -- Player changed teams
# MAGIC select fi.*
# MAGIC from fifa19 fi
# MAGIC left join summary_by_player s
# MAGIC on fi.id = s.id
# MAGIC where s.player is null
# MAGIC ;
