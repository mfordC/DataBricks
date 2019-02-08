-- Code continued from Soccer.py

# COMMAND ----------

 %sql
 -- Perfect matches on name
 drop table if exists player_match_perfect;
 
 create table if not exists player_match_perfect as
 select e2.player, fi2.id
 from football_events e2
 join (
     select e.player, count(distinct fi.id) num_of_ids
     from football_events e
     join fifa19 fi
     on e.player = fi.name
     group by e.player
     having num_of_ids = 1
 ) inn
 on inn.player = e2.player
 join fifa19 fi2
 on inn.player = fi2.name
 group by e2.player, fi2.id

# COMMAND ----------

 %sql
 -- Matches on the first initial, last name, and team
 drop table if exists player_match_first_inital_last_name_team;
 
 create table if not exists player_match_first_inital_last_name_team as 
 select e2.player, fi2.id, fi2.club
 from (
    select fi.id, count(distinct e.player) num_of_player_names
    from football_events e
    join fifa19 fi
    on e.name = fi.name
    and e.event_team = fi.club
    left join player_match_perfect pmp
    on fi.id = pmp.id
    left join player_match_perfect pmp2
    on e.player = pmp2.player
    where pmp.id is null
    and pmp2.player is null
    group by fi.id
    having num_of_player_names = 1
 ) inn
 join fifa19 fi2
 on inn.id = fi2.id
 join football_events e2
 on e2.name = fi2.name
 and e2.event_team = fi2.club
 group by e2.player, fi2.id, fi2.club
 ;

# COMMAND ----------

 %sql
 -- Union the tables
 drop table if exists player_id_matches_first_two_rounds;
 
 create table player_id_matches_first_two_rounds as 
 select player, id
 from player_match_perfect
 
 UNION ALL 
 
 select player, id
 from player_match_first_inital_last_name_team

# COMMAND ----------

 %sql
 -- Matches on the first initial and last name
 drop table if exists player_match_first_inital_last_name;
 
 create table if not exists player_match_first_inital_last_name as 
 select e3.player, fi3.id, fi3.club
 from (
     select e2.player, e2.name, count(distinct fi2.id) num_of_ids
     from (
         select fi.id, count(distinct e.player) num_of_player_names
         from football_events e
         join fifa19 fi
         on e.name = fi.name
         left join player_id_matches_first_two_rounds pimftr
         on fi.id = pimftr.id
         left join player_id_matches_first_two_rounds pimftr2
         on e.player = pimftr2.player
         where pimftr.id is null
         and pimftr2.id is null
         group by fi.id
         having num_of_player_names = 1
     ) inn
     join fifa19 fi2
     on inn.id = fi2.id
     join football_events e2
     on e2.name = fi2.name
     left join player_id_matches_first_two_rounds pimftr3
     on e2.player = pimftr3.player
     where pimftr3.id is null
     group by e2.player, e2.name
     having num_of_ids = 1
 ) inn2
 join football_events e3
 on inn2.player = e3.player
 join fifa19 fi3
 on e3.name = fi3.name
 left join player_id_matches_first_two_rounds pimftr4
 on fi3.id = pimftr4.id
 where pimftr4.id is null
 group by e3.player, fi3.id, fi3.club
 ;

# COMMAND ----------

 %sql
 -- Union the tables
 drop table if exists player_id_matches;
 
 create table player_id_matches as 
 select player, id
 from player_id_matches_first_two_rounds
 
 UNION ALL 
 
 select player, id
 from player_match_first_inital_last_name

# COMMAND ----------

 %sql
 -- Matches by last name
 drop table if exists player_match_last_name;
 
 create table if not exists player_match_last_name as 
 select e3.player, fi3.id, fi3.club
 from (
     select e2.player, count(distinct fi2.id) num_of_ids
     from (
         select fi.id, count(distinct e.player) num_of_player_names
         from football_events e
         join fifa19 fi
         on e.last_name = fi.last_name
         left join player_id_matches pim
         on fi.id = pim.id
         left join player_id_matches pim2
         on e.player = pim2.player
         where pim.id is null
         and pim2.id is null
         group by fi.id
         having num_of_player_names = 1
     ) inn
     join fifa19 fi2
     on inn.id = fi2.id
     join football_events e2
     on e2.last_name = fi2.last_name
     left join player_id_matches pim3
     on e2.player = pim3.player
     where pim3.id is null
     group by e2.player
     having num_of_ids = 1
 ) inn2
 join football_events e3
 on inn2.player = e3.player
 join fifa19 fi3
 on e3.last_name = fi3.last_name
 left join player_id_matches pim4
 on fi3.id = pim4.id
 where pim4.id is null
 group by e3.player, fi3.id, fi3.club
 ;

# COMMAND ----------

 %sql
 -- Union data from these tables.
 -- The reason we have to re-combine these is because player_match_first_inital_last_name_team needs to join on team
 -- The other tables don't need to
 drop table if exists player_id_matches_final;
 
 create table player_id_matches_final as 
 select player, id
 from player_match_perfect
 
 UNION ALL 
 
 select player, id
 from player_match_first_inital_last_name
 
 UNION ALL
 
 select player, id
 from player_match_last_name

# COMMAND ----------

 %sql
 -- Create the summary table!
 drop table if exists summary_by_player;
 
 create table if not exists summary_by_player as
 select e.player, fi.id, fi.club,fi.height,
 fi.value, fi.wage, fi.position, fi.joined, fi.weight,
 fi.crossing, fi.finishing, fi.HeadingAccuracy, fi.ShortPassing,
 fi.Volleys, fi.Dribbling, fi.Curve, fi.FKAccuracy, fi.LongPassing,
 fi.BallControl, fi.Acceleration, fi.SprintSpeed, fi.Agility,
 fi.Reactions, fi.Balance, fi.ShotPower, fi.Jumping, fi.Stamina,
 fi.Strength, fi.LongShots, fi.Aggression, fi.Interceptions,
 fi.Positioning, fi.Vision, fi.Penalties, fi.Composure, fi.Marking,
 fi.StandingTackle, fi.SlidingTackle, fi.GKDiving, fi.GKHandling,
 fi.GKKicking, fi.GKPositioning, fi.GKReflexes,
 sum(is_goal) sum_goals, 
 count(distinct g.id_odsp) num_of_games,
 max(date) max_date
 from football_events e
 join football_games g
 on e.id_odsp = g.id_odsp
 join player_id_matches_final matches
 on e.player = matches.player
 join fifa19 fi
 on fi.id = matches.id
 group by e.player, fi.id, fi.club,fi.height,
 fi.value, fi.wage, fi.position, fi.joined, fi.weight,
 fi.crossing, fi.finishing, fi.HeadingAccuracy, fi.ShortPassing,
 fi.Volleys, fi.Dribbling, fi.Curve, fi.FKAccuracy, fi.LongPassing,
 fi.BallControl, fi.Acceleration, fi.SprintSpeed, fi.Agility,
 fi.Reactions, fi.Balance, fi.ShotPower, fi.Jumping, fi.Stamina,
 fi.Strength, fi.LongShots, fi.Aggression, fi.Interceptions,
 fi.Positioning, fi.Vision, fi.Penalties, fi.Composure, fi.Marking,
 fi.StandingTackle, fi.SlidingTackle, fi.GKDiving, fi.GKHandling,
 fi.GKKicking, fi.GKPositioning, fi.GKReflexes
 
 UNION ALL 
 
 select e.player, fi.id, fi.club,fi.height,
 fi.value, fi.wage, fi.position, fi.joined, fi.weight,
 fi.crossing, fi.finishing, fi.HeadingAccuracy, fi.ShortPassing,
 fi.Volleys, fi.Dribbling, fi.Curve, fi.FKAccuracy, fi.LongPassing,
 fi.BallControl, fi.Acceleration, fi.SprintSpeed, fi.Agility,
 fi.Reactions, fi.Balance, fi.ShotPower, fi.Jumping, fi.Stamina,
 fi.Strength, fi.LongShots, fi.Aggression, fi.Interceptions,
 fi.Positioning, fi.Vision, fi.Penalties, fi.Composure, fi.Marking,
 fi.StandingTackle, fi.SlidingTackle, fi.GKDiving, fi.GKHandling,
 fi.GKKicking, fi.GKPositioning, fi.GKReflexes,
 sum(is_goal) sum_goals, 
 count(distinct g.id_odsp) num_of_games,
 max(date) max_date
 from football_events e
 join football_games g
 on e.id_odsp = g.id_odsp
 join player_match_first_inital_last_name_team matches
 on e.player = matches.player
 and e.event_team = matches.club
 join fifa19 fi
 on fi.id = matches.id
 group by e.player, fi.id, fi.club,fi.height,
 fi.value, fi.wage, fi.position, fi.joined, fi.weight,
 fi.crossing, fi.finishing, fi.HeadingAccuracy, fi.ShortPassing,
 fi.Volleys, fi.Dribbling, fi.Curve, fi.FKAccuracy, fi.LongPassing,
 fi.BallControl, fi.Acceleration, fi.SprintSpeed, fi.Agility,
 fi.Reactions, fi.Balance, fi.ShotPower, fi.Jumping, fi.Stamina,
 fi.Strength, fi.LongShots, fi.Aggression, fi.Interceptions,
 fi.Positioning, fi.Vision, fi.Penalties, fi.Composure, fi.Marking,
 fi.StandingTackle, fi.SlidingTackle, fi.GKDiving, fi.GKHandling,
 fi.GKKicking, fi.GKPositioning, fi.GKReflexes

# COMMAND ----------

 %sql
 --Goals vs height
 select height, percentile(sum_goals,0.5) median_goals
 from summary_by_player
 where sum_goals > 10
 group by height
 order by height

# COMMAND ----------

 %sql
 --histogram for 6'5
 select height, sum_goals
 from summary_by_player
 where sum_goals > 10
 and height = "6'5"

# COMMAND ----------

 %sql
 --Goals vs position
 select position, percentile(sum_goals,0.5) median_goals
 from summary_by_player
 where sum_goals > 10
 group by position
 order by median_goals desc
 limit 15

# COMMAND ----------

 %sql
 --Goals per game
 select player, sum_goals, num_of_games, sum_goals / num_of_games goals_per_game
 from summary_by_player
 where num_of_games > 10
 order by goals_per_game desc

# COMMAND ----------

 %sql
 --Goals per game by club
 select club, sum(sum_goals / num_of_games) goals_per_game
 from summary_by_player
 where num_of_games > 10
 group by club
 order by goals_per_game desc

# COMMAND ----------

 %sql
 --Dedupe verification
 select id, count(*) c
 from summary_by_player
 group by id
 having c > 1
 order by c desc

# COMMAND ----------

 %sql
 --Dedupe verification
 select player, count(*) c
 from summary_by_player
 group by player
 having c > 1
 order by c desc

# COMMAND ----------

 %sql
 -- There are 2 player names that repeat
 -- These are not duplicates because we joined on club for those records
 select *
 from summary_by_player
 where player = 'rafael' or player = 'rafinha'
 order by player, id

# COMMAND ----------

 %sql
 -- Players in events data not in merged dataset. Common reasons why:
 -- Player retired
 -- Ambiguous name
 -- Name format is completely different from other dataset
 -- Player is missing from Fifa19 dataset
 select e.player, sum(e.is_goal) sum_goals, max(g.date) max_date 
 from football_events e
 join football_games g
 on e.id_odsp = g.id_odsp
 left join summary_by_player s
 on e.player = s.player
 where s.player is null
 group by e.player
 order by max_date desc, sum_goals desc

# COMMAND ----------

 %sql
 -- # of players in merged dataset
 select count(*)
 from summary_by_player

# COMMAND ----------

 %sql
 -- # of players with goals in Events data that aren't in merged data
 select count(distinct player) num_of_players
 from (
     select e.player, sum(is_goal) sum_goals
     from football_events e
     join football_games g
     on e.id_odsp = g.id_odsp
     left join summary_by_player s
     on e.player = s.player
     where s.player is null
     group by e.player
     having sum_goals > 0
 ) players_with_goals;

# COMMAND ----------

 %sql
 -- # of players in Fifa19 data that aren't in merged data
 select count(distinct fi.id) num_of_players
 from fifa19 fi
 left join summary_by_player s
 on fi.id = s.id
 where s.player is null
 ;

# COMMAND ----------

 %sql
 -- Players in Fifa19 data that aren't in merged data. Common reasons why:
 -- Name format is completely different from other dataset
 -- Ambiguous name
 -- Player changed teams
 select fi.*
 from fifa19 fi
 left join summary_by_player s
 on fi.id = s.id
 where s.player is null
 ;