# Sparkify: Database population

This project populates a database with transactional data of users playing songs on Sparkify.

This database serves as data source for analytical queries on user behaviour on the platform. Specifically, the tables are designed to optimize queries on song play analysis. 

## Usage
to initialize the tables and fill them with the data provided in the JSON dumps, you can run the following commands

```bash
python create_tables.py
python etl.py
```

## Examples queries for song analysis

Count how often each song in the library has been played
```sql
SELECT song_id, COUNT(level) 
FROM songplays 
GROUP BY song_id;
```

Find out through what platform/tool your app is accessed most
```sql
SELECT user_agent, COUNT(level) 
FROM songplays 
GROUP BY user_agent;
```

inspect at what time of the day Sparkify is used most
```sql
SELECT start_time, COUNT(level) 
FROM songplays 
GROUP BY start_time 
ORDER BY start_time;
```


Adding in some additional information from other tables
```sql
SELECT ts, start_time, artist_name, title 
FROM songplays 
JOIN artists ON songplays.artist_id=artists.artist_id 
JOIN songs ON songplays.song_id=songs.song_id;
```

## Database Schema
The database schema is designed to optimize queries on song play analysis. Hence, the main (fact) table that should be queried is `songplays`. Additional information from other dimension tables can be added by joining the tables on the corresponding id `artist_id`, `user_id`, `song_id` and `ts`. 
