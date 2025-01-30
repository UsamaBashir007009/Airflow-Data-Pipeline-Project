songplays_count_check = ("""
        SELECT COUNT(*) FROM songplays;
""")

songplays_nulls_check = ("""
    SELECT COUNT(*) FROM songplays WHERE playid IS NULL;
""")

users_count_check = ("""
    SELECT COUNT(*) FROM users;
""")

users_nulls_check = ("""
    SELECT COUNT(*) FROM users WHERE userid IS NULL;
""")

songs_count_check = ("""
    SELECT COUNT(*) FROM songs;
""")

songs_nulls_check = ("""
    SELECT COUNT(*) FROM songs WHERE songid IS NULL;
""")

artists_count_check = ("""
    SELECT COUNT(*) FROM artists;
""")

artists_nulls_check = ("""
    SELECT COUNT(*) FROM artists WHERE artistid IS NULL;
""")

time_count_check = ("""
    SELECT COUNT(*) FROM time;
""")

time_nulls_check = ("""
    SELECT COUNT(*) FROM time WHERE start_time IS NULL;
""")

data_quality_count_check_queries = [
    songplays_count_check, 
    users_count_check,
    songs_count_check, artists_count_check, time_count_check
]

data_quality_null_check_queries = [
    songplays_nulls_check,
    users_nulls_check, songs_nulls_check,
    artists_nulls_check, time_nulls_check
]
