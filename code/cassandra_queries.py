# CREATE KEYSPACE
sparkify_keyspace_create = ("""
    CREATE KEYSPACE IF NOT EXISTS sparkifyks
    WITH REPLICATION = { 'class':'SimpleStrategy', 'replication_factor':1 }
    """)


# DROP TABLES
songs_and_sessions_table_drop = "DROP TABLE IF EXISTS songs_and_sessions"
users_and_songs_table_drop = "DROP TABLE IF EXISTS users_and_songs"
music_app_history_table_drop = "DROP TABLE IF EXISTS music_app_history"


# CREATE TABLES
songs_and_sessions_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs_and_sessions (
        sessionId int,
        itemInSession int,
        artistName text,
        songName text,
        songLength float,
        PRIMARY KEY(sessionId, itemInSession))
    """)

users_and_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS users_and_songs (
        userId int,
        sessionId int,
        artistName text,
        songName text,
        firstName text,
        lastName text,
        itemInSession int,
        PRIMARY KEY((userId, sessionId), itemInSession))
    """)

music_app_history_table_create = ("""
    CREATE TABLE IF NOT EXISTS music_app_history (
        songName text,
        firstName text,
        lastName text,
        userId int,
        PRIMARY KEY(songName, userId))
    """)


# INSERT RECORDS
songs_and_sessions_table_insert = ("""
    INSERT INTO songs_and_sessions (
    sessionId, itemInSession, artistName, songName, songLength)
    VALUES (%s, %s, %s, %s, %s)
""")

users_and_songs_table_insert = ("""
    INSERT INTO users_and_songs (
    userId, sessionId, artistName, songName, firstName, lastName,
    itemInSession)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

music_app_history_table_insert = ("""
    INSERT INTO music_app_history (
    songName, firstName, lastName, userId)
    VALUES (%s, %s, %s, %s)
""")


# FIND QUERIES
songs_and_sessions_table_find = ("""
    SELECT artistName, songName, songLength
    FROM songs_and_sessions WHERE sessionId = 338 AND itemInSession = 4
""")

users_and_songs_table_find = ("""
    SELECT itemInSession, artistName, songName,
    firstName, lastName FROM users_and_songs WHERE userId = 10 AND
    sessionId = 182
""")

music_app_history_table_find = ("""
    SELECT firstName, lastName FROM
    music_app_history WHERE songName = 'All Hands Against His Own'
""")


# QUERY LISTS
create_table_queries_list = [songs_and_sessions_table_create,
                             users_and_songs_table_create,
                             music_app_history_table_create]
drop_table_queries_list = [songs_and_sessions_table_drop,
                           users_and_songs_table_drop,
                           music_app_history_table_drop]
