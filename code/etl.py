
# Import Python packages
import pandas as pd
import cassandra
from cassandra.cluster import Cluster
import os
# import logging
from cassandra_queries import sparkify_keyspace_create
from cassandra_queries import create_table_queries_list
from cassandra_queries import drop_table_queries_list
from cassandra_queries import songs_and_sessions_table_insert
from cassandra_queries import users_and_songs_table_insert
from cassandra_queries import music_app_history_table_insert

# Create and configure logger
# logging.basicConfig(filename=os.getcwd()+"/logs/"+"etl.log",
#                     format='%(asctime)s %(level) %(message)s',
#                     filemode='w')
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)


class SparkifyEventsDBSetup:

    def __init__(self):
        self.hostname = "127.0.0.1"
        self.keyspace_name = "sparkifyks"
        self.event_data_file_path = os.getcwd()+"/data/event_datafile_new.csv"
        self.songs_and_sessions_cols = ['sessionId',
                                        'itemInSession',
                                        'artist',
                                        'song',
                                        'length']
        self.users_and_songs_cols = ['userId',
                                     'sessionId',
                                     'artist',
                                     'song',
                                     'firstName',
                                     'lastName',
                                     'itemInSession']
        self.music_app_history_cols = ['song',
                                       'firstName',
                                       'lastName',
                                       'userId']

    def create_keyspace(self):
        """
        Description: This function is responsible for creating the sparkifyks
        keyspace and creating a session for the same.

        Arguments:
            None

        Returns:
            session: the session object.
            cluster: the cassandra cluster object.
        """
        try:
            cluster = Cluster([self.hostname])

            # To establish connection and execute queries, we need a session
            session = cluster.connect()

            # Create the sparkifyks keyspace
            session.execute(sparkify_keyspace_create)
        except Exception as e:
            print(e)
        return session, cluster

    def set_keyspace(self, session):
        """
        Description: Function to set the keyspace to the one created above.

        Arguments:
            session: the session object.

        Returns:
            None.
        """
        try:
            session.set_keyspace(self.keyspace_name)
        except Exception as e:
            print(e)

    def create_all_tables(self, session):
        """
        Description: Function to create the tables songs_and_sessions,
        users_and_songs, and music_app_history.

        Arguments:
            session: the session object.

        Returns:
            None.
        """
        try:
            for create_table_query in create_table_queries_list:
                session.execute(create_table_query)
        except Exception as e:
            print("Error while executing the create table query:\n",
                  create_table_query)
            print(e)

    def create_event_data_frame(self):
        """
        Description: Function to read the input CSV file and load it into a
        pandas dataframe.

        Arguments:
            None.

        Returns:
            df_event_data: dataframe object containing the events data.
        """
        df_event_data = pd.read_csv(self.event_data_file_path)
        return df_event_data

    def process_songs_and_sessions_data(self, session, df_event_data):
        """
        Description: Function to insert the relevant events data into the
        songs_and_sessions table.

        Arguments:
            session: the session object.
            df_event_data: dataframe object containing the events data.

        Returns:
            df_event_data: dataframe object containing the events data.
        """
        try:
            # Extract the relevant columns alone from the master dataframe
            df_songs_and_sessions = df_event_data[self.songs_and_sessions_cols]
            # Loop through the rows in the new dataframe
            for index, row in df_songs_and_sessions.iterrows():
                # Insert the records into the songs_and_sessions table
                session.execute(songs_and_sessions_table_insert,
                                (int(row['sessionId']),
                                 int(row['itemInSession']),
                                 row['artist'], row['song'],
                                 float(row['length'])))

        except Exception as e:
            print("Error while inserting data into songs_and_sessions table")
            print(e)

    def process_users_and_songs_data(self, session, df_event_data):
        """
        Description: Function to insert the relevant events data into the
        users_and_songs table.

        Arguments:
            session: the session object.
            df_event_data: dataframe object containing the events data.

        Returns:
            None.
        """
        try:
            # Extract the relevant columns alone from the master dataframe
            df_users_and_songs = df_event_data[self.users_and_songs_cols]
            # Loop through the rows in the new dataframe
            for index, row in df_users_and_songs.iterrows():
                # Insert the records into the users_and_songs table
                session.execute(users_and_songs_table_insert,
                                (int(row['userId']), int(row['sessionId']),
                                 row['artist'], row['song'], row['firstName'],
                                 row['lastName'], int(row['itemInSession'])))
        except Exception as e:
            print("Error while inserting data into users_and_songs table")
            print(e)

    def process_music_app_history_data(self, session, df_event_data):
        """
        Description: Function to insert the relevant events data into the
        music_app_history table.

        Arguments:
            session: the session object.
            df_event_data: dataframe object containing the events data.

        Returns:
            None.
        """
        try:
            # Extract the relevant columns alone from the master dataframe
            df_music_app_history = df_event_data[self.music_app_history_cols]
            # Loop through the rows in the new dataframe
            for index, row in df_music_app_history.iterrows():
                # Insert the records into the music_app_history table
                session.execute(music_app_history_table_insert,
                                (row['song'], row['firstName'],
                                 row['lastName'], int(row['userId'])))
        except Exception as e:
            print("Error while inserting data into music_app_history table")
            print(e)

    def drop_all_tables(self, session, cluster):
        """
        Description: Function to drop the tables songs_and_sessions,
        users_and_songs, and music_app_history.

        Arguments:
            session: the session object.
            cluster: the cassandra cluster object.

        Returns:
            None.
        """
        try:
            for drop_table_query in drop_table_queries_list:
                session.execute(drop_table_query)
        except Exception as e:
            print("Error while executing the drop table query:\n",
                  drop_table_query)
            print(e)

    def main(self):
        """
        Description: Main function to execute the different steps involved in
        the processing of the events data CSV file, in the right order.

        Arguments:
            None.

        Returns:
            None
        """
        # Create & set the keyspace
        session, cluster = self.create_keyspace()
        self.set_keyspace(session)

        # Read the input CSV file & load it to a pandas dataframe
        df_event_data = self.create_event_data_frame()

        # Drop all the tables, if they exist already
        self.drop_all_tables(session, cluster)

        # Create all the required tables
        self.create_all_tables(session)

        # Insert the events data records into the tables
        self.process_songs_and_sessions_data(session, df_event_data)
        self.process_users_and_songs_data(session, df_event_data)
        self.process_music_app_history_data(session, df_event_data)

        # Close the connection
        session.shutdown()
        cluster.shutdown()


# Generate the consolidated event data CSV file
sparkify_events_db_setup = SparkifyEventsDBSetup()
sparkify_events_db_setup.main()
