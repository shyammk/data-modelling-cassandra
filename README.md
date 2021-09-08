
# Sparkify Cassandra Database Setup


## Introduction


This project is about analyzing the data on songs and user activity on the new music streaming app from Sparkify. The analytics team at Sparkify, is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of CSV files on user activity on the app. The data engineers are tasked with the creation of an Apache Cassandra database with tables specifically designed to optimize queries on song play analysis. We will be modelling a few sample tables in Cassandra, with a few sample data use-cases.


## Datasets

`Events Dataset`: Our dataset comprises of several different CSV files, containing the events log data. We would be combining all of these CSV files, by consolidating all the data records into a single CSV file. A snapshot of the data in the consolidated CSV file would look like this.

![Events Data Snapshot](/assets/images/image_event_datafile_new.jpg)


## Schema

Using the above dataset, we will be modelling a few tables in Apache Cassandra, based on three sample questions we would like to answer. We will create three tables namely `songs_and_sessions`, `users_and_songs`, and `music_app_history`.


## Directory Structure

1. `assets/`: Parent directory for all assets like images, documents etc.
    - `images/`: This folder contains all the images
2. `code/`: Parent directory containing all the source code.
    - `cassandra_queries.py`: File containing all the required CQL queries, used to create tables, insert records and drop tables in Apache Cassandra. This is imported into other python files.
    - `generate_event_data_file.py`: Script to parse all the individual events data CSV files, and consolidate them into a single CSV file.
    - `etl.py`: Script to read and process the files present inside the `data` directory.
3. `data/`: Parent directory containing all the raw data files.
    - `event_data/`: Sub-directory containing all the CSV files containing the events data.
    - `event_datafile_new.csv`: The consolidated CSV file, that would be generated by the script `generate_event_data_file.py`.
4. `notebooks/`: Parent directory for all the iPython notebooks.
    - `sparkify_data_modelling_cassandra.ipynb`: The python notebook containing a step-by-step explanation and execution of this project.
5. `pre_requisite_libraries.txt`: File containing the required python libraries. Can be directly fed to a `pip` command to setup.


## Execution

 - Note that you need to have both Python (v3.7+) and a local version of Apache Cassandra (preferably V3.0 & above) installed to create the required tables and execute this data pipeline.
 - Install the required python libraries using the command:
 `pip install -r pre_requisite_libraries.txt`.
 - Next, open the terminal or command prompt, navigate to the `code` directory and execute the command:
 `python generate_event_data_file.py`
 - Once the consolidated input CSV file is created, execute the command:
 `python etl.py`
 - Then use a python notebook or CQL shell to execute the validation queries and check the results.
