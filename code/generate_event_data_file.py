import os
import glob
import csv


class EventFileGenerator:

    def __init__(self):
        self.input_file_sub_dir = '/data/event_data'
        self.event_data_file_name = "event_datafile_new.csv"
        self.output_file_sub_dir = '/data/' + self.event_data_file_name
        self.event_data_file_header = ['artist', 'firstName', 'gender',
                                       'itemInSession', 'lastName', 'length',
                                       'level', 'location', 'sessionId',
                                       'song', 'userId']
        self.event_data_file_name = "event_datafile_new.csv"

    def get_parent_directory_path(self, sub_dir_path):
        """
        Description: Function to create the main directory filepath string,
        using the sub-directory paths. This would be used to create the parent
        directory for input files and output file.

        Arguments:
            sub_dir_path: string containing the sub directory path.

        Returns:
            main_dir_path: the string containing the input file path.
        """
        # Get the current folder and subfolder event data
        main_dir_path = os.getcwd() + sub_dir_path
        return main_dir_path

    def create_file_path_list(self, parent_dir):
        """
        Description: Function to create a list of the filepaths using the
        individual event data files.

        Arguments:
            parent_dir: the parent folder containing multiple sub-folders and
            all the input files.

        Returns:
            file_path_list: the list containing all the input file paths.
        """
        # Create a for loop to create a list of files and collect each filepath
        for root, dirs, files in os.walk(parent_dir):
            # Join the file path and roots with the subdirectories using glob
            file_path_list = glob.glob(os.path.join(root, '*'))
            return file_path_list

    def get_event_data_rows(self, file_path_list):
        """
        Description: Function to loop through the list of the filepaths, read
        each file and extract the data rows from them.

        Arguments:
            file_path_list: the list containing all the input file paths.

        Returns:
            event_data_rows_list: the list containing all the data rows, from
            all the input files.
        """
        # Initiating empty list to hold the data rows generated from each file
        event_data_rows_list = []

        # Loop through every filepath in the file path list
        for file_path in file_path_list:

            # Reading csv file
            with open(file_path, 'r', encoding='utf8', newline='') as csvfile:
                # Creating a csv reader object
                csvreader = csv.reader(csvfile)
                next(csvreader)
                # Extracting each data row one by one and appending it
                for line in csvreader:
                    # print(line)
                    event_data_rows_list.append(line)

        return event_data_rows_list

    def write_event_data_to_csv(self, output_file_dir_path,
                                event_data_rows_list):
        """
        Description: Function to create a smaller event data csv file that will
        be used to insert data into the Apache Cassandra tables.

        Arguments:
            output_file_dir_path: the folder in which the CSV file will be
            saved.
            event_data_rows_list: the list containing all the data rows, from
            all the input files.

        Returns:
            None
        """
        # Define the format of the CSV file
        csv.register_dialect('myDialect',
                             quoting=csv.QUOTE_ALL,
                             skipinitialspace=True)

        # Write the relevent columns in the event data rows to the CSV file
        with open(output_file_dir_path, 'w', encoding='utf8', newline='') as f:
            writer = csv.writer(f, dialect='myDialect')
            writer.writerow(self.event_data_file_header)
            for row in event_data_rows_list:
                if (row[0] == ''):
                    continue
                writer.writerow((row[0], row[2], row[3], row[4], row[5],
                                row[6], row[7], row[8], row[12], row[13],
                                row[16]))

    def main(self):
        """
        Description: Main function to execute the different steps involved in
        the creation of the events data CSV file, in the right order.

        Arguments:
            None.

        Returns:
            None
        """
        input_files_dir_path = self.get_parent_directory_path(
                                                    self.input_file_sub_dir)
        input_file_paths_list = self.create_file_path_list(
                                                    input_files_dir_path)
        event_data_rows_list = self.get_event_data_rows(input_file_paths_list)
        output_file_dir_path = self.get_parent_directory_path(
                                                    self.output_file_sub_dir)
        self.write_event_data_to_csv(output_file_dir_path,
                                     event_data_rows_list)

        with open(output_file_dir_path, 'r', encoding='utf8') as f:
            print("No. of rows in input file: ", sum(1 for line in f))


# Generate the consolidated event data CSV file
event_file_generator = EventFileGenerator()
event_file_generator.main()
