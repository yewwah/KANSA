import os
import pandas as pd
from pandas.io.parsers import CParserError
import chardet
import traceback


class Process():
    def __init__(self, input_fol = 'data', output_fol = 'data_processed'):
        self.input_folder = input_fol
        self.output_folder = output_fol


    def process_kansa_report(self, input_folder = 'data'):
        data_files = os.listdir(input_folder)
        for data in data_files:

            # Creates the processed folder if it does not exists
            try:
                df, pc_name = self._process_data(data)
            except:
                print data

            processed_path = self.output_folder + os.sep + pc_name + '_processed'
            if not os.path.exists(processed_path):
                os.mkdir(processed_path)

    def _process_data(self, data_folder):
        """
        :param data_folder:
        Folder which contains the following structure
        Original Folder
            - AMhealthstatus
                -AMHealthStatus.csv

        :return:
        Dataframe: A processed dataframe
        directory : str representation of the name of the folder
        """
        for root, dir, fil in os.walk(data_folder):
            for directory in dir:
                for fname in os.listdir(root + os.sep + directory):
                    abs_path = root + os.sep + directory + os.sep + fname
                    # pc_name format is NAME-datatype
                    pc_name = fname.rsplit('-', 1)[0]
                    dataframe = self._read_csv(abs_path, pc_name)
                    return dataframe , directory

    def _read_csv(self, abs_path, pc_name):
        """
        :param abs_path: abs path of fil
        pc_name : STR - Name of the PC
        :return: Dataframe
        """

        # Check if the file is empty
        with open(abs_path) as f:
            first_line = f.readline()
            # \xff\xfe is a null byte
            if not first_line or first_line == '\xff\xfe':
                return None

        # Detects encoding
        rawdata = open(abs_path, 'rb').read()
        result = chardet.detect(rawdata)
        charenc = result['encoding']

        # Reads the csv
        try:
            df = pd.read_csv(abs_path, encoding=charenc)
            df['header'] = pc_name

            # Appends the pc name to the start of a column
            df.reindex(columns=['n'] + df.columns[:-1].tolist())
            return df

        except CParserError:
            print '{} failed to parse'.format(abs_path)
        except:
            traceback.print_exc()
        return None

        # _read_csv('Output_20180104160930\ClrVersion\DESKTOP-GPAN1LT-ClrVersion.csv', 'a')
a = Process()
a.process_kansa_report()
