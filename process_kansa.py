import os
import pandas as pd
from pandas.io.parsers import CParserError
import chardet
import traceback


class Process():
    def __init__(self, input_fol='data', output_fol='data_processed'):
        self.input_folder = input_fol
        self.output_folder = output_fol

    def process_kansa_report(self, input_folder='data'):
        data_folders = os.listdir(input_folder)
        for data_folder in data_folders:
            for root, dir, fil in os.walk(input_folder + os.sep + data_folder):
                # Only process the first fil
                fname = fil[0]

                # Skip the log file
                if not fil or '.Log' in fname:
                    continue

                try:
                    # splits the files into folder corresponding to each data type
                    pc_name, datatype = fname.replace('.csv', '').rsplit('-', 1)
                    df = self._read_csv(root + os.sep + fname, pc_name)

                    # Create the processed folder if it does not exists
                    processed_path = self.output_folder + os.sep + datatype + '_processed'
                    if not os.path.exists(processed_path):
                        os.mkdir(processed_path)
                    if not df.empty:
                        df.to_csv(processed_path + os.sep + pc_name + '.csv', encoding='UTF-8', index=False)
                except:
                    print fil
                    traceback.print_exc()
                    exit(0)
                    print 'here'

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
                return pd.DataFrame()

        # Detects encoding
        rawdata = open(abs_path, 'rb').read()
        result = chardet.detect(rawdata)
        charenc = result['encoding']

        # Reads the csv
        try:
            df = pd.read_csv(abs_path, encoding=charenc)
            df['PC_name'] = pc_name

            # Appends the pc name to the start of a column
            df = df.reindex(columns=['PC_name'] + df.columns[:-1].tolist())
            return df

        except CParserError:
            print '{} failed to parse'.format(abs_path)
        except:
            traceback.print_exc()

        return pd.DataFrame()


a = Process()
a.process_kansa_report()
