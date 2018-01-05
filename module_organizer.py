import os

class module_organizer():
    """
    This class performs functionality takes the processed logs from a new computer and adds them
    to an existing data folder so that we do not have to recompute the dataframe from scratch again

    """

    def organize_modules(self):
        for root, dir, fil in os.walk(data_folder):
            for directory in dir:
                for fname in os.listdir(root + os.sep + directory):
                    abs_path = root + os.sep + directory + os.sep + fname
                    # pc_name format is NAME-datatype
                    pc_name = fname.rsplit('-', 1)[0]
                    _read_csv(abs_path, pc_name)

