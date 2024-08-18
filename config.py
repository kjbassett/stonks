import datetime
import os

CONFIG = {
    'root_path': os.getcwd(),
    'min_date': datetime.date.today() - datetime.timedelta(days=730),
    'db_folder': 'C:/Users/origa/software/stonks/data_access/db/',
    'db_name': 'stonksDB.sqlite'
}
