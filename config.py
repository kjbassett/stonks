import datetime
import os

CONFIG = {
    'root_path': os.getcwd(),
    'min_date': datetime.date.today() - datetime.timedelta(days=730),
    'db_folder': 'H:/databases/',
    'db_name': 'stonksDB.sqlite'
}
