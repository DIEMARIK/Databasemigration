import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))

from src.queries.core import create_table
from src.migrate import migrate_database
create_table()
migrate_database()