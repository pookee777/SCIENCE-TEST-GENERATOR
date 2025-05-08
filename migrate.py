import os
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from extensions import db
from models import *

# Set up base directory for SQLite path
basedir = os.path.abspath(os.path.dirname(__file__))

# 1. SQLite engine
sqlite_path = os.path.join(basedir, 'app.db')
sqlite_uri = 'sqlite:///' + sqlite_path
sqlite_engine = create_engine(sqlite_uri)

# 2. MySQL engine (replace username/password/host as needed)
mysql_uri = 'mysql+pymysql://root:Naruto7psk!!@localhost/app_db?charset=utf8mb4'
mysql_engine = create_engine(mysql_uri)

# 3. Reflect existing SQLite schema
sqlite_metadata = MetaData()
sqlite_metadata.reflect(bind=sqlite_engine)

# 4. Create all tables in MySQL
print("Creating tables in MySQL...")
db.metadata.create_all(mysql_engine)

# 5. Session setup
SQLiteSession = sessionmaker(bind=sqlite_engine)
MySQLSession = sessionmaker(bind=mysql_engine)
sqlite_session = SQLiteSession()
mysql_session = MySQLSession()

def convert_row_to_dict(row, table):
    """Convert a row to a dictionary with proper type conversion"""
    return {str(column.name): value for column, value in zip(table.columns, row)}

# 6. Data migration with proper type handling
def migrate_table(table_name, source_engine, target_engine):
    print(f"Migrating: {table_name}")
    try:
        # Get table structure
        table = Table(table_name, sqlite_metadata, autoload_with=source_engine)
        
        # Fetch all rows
        rows = source_engine.execute(table.select()).fetchall()
        if rows:
            # Convert rows to dictionaries with proper type handling
            dict_rows = [convert_row_to_dict(row, table) for row in rows]
            
            # Insert in batches of 100
            batch_size = 100
            for i in range(0, len(dict_rows), batch_size):
                batch = dict_rows[i:i + batch_size]
                target_engine.execute(table.insert(), batch)
                
        print(f"✓ Successfully migrated {len(rows)} rows from {table_name}")
    except Exception as e:
        print(f"× Error migrating {table_name}: {str(e)}")
        raise

# Migration order to handle foreign key constraints
migration_order = [
    'user',
    'chapter',
    'question',
    'test',
    'test_question',
    'test_result',
    'question_answer'
]

# Clear existing data in MySQL (optional, comment out if not needed)
print("Clearing existing data in MySQL...")
with mysql_engine.begin() as connection:
    # Disable foreign key checks
    connection.execute("SET FOREIGN_KEY_CHECKS=0")
    
    # Truncate all tables
    for table_name in reversed(migration_order):
        connection.execute(f"TRUNCATE TABLE {table_name}")
    
    # Re-enable foreign key checks
    connection.execute("SET FOREIGN_KEY_CHECKS=1")

# Perform migration in correct order
print("Starting migration...")
for table_name in migration_order:
    migrate_table(table_name, sqlite_engine, mysql_engine)

print("Migration complete.")

# 7. Clean up
sqlite_session.close()
mysql_session.close()
