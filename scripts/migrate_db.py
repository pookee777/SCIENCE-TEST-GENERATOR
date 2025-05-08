import os
import sys
from sqlalchemy import text

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from extensions import db

def migrate_database():
    app = create_app()
    with app.app_context():
        try:
            # Add image_path column using text()
            sql = text('ALTER TABLE question ADD COLUMN image_path VARCHAR(255) NULL')
            db.session.execute(sql)
            db.session.commit()
            print("Successfully added image_path column to question table")
        except Exception as e:
            if 'duplicate column name' in str(e).lower():
                print("Column already exists")
            else:
                print(f"Error: {e}")
                db.session.rollback()

if __name__ == "__main__":
    migrate_database()
