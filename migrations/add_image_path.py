from app import create_app
from extensions import db

def upgrade():
    app = create_app()
    with app.app_context():
        db.engine.execute('ALTER TABLE question ADD COLUMN image_path VARCHAR(255)')
        
if __name__ == '__main__':
    upgrade()
