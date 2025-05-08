import logging
from app import create_app

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        app = create_app()
        logger.info("Starting server at http://127.0.0.1:5000")
        app.run(debug=True, host='127.0.0.1', port=5000)
    except Exception as e:
        logger.exception("Failed to start application")
        raise
