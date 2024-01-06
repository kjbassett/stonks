from app import create_app
from config import CONFIG
from models.db.async_database import AsyncDatabase

# Initialize the database instance
db_instance = AsyncDatabase(CONFIG["db_folder"] + CONFIG["db_name"])
app = create_app(db_instance)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
