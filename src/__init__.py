import os
from flask import Flask, g

app = Flask(__name__)

from src.infra.db_repo import DatabaseManager
from src.routes import api 
app.register_blueprint(api, url_prefix='/api')

db_manager = DatabaseManager(
    server=os.getenv('DB_SERVER'),
    database=os.getenv('DB_NAME'),
    username=os.getenv('DB_USER'),
    password=os.getenv('DB_PASS')
)

@app.before_request
def before_request():
    g.db_manager = db_manager