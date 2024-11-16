from flask import Blueprint
from src.controllers.rules_engine_controller import rules

api = Blueprint('api', __name__)

api.register_blueprint(rules, url_prefix='/rule')