from flask import Blueprint
from src.controllers.rules_engine_controller import rules
from src.controllers.anomaly_controller import anomaly

api = Blueprint('api', __name__)

api.register_blueprint(rules, url_prefix='/rule')
api.register_blueprint(anomaly, url_prefix='/anomaly')