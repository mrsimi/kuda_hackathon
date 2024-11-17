from flask import Blueprint, Response, app, g, json, logging, request

from src.dto.response_dto import ResponseDto
from src.infra.db_repo import DatabaseManager
from src.services.anomaly_engine_service import AnomalyEngine
from src.services.rule_engine_service import RuleEngine

anomaly = Blueprint("anomaly", __name__)

@anomaly.route('/record', methods=['POST'])
def save_record():
    dataRequest = request.json
    try:
        anomaly = AnomalyEngine()
        res = anomaly.save_record(dataRequest)
        return Response(response=json.dumps(res.to_dict()),
                            status=res.statuscode,
                            mimetype='application/json'
                            )
    except Exception as e:
        return Response(response=json.dumps(ResponseDto(
            False, 'An error occured. Try again later', None, 500
        ).to_dict()),
            status=500,
            mimetype='application/json'
        )