from flask import Blueprint, Response, app, json, logging, request

from src.services.rule_engine_service import RuleEngine

rules = Blueprint("rules", __name__)

@rules.route('/setup', methods=['POST'])
def rule_setup():
    data = request.json
    print(data)
    return Response(
        response=json.dumps({
            'status': 'success'
        }), 
        status=200,
        mimetype='application/json'
    )

@rules.route('/datapoints', methods=['GET'])
def get_data_points():
    try:
        rules_service = RuleEngine()
        datapoints, status_code = rules_service.get_data_points()
        # this endpoint returns all the 
        # data points that can be used in creating the rules.
        return Response(response= json.dumps({
                'status': status_code, 
                'data':datapoints
            }), 
                status=status_code, 
                mimetype='application/json'
        )
    except Exception as e:
        app.logger.info('error_occured_setting_up_rule {0}', e)
        return Response(response= json.dumps({
                'status': 500, 
            }), 
                status=500, 
                mimetype='application/json'
        )