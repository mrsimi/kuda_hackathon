from flask import Blueprint, Response, app, json, logging, request

from src.dto.response_dto import ResponseDto
from src.services.rule_engine_service import RuleEngine

rules = Blueprint("rules", __name__)


@rules.route('/setup', methods=['POST'])
def rule_setup():
    try:
        data = request.json
        rules_service = RuleEngine()
        if 'isExpression' in data:
            if not data['isExpression']:
                res = rules_service.set_value_type_rule(data)
                return Response(
                    response=json.dumps(res.to_dict()),
                    status=res.statuscode,
                    mimetype='application/json'
                )
            else:
                return Response(
                    response=json.dumps({
                        'isSuccessful': True,
                        'message': 'still building expression rule type'
                    }),
                    status=400,
                    mimetype='application/json'
                )
        else:
            return Response(response=json.dumps(ResponseDto(
                False, 'Invalid Request payload. isExpression value not present', None, 400
            ).to_dict()),
                status=400,
                mimetype='application/json'
            )
    except Exception as e:
        print(e)
        return Response(response=json.dumps(ResponseDto(
            False, 'An error occured. Try again later', None, 500
        ).to_dict()),
            status=500,
            mimetype='application/json'
        )


@rules.route('/datapoints', methods=['GET'])
def get_data_points():
    try:
        rules_service = RuleEngine()
        res = rules_service.get_data_points()
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


@rules.route('/rulecheck', methods=['POST'])
def checkrule():
    try:
        data = request.json

        rules_service = RuleEngine()
        res = rules_service.rule_check(data)
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
