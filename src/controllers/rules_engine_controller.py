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
                #print('set value type')
                res = rules_service.set_value_type_rule(data)
                return Response(
                    response=json.dumps(res.to_dict()),
                    status=res.statuscode,
                    mimetype='application/json'
                )
                return
            else:
                print('set expression type')
                res = rules_service.set_expression_type_rule(data)
                return Response(
                    response=json.dumps(res.to_dict()),
                    status=res.statuscode,
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

@rules.route('/rules', methods=['GET'])
def get_rules():
    try:
        rules_service = RuleEngine()
        res = rules_service.get_rules()
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

@rules.route('/report', methods=['GET'])
def get_repot():
    try:
        rules_service = RuleEngine()
        res = rules_service.get_report()
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
    
@rules.route('/disable', methods=['POST'])
def disable_rule():
    try:
        rules_service = RuleEngine()
        req = request.json
        if req['ruleId']:
            ruleId = req['ruleId']
            res = rules_service.disable_rule(ruleId)
            return Response(response=json.dumps(res.to_dict()),
                            status=res.statuscode,
                            mimetype='application/json'
                            )
        return Response(response=json.dumps(ResponseDto(
            False, 'No ruleId passed', None, 400
        ).to_dict()),
            status=400,
            mimetype='application/json'
        )
    except Exception as e:
        return Response(response=json.dumps(ResponseDto(
            False, 'An error occured. Try again later', None, 500
        ).to_dict()),
            status=500,
            mimetype='application/json'
        )
    

