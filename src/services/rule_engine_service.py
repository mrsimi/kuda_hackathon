from datetime import datetime
from flask import g
from src.dto.response_dto import ResponseDto
from src.infra.db_repo import DatabaseManager


class RuleEngine:
    def __init__(self):
        self.db:DatabaseManager = g.db_manager
        self.conditional_map = {
            "GreaterThan": lambda x, y: x > y,
            "LessThan": lambda x, y: x < y,
            "EqualTo": lambda x, y: x == y,
            "GreaterThanOrEqualTo": lambda x, y: x >= y,
            "LessThanOrEqualTo": lambda x, y: x <= y,
            "NotEqualTo": lambda x, y: x != y
        }
        self.type_validation_map = {
                'float': lambda v: float(v),
                'int': lambda v: int(v),
                'datetime': lambda v: datetime.strptime(v, "%Y-%m-%d %H:%M:%S"),
            }

    def get_data_points(self):
        table_columns = self.db.get_columns_of_table('kd_hk_transactions')
        if table_columns:
            data_columns = {}
            for row in table_columns:
                data_columns[row[0]] = row[1]

            return ResponseDto(True, 'success', data_columns, 200)
        else:
            return ResponseDto(False, 'Failed to get the data points. Kindly try again', None, 400)
        
    def __keys_exist(self, data:dict, keys:list):
        return all(key in data and data[key] for key in keys)
    
    def __validate_value_type_rule(self, rule, data):
        column_to_check = rule[1]
        value_to_check_against = rule[4]
        conditional = rule[3]

        if self.__keys_exist(data, [column_to_check]):
            check_value = data[column_to_check]
            value_type = type(check_value)
            converted = value_to_check_against
            if value_type != 'str':
                converted = self.type_validation_map[value_type](value_to_check_against)
            
            #if true it faulted the rule
            if self.conditional_map[conditional](check_value, converted):
                #log request 
                return True

        print('column does for rule does not exist in request sent')
        return False
    
    def __validate_expression_type_rule(self, rule, data):
        return False
    
    def __validate_rule(self, rule, data):
        #get rule type
        # isExpression or not
        if bool(rule[2]):
            return self.__validate_value_type_rule(rule, data)
        else:
            return self.__validate_expression_type_rule(rule, data)

    def set_value_type_rule(self, dataRequest: dict):
        if not self.__keys_exist(dataRequest, ['dataColumn', 'checkValue', 'conditional']):
            return ResponseDto(False, 'Invalid request: Missing or empty values', None, 400)

        dataColumn = dataRequest['dataColumn']
        checkValue = dataRequest['checkValue']
        conditional = dataRequest['conditional']
        
        if conditional not in self.conditional_map:
            return ResponseDto(False, f"Unsupported conditional: {conditional}", None, 400)
        data_points = self.get_data_points()
        if dataColumn not in data_points.data:
            return ResponseDto(False, 'dataColumn is not mapped to the table', None, 400)

        column_data_type = data_points.data[dataColumn]

        if 'char' not in column_data_type:
            if column_data_type in self.type_validation_map:
                try:
                    self.type_validation_map[column_data_type](checkValue)
                except ValueError:
                    return ResponseDto(False,
                                       f'Invalid data type for checkValue. Expected {
                                           column_data_type}.',
                                       None, 400)

        insert_query = """
            INSERT INTO kd_hk_rules_config
            (dataColumn, isExpression, conditional, checkValue)
            VALUES (?, 0, ?, ?)
        """
        res = self.db.single_inserts(
            insert_query, (dataColumn, conditional, checkValue))
        if res is None:
            return ResponseDto(False, 'Error creating rule. Please try again later.', None, 400)

        return ResponseDto(True, 'Rule has been set successfully', None, 200)
    
    def rule_check(self, data) -> ResponseDto:
        select_rules_query = "select * from kd_hk_rules_config where isActive = 1"
        active_rules = self.db.fetch_records(select_rules_query, ())
        if active_rules:
            result = False
            for rule in active_rules:
                validate_result = self.__validate_rule(rule, data)
                result = True if validate_result is True else False

            message = 'Transaction is suspicious' if result else 'Not a suspicious transaction'
            res =  ResponseDto(True, message, result, 200)
            return res
        else:
            return {
                'isSuccessful': True, 
                'data': None,
                'message': 'No active rules'
            }
