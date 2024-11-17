from datetime import datetime
import json
from typing import List
from flask import g
from src.dto.response_dto import ResponseDto
from src.infra.db_repo import DatabaseManager
from concurrent.futures import ThreadPoolExecutor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor(max_workers=1)


class RuleEngine:
    def __init__(self):
        self.db: DatabaseManager = g.db_manager
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
            'vachar': lambda v: str(v),
            'string': lambda v: str(v),
            'str': lambda v: str(v),
        }

    def get_conditionals(self):
        return [key for key in self.conditional_map]

    def __get_table_columns(self, table_name):
        table_columns = self.db.get_columns_of_table(table_name)
        if table_columns:
            return {row[0]: row[1] for row in table_columns}
        return None

    def get_data_points(self):
        try:
            table_columns = self.__get_table_columns('kd_hk_transactions')
            if table_columns:
                data = {
                    'datapoints': table_columns,
                    'conditionals': self.get_conditionals(),
                    'category': ['transactions']
                }
                return ResponseDto(True, 'success', data, 200)
            else:
                return ResponseDto(False, 'Failed to get the data points. Kindly try again', None, 400)
        except Exception as e:
            logger.error(f"Error fetching table columns: {e}")
            return ResponseDto(False, 'An error occurred while fetching data points', None, 500)

    def __keys_exist(self, data: dict, keys: list):
        return all(key in data and data[key] for key in keys)

    def __convert_keys_to_lowercase(self, input_dict):
        return {k.lower(): v for k, v in input_dict.items()}

    def __save_report(self, rule_id, data, saving_count=0):
        insert_query = """
                    insert into kd_hk_report
                    (ruleId, payloadType, payloadDetails)
                    values(?, 'Transaction', ?)
                """
        # push to a query to save it
        try:
            self.db.single_inserts(insert_query, (rule_id, json.dumps(data)))
        except Exception as e:
            logger.error(f"Failed to insert report for rule_id {rule_id}: {e}")

    def __validate_value_type_rule(self, rule, data):
        """
        Validates a rule against the given data.

        :param rule: The rule to validate.
        :param data: The data to validate the rule against.
        :return: True if the rule is faulted, False otherwise.
        """
        # Normalize input
        column_to_check = rule[1].lower()
        value_to_check_against = rule[4]
        conditional = rule[3]
        check_value_data_type = rule[8]
        data = self.__convert_keys_to_lowercase(data)

        # Guard clause: Ensure the column exists in the data
        if column_to_check not in data:
            logger.warning(f"Data column '{column_to_check}' not present in the request")
            return False

        try:
            # Convert values
            converter = self.type_validation_map.get(check_value_data_type)
            if not converter:
                logger.error(f"Unsupported data type for conversion: {check_value_data_type}")
                return False

            converted_check_value = converter(data[column_to_check])
            converted_value_to_check_against = converter(
                value_to_check_against)

            # Evaluate the condition
            is_faulted = self.conditional_map[conditional](
                converted_check_value, converted_value_to_check_against
            )

            if is_faulted:
                self.__save_report(rule[0], data)
                return True
            return False
        except KeyError as e:
            logger.error(f"Key error: {e}")
            return False
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid data type for rule comparison: {e}")
            return False

    def __validate_expression_type_rule(self, rule, data):
        return False

    def __validate_rule(self, rule, data):
        # get rule type
        # isExpression or not
        if not rule[2]:
            return self.__validate_value_type_rule(rule, data)
        else:
            return self.__validate_expression_type_rule(rule, data)

    def set_value_type_rule(self, dataRequest: dict):
        if not self.__keys_exist(dataRequest, ['dataPoint', 'checkValue', 'conditional']):
            return ResponseDto(False, 'Invalid request: Missing or empty values', None, 400)

        dataPoint = dataRequest['dataPoint']
        checkValue = dataRequest['checkValue']
        conditional = dataRequest['conditional']

        if conditional not in self.conditional_map:
            return ResponseDto(False, f"Unsupported conditional: {conditional}", None, 400)
        table_columns = self.__get_table_columns('kd_hk_transactions')

        if dataPoint not in table_columns:
            return ResponseDto(False, 'dataPoint is not mapped to the table', None, 400)

        column_data_type = table_columns[dataPoint]

        if 'char' not in column_data_type:
            if column_data_type in self.type_validation_map:
                try:
                    self.type_validation_map[column_data_type](checkValue)
                except ValueError:
                    return ResponseDto(False, f'Invalid data type for checkValue. Expected {column_data_type}.', None, 400)
        description = dataRequest['description'] if dataRequest['description'] else ''
        insert_query = """
            INSERT INTO kd_hk_rules
            (dataPoint, isExpression, conditional, checkValue, CheckValueDatatype, Description)
            VALUES (?, 0, ?, ?, ?, ?)
        """
        res = self.db.single_inserts(
            insert_query, (dataPoint, conditional, checkValue, column_data_type, description))
        if res is None:
            return ResponseDto(False, 'Error creating rule. Please try again later.', None, 400)

        return ResponseDto(True, 'Rule has been set successfully', None, 200)

    def rule_check(self, data) -> ResponseDto:
        try:
            select_rules_query = "select * from kd_hk_rules with(nolock) where isActive = 1"
            active_rules = self.db.fetch_records(select_rules_query, ())
            if active_rules:
                result = False
                for rule in active_rules:
                    validate_result = self.__validate_rule(rule, data)
                    result = result or validate_result

                message = 'Transaction is suspicious' if result else 'Not a suspicious transaction'
                return ResponseDto(True, message, result, 200)
            else:
                return ResponseDto(True, 'No active rules', result, 200)
        except Exception as e:
            logger.error(f"Error occurred during rule check: {e}")
            return ResponseDto(False, 'An error occured', False, 500)

    def get_rules(self) -> List[dict]:
        try:
            select_rules_query = "select * from kd_hk_rules with(nolock) where isActive = 1"
            active_rules = self.db.fetch_records(select_rules_query, ())
            results = []
            if active_rules:
                for rule in active_rules:
                    rule_dict = {}
                    rule_dict['type'] = 'Expression' if rule[2] else 'ValueCheck'
                    rule_dict['description'] = rule[9]
                    rule_dict['id'] = rule[0]
                    results.append(rule_dict)

            return ResponseDto(True, 'Success', results, 200)

        except Exception as e:
            logger.error(f'error_trying_to_get_rules {e}')
            return ResponseDto(False, 'An error occured', False, 500)

    def get_report(self) -> List[dict]:
        try:
            select_report_query = """
                select report.PayloadType, report.PayloadDetails, 
                report.DateInserted, rules.id, rules.Description  
                from kd_hk_report as report with(nolock)
                join kd_hk_rules as rules  on report.RuleId = rules.Id 

            """
            records = self.db.fetch_records(select_report_query, ())
            results = []
            if records:
                for index, record in enumerate(records):
                    results.append({
                        'sn': index+1,
                        'payloadType': record[0],
                        'payloadDetails': json.loads(record[1]),
                        'date': record[2],
                        'ruleId': record[3],
                        'ruleDescription': record[4]
                    })
            return ResponseDto(True, 'Success', results, 200)
        except Exception as e:
            logger.error(f'error_trying_to_get_report {e}')
            return ResponseDto(False, 'An error occured', False, 500)

    def set_expression_type_rule(self, dataRequest: dict):
        try:
            if not self.__keys_exist(dataRequest, ['dataPoint', 'expression', 'conditional']):
                return ResponseDto(False, 'Invalid request: Missing or empty values', None, 400)

            # check if the expression is valid
            # run the script and get the first result and insert to the expression db
            # -- Enable the trigger and set trigger
            # save rule in the database and triggername

            # test trigger - pick stored trigger name, add a test record to transaction, check if dbtrigger is triggered
            return
        except Exception as e:
            return

    #every minute, get all active rules with triggernames and check if they exists