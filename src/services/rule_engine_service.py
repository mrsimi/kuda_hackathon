from datetime import datetime
import json
import random
import re
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
        column_to_check = rule[1].lower()
        conditional = rule[3]

        
        if column_to_check not in data:
            logger.warning(f"Data column '{column_to_check}' not present in the request")
            return False
        
        try:
            #get rule result 
            rule_result_query = """
            select ResultValue from kd_hk_expression_result with(nolock)
            where RuleId = ? and SourceAccountNumber = ?
            """
            rule_result = self.db.fetch_record(rule_result_query, (rule[0], data['sourceaccountnumber']))
            
            logger.info(f'rule_result {rule_result}')
            if rule_result is None:
                return False
            
            
            converter = self.type_validation_map.get(rule[12])
            if not converter:
                logger.error(f"Unsupported data type for conversion: {rule[12]}")
                return False
            
            converted_check_value = converter(data[column_to_check])
            converted_value_to_check_against = converter(rule_result[0])

            # # Evaluate the condition
            logger.info(converted_check_value, converted_value_to_check_against)
            is_faulted = self.conditional_map[conditional](converted_check_value, converted_value_to_check_against)

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
        ruleName = dataRequest['name'] if dataRequest['name'] else ''
        insert_query = """
            INSERT INTO kd_hk_rules
            (dataPoint, isExpression, conditional, checkValue, CheckValueDatatype, Description, RuleName)
            VALUES (?, 0, ?, ?, ?, ?, ?)
        """
        res = self.db.single_inserts(
            insert_query, (dataPoint, conditional, checkValue, column_data_type, description, ruleName))
        if res is None:
            return ResponseDto(False, 'Error creating rule. Please try again later.', None, 400)

        return ResponseDto(True, 'Rule has been set successfully', None, 200)

    def rule_check(self, data) -> ResponseDto:
        try:
            data = self.__convert_keys_to_lowercase(data)
            select_rules_query = "select * from kd_hk_rules with(nolock) where isActive = 1"
            active_rules = self.db.fetch_records(select_rules_query, ())
            if active_rules:
                result = False
                for rule in active_rules:
                    validate_result = self.__validate_rule(rule, data)
                    result = result or validate_result
            
                message = 'Transaction is suspicious' if result else 'Not a suspicious transaction'
                res= ResponseDto(True, message, result, 200)
            else:
                res = ResponseDto(True, 'No active rules', result, 200)
            
            
            #insert transaction
            tranx_insert_query = """
                insert into kd_hk_transactions (sourceAccountNumber, DestinationAccountNumber,
                 Amount,DestinationBankCode) values (?, ?, ?, ?)
            """
            self.db.single_inserts(tranx_insert_query, (data['sourceaccountnumber'], data['destinationaccountnumber'],
                                                     data['amount'], data['destinationbankcode']))
            logger.info(f'insert record {data}')
            return res
        except Exception as e:
            logger.error(f"Error occurred during rule check: {e}")
            return ResponseDto(False, 'An error occured', False, 500)

    def get_rules(self) -> List[dict]:
        try:
            select_rules_query = "select * from kd_hk_rules with(nolock)"
            active_rules = self.db.fetch_records(select_rules_query, ())
            results = []
            if active_rules:
                for rule in active_rules:
                    rule_dict = {}
                    rule_dict['name'] = rule[11]
                    rule_dict['type'] = 'ExpressionCheck' if rule[2] else 'ValueCheck'
                    rule_dict['description'] = rule[9]
                    rule_dict['id'] = rule[0]
                    rule_dict['isactive'] = rule[7]
                    results.append(rule_dict)

            return ResponseDto(True, 'Success', results, 200)

        except Exception as e:
            logger.error(f'error_trying_to_get_rules {e}')
            return ResponseDto(False, 'An error occured', False, 500)

    def get_report(self) -> List[dict]:
        try:
            select_report_query = """
                select report.PayloadType, report.PayloadDetails, 
                report.DateInserted, rules.id, rules.Description, rules.ruleName  
                from kd_hk_report as report with(nolock)
                join kd_hk_rules as rules  on report.RuleId = rules.Id 
                order by report.DateInserted DESC 
            """
            select_anomaly_query = """
                select * from kd_hk_anomalies
                order by timestamp desc
            """
            records = self.db.fetch_records(select_report_query, ())
            rule_results = []
            if records:
                for index, record in enumerate(records):
                    rule_results.append({
                        'sn': index+1,
                        'payloadType': record[0],
                        'payloadDetails': json.loads(record[1]),
                        'date': record[2],
                        'ruleId': record[3],
                        'ruleDescription': record[4],
                        'ruleName': record[5]
                    })
            results = {}
            results['rules'] = rule_results 

            anomaly_records = self.db.fetch_records(select_anomaly_query, ())
            anomlay_result = []
            if anomaly_records:
                for index, record in enumerate(anomaly_records):
                    anomlay_result.append({
                        'sn': index+1,
                        'userId': record[1],
                        'alertType': record[2],
                        'riskScore': record[3],
                        'date': record[4]
                    })
            
            results['anomalies'] = anomlay_result

            return ResponseDto(True, 'Success', results, 200)
        except Exception as e:
            logger.error(f'error_trying_to_get_report {e}')
            return ResponseDto(False, 'An error occured', False, 500)

    def set_expression_type_rule(self, dataRequest: dict):
        try:
            if not self.__keys_exist(dataRequest, ['dataPoint', 'expression', 'conditional']):
                return ResponseDto(False, 'Invalid request: Missing or empty values', None, 400)

            dataPoint = dataRequest['dataPoint']
            description = dataRequest['description']
            conditional = dataRequest['conditional']
            ruleName = dataRequest['name']
            
            categories = {
                'transactions': 'kd_hk_transactions'
            }
            user_expression = str(dataRequest['expression']).lower()
            replace_action = 0
            table_name = ''

            if '1=1' in user_expression or user_expression.startswith('select') == False:
                return ResponseDto(False, 'Invalid request: check your expression', None, 400)
            if 'where' not in user_expression:
                return ResponseDto(False, 'Invalid boundary: expression has no where clause. Without where clause expression is performed on all customer records', None, 400)
            
            for category in categories:
                if category in user_expression:
                    table_name = categories[category]
                    user_expression = user_expression.replace(category, table_name)
                    replace_action +=1
            
            
            if replace_action == 0:
                return ResponseDto(False, 'Invalid request: Missing category', None, 400)
            
            if conditional not in self.conditional_map:
                return ResponseDto(False, f"Unsupported conditional: {conditional}", None, 400)
            
            table_columns = self.__get_table_columns('kd_hk_transactions')

            if dataPoint not in table_columns:
                return ResponseDto(False, 'dataPoint is not mapped to the table', None, 400)

            data_point_data_type = table_columns[dataPoint]

            expression = user_expression +' and sourceaccountnumber=?'

            # check if the expression is valid
            query_result = self.db.fetch_record(expression, ('1',))
            if query_result == None:
                return ResponseDto(False, 'Invalid request: Expression is not valid', None, 400)

            
            trigger_name = f'{re.sub(r"\s+", "_", ruleName)}_{random.randint(1, 1000)}'
            # save rule in db
            insert_rule_query = """
                insert into kd_hk_rules  (dataPoint, isExpression, conditional, expression, triggerName, description, ruleName, DataPointDataType)
                values(?, 1, ?, ?, ?, ?, ?, ?)
            """
            self.db.single_inserts(insert_rule_query, (dataPoint, conditional, expression, trigger_name, description, ruleName, data_point_data_type))
            inserted_rule = self.db.fetch_record(query=f'select id from kd_hk_rules where triggerName=?', params=(trigger_name,))

            if inserted_rule is None:
                return ResponseDto(False, 'Error trying to save the rule', None, 400)
            
            # run the script and get the first result and insert to the expression result db
            test_expression = user_expression.replace('select', 'select sourceaccountnumber, ') + 'group by sourceaccountnumber'
            #logger.info(test_expression)
            first_expression_results = self.db.fetch_records(test_expression, ())
            if first_expression_results:
                #multiple inserts 
                data = [(inserted_rule[0], expression_result[1], str(type(expression_result[1])), expression_result[0]) for expression_result in first_expression_results]
                insert_query = """
                    insert into kd_hk_expression_result (RuleId, ResultValue, ResultDataType, SourceAccountNumber)
                    values (?, ?, ?, ?)
                """
                self.db.multiple_inserts(insert_query, data)

            # -- Enable the trigger and set trigger
            str_trigger_name = '\''+trigger_name+'\''
            create_trigger_query = f"""
                CREATE TRIGGER {trigger_name}
                ON {table_name}
                AFTER INSERT
                AS
                BEGIN
                    DECLARE @SourceAccountNumber NVARCHAR(10);
                    DECLARE @ResultValue NVARCHAR(MAX);
                    DECLARE @RuleId INT;
                    DECLARE @ExistingRuleCount INT;

                    DECLARE cur CURSOR FOR
                    SELECT SourceAccountNumber FROM inserted;

                    OPEN cur;
                    FETCH NEXT FROM cur INTO @SourceAccountNumber;

                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        -- Construct the result value for each row
                        SET @ResultValue = ({user_expression} and sourceaccountnumber= @SourceAccountNumber);

                        -- Retrieve RuleId
                        SELECT @RuleId = Id FROM kd_hk_rules WHERE TriggerName = {str_trigger_name};

                        -- Check if the rule already exists in the result table
                        SELECT @ExistingRuleCount = COUNT(*) FROM kd_hk_expression_result 
                        WHERE RuleId = @RuleId AND SourceAccountNumber=@SourceAccountNumber;

                        -- If the rule exists, update, otherwise insert
                        IF @ExistingRuleCount > 0
                        BEGIN
                            UPDATE kd_hk_expression_result
                            SET ResultValue = CAST(@ResultValue AS NVARCHAR(MAX)), DateTimeUpdated = GETDATE()
                            WHERE RuleId = @RuleId AND SourceAccountNumber=@SourceAccountNumber;
                        END
                        ELSE
                        BEGIN
                            INSERT INTO kd_hk_expression_result (RuleId, ResultValue, ResultDataType, SourceAccountNumber)
                            VALUES (@RuleId, CAST(@ResultValue AS NVARCHAR(MAX)), '', @SourceAccountNumber);
                        END

                        FETCH NEXT FROM cur INTO @SourceAccountNumber;
                    END;

                    CLOSE cur;
                    DEALLOCATE cur;
                END;
            """
            #logger.info(create_trigger_query)
            self.db.single_insert_no_param(create_trigger_query)

            #validate trigger insertion
            check_trigger = f"""
                SELECT t.name AS TriggerName
                FROM sys.triggers t
                INNER JOIN sys.tables tb ON t.parent_id = tb.object_id
                WHERE tb.name = ? and t.name = ?;
            """
            trigger = self.db.fetch_record(check_trigger, (table_name, trigger_name))
            if trigger is None:
                logger.error('could not create trigger for rule')
                #log it somewhere to retry .
            
            logger.info(trigger)

            # test trigger - pick stored trigger name, add a test record to transaction, check if dbtrigger is triggered
            return ResponseDto(True, 'Success', None, 200)
        except Exception as e:
            logger.error(f'error_set_expression_type_rule {e}')
            return ResponseDto(False, 'An error occured', False, 500) 

    #every minute, get all active rules with triggernames and check if they exists

    def disable_rule(self, ruleId):
        try:
            deactivate_rule_query = """
            update kd_hk_rules set isActive = 0
                where Id = ? 
            """
            self.db.single_inserts(deactivate_rule_query, (ruleId))
            return ResponseDto(True, 'Success', None, 200)
        except Exception as err:
            logger.error(f'error_trying_to_get_rules {err}')
            return ResponseDto(False, 'An error occured', False, 500)
    
    def enable_rule(self, ruleId):
        try:
            activate_rule_query = """
            update kd_hk_rules set is IsActive = 1
                where Id = ? 
            """
            self.db.single_inserts(activate_rule_query, (ruleId))
            return ResponseDto(True, 'Success', None, 200)
        except Exception as err:
            logger.error(f'error_trying_to_get_rules {err}')
            return ResponseDto(False, 'An error occured', False, 500)
